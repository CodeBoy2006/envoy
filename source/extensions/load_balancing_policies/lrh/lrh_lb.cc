#include "source/extensions/load_balancing_policies/lrh/lrh_lb.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "envoy/config/cluster/v3/cluster.pb.h"

#include "source/common/common/assert.h"
#include "source/common/common/hash.h"

#include "absl/container/inlined_vector.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace Envoy {
namespace Upstream {

struct LrhLoadBalancer::Ring::Topology {
  std::vector<RingEntry> ring_;
  std::vector<std::string> host_keys_;
  uint64_t min_hashes_per_host_{0};
  uint64_t max_hashes_per_host_{0};
};

TypedLrhLbConfig::TypedLrhLbConfig(const LrhLbProto& lb_config, Regex::Engine& regex_engine,
                                   absl::Status& creation_status)
    : TypedHashLbConfigBase(lb_config.consistent_hashing_lb_config().hash_policy(), regex_engine,
                            creation_status),
      lb_config_(lb_config) {}

LrhLoadBalancer::LrhLoadBalancer(const PrioritySet& priority_set, ClusterLbStats& stats,
                                 Stats::Scope& scope, Runtime::Loader& runtime,
                                 Random::RandomGenerator& random, uint32_t healthy_panic_threshold,
                                 const LrhLbProto& config, HashPolicySharedPtr hash_policy)
    : ThreadAwareLoadBalancerBase(priority_set, stats, runtime, random, healthy_panic_threshold,
                                  config.has_locality_weighted_lb_config(), std::move(hash_policy)),
      scope_(scope.createScope("lrh_lb.")), stats_(generateStats(*scope_)),
      min_ring_size_(
          PROTOBUF_GET_WRAPPED_OR_DEFAULT(config, minimum_ring_size, DefaultMinRingSize)),
      max_ring_size_(
          PROTOBUF_GET_WRAPPED_OR_DEFAULT(config, maximum_ring_size, DefaultMaxRingSize)),
      candidate_count_(
          PROTOBUF_GET_WRAPPED_OR_DEFAULT(config, candidate_count, DefaultCandidateCount)),
      deduplicate_hosts_(config.deduplicate_hosts()),
      use_hostname_for_hashing_(config.consistent_hashing_lb_config().use_hostname_for_hashing()),
      weight_preprocessing_(config.weight_preprocessing()),
      hash_balance_factor_(PROTOBUF_GET_WRAPPED_OR_DEFAULT(config.consistent_hashing_lb_config(),
                                                           hash_balance_factor, 0)) {
  if (min_ring_size_ > max_ring_size_) {
    throw EnvoyException(fmt::format("lrh: minimum_ring_size ({}) > maximum_ring_size ({})",
                                     min_ring_size_, max_ring_size_));
  }
  if (candidate_count_ == 0 || candidate_count_ > MaxCandidateCount) {
    throw EnvoyException(fmt::format("lrh: candidate_count ({}) must be in range [1, {}]",
                                     candidate_count_, MaxCandidateCount));
  }
}

ThreadAwareLoadBalancerBase::HashingLoadBalancerSharedPtr
LrhLoadBalancer::createLoadBalancer(const NormalizedHostWeightVector& normalized_host_weights,
                                    double min_normalized_weight,
                                    double /* max_normalized_weight */) {
  if (hash_balance_factor_ == 0) {
    for (auto it = cached_rings_.begin(); it != cached_rings_.end();) {
      auto cached_ring = it->lock();
      if (cached_ring == nullptr) {
        it = cached_rings_.erase(it);
        continue;
      }

      if (cached_ring->updateWeightsForSameHosts(normalized_host_weights,
                                                 use_hostname_for_hashing_)) {
        stats_.weight_only_updates_.inc();
        latest_ring_for_test_ = cached_ring;
        return cached_ring;
      }
      ++it;
    }
  } else {
    for (auto it = cached_bounded_rings_.begin(); it != cached_bounded_rings_.end();) {
      auto cached_bounded_ring = it->lock();
      if (cached_bounded_ring == nullptr) {
        it = cached_bounded_rings_.erase(it);
        continue;
      }

      if (cached_bounded_ring->updateWeightsForSameHosts(normalized_host_weights,
                                                         use_hostname_for_hashing_)) {
        stats_.weight_only_updates_.inc();
        latest_ring_for_test_ = cached_bounded_ring->ring();
        return cached_bounded_ring;
      }
      ++it;
    }
  }

  auto ring = std::make_shared<Ring>(normalized_host_weights, min_normalized_weight, min_ring_size_,
                                     max_ring_size_, candidate_count_, deduplicate_hosts_,
                                     use_hostname_for_hashing_, weight_preprocessing_,
                                     cached_topology_, stats_);
  cached_topology_ = ring->topology();
  cached_rings_.push_back(ring);
  latest_ring_for_test_ = ring;

  HashingLoadBalancerSharedPtr lrh_lb = ring;

  if (hash_balance_factor_ == 0) {
    return lrh_lb;
  }

  auto bounded_lrh_lb =
      std::make_shared<BoundedRing>(ring, normalized_host_weights, hash_balance_factor_);
  cached_bounded_rings_.push_back(bounded_lrh_lb);
  return bounded_lrh_lb;
}

LrhLoadBalancerStats LrhLoadBalancer::generateStats(Stats::Scope& scope) {
  return {ALL_LRH_LOAD_BALANCER_STATS(POOL_COUNTER(scope), POOL_GAUGE(scope))};
}

bool LrhLoadBalancer::updateWeightsForTest(absl::Span<const double> capacity_weights_by_host) {
  auto ring = latest_ring_for_test_.lock();
  return ring != nullptr && ring->updateWeightsForTest(capacity_weights_by_host);
}

bool LrhLoadBalancer::updateHostWeightsForTest(
    absl::Span<const std::pair<uint32_t, double>> weight_updates) {
  auto ring = latest_ring_for_test_.lock();
  return ring != nullptr && ring->updateHostWeightsForTest(weight_updates);
}

size_t LrhLoadBalancer::Ring::lowerBound(uint64_t hash) const {
  auto it =
      std::lower_bound(ring_->begin(), ring_->end(), hash,
                       [](const RingEntry& entry, uint64_t value) { return entry.hash_ < value; });
  if (it == ring_->end()) {
    return 0;
  }
  return std::distance(ring_->begin(), it);
}

double LrhLoadBalancer::Ring::sanitizeWeight(double effective_weight) {
  if (!std::isfinite(effective_weight) || effective_weight <= 0.0) {
    return MinEffectiveWeight;
  }
  return std::max(effective_weight, MinEffectiveWeight);
}

double LrhLoadBalancer::Ring::windowDebiasedWeight(double capacity_weight,
                                                   double total_capacity_weight, size_t host_count,
                                                   uint32_t candidate_count) {
  if (candidate_count <= 1 || host_count == 0) {
    return sanitizeWeight(capacity_weight);
  }

  const double c = static_cast<double>(candidate_count);
  const double max_relative = c - WindowDebiasedEpsilon;
  const double relative_capacity = std::min(
      static_cast<double>(host_count) * capacity_weight / total_capacity_weight, max_relative);
  return sanitizeWeight(((c - 1.0) * relative_capacity) / (c - relative_capacity));
}

double LrhLoadBalancer::Ring::weightedScore(uint64_t request_hash,
                                            const HostState& host_state) const {
  const uint64_t score_hash = HashUtil::xxHash64Value(request_hash, host_state.host_hash_);

  // Map to (0, 1] and use exponential-race scoring. Lower scores win.
  const double u = (static_cast<double>(score_hash) + 1.0) /
                   (static_cast<double>(std::numeric_limits<uint64_t>::max()) + 1.0);
  const double capacity_weight = host_state.capacity_weight_.load(std::memory_order_relaxed);
  if (weight_preprocessing_ == LrhLbProto::WINDOW_DEBIASED) {
    const double debiased_weight = windowDebiasedWeight(
        capacity_weight, total_capacity_weight_.load(std::memory_order_relaxed),
        host_states_.size(), candidate_count_);
    return -std::log(u) / debiased_weight;
  }
  return -std::log(u) / capacity_weight;
}

const LrhLoadBalancer::Ring::HostState*
LrhLoadBalancer::Ring::bestInCandidateBlock(uint64_t hash, size_t start, size_t max_slots,
                                            size_t& walked) const {
  ASSERT(!ring_->empty());

  const HostState* best = nullptr;
  double best_score = std::numeric_limits<double>::infinity();
  absl::InlinedVector<const Host*, MaxCandidateCount> seen_hosts;

  size_t candidates = 0;
  walked = 0;
  while (walked < max_slots && candidates < candidate_count_) {
    const RingEntry& entry = (*ring_)[(start + walked) % ring_->size()];
    const HostState& host_state = *host_states_[entry.host_index_];
    ++walked;

    if (deduplicate_hosts_) {
      const Host* host = host_state.host_.get();
      if (std::find(seen_hosts.begin(), seen_hosts.end(), host) != seen_hosts.end()) {
        continue;
      }
      seen_hosts.push_back(host);
    }
    ++candidates;

    const double score = weightedScore(hash, host_state);
    if (score < best_score) {
      best = &host_state;
      best_score = score;
    }
  }

  return best;
}

HostSelectionResponse LrhLoadBalancer::Ring::chooseHost(uint64_t hash, uint32_t attempt) const {
  if (ring_->empty()) {
    return {nullptr};
  }

  const size_t ring_size = ring_->size();
  size_t start = (lowerBound(hash) + static_cast<size_t>(attempt) * candidate_count_) % ring_size;
  size_t remaining = ring_size;

  while (remaining > 0) {
    size_t walked = 0;
    const HostState* best = bestInCandidateBlock(hash, start, remaining, walked);
    if (best != nullptr) {
      return {best->host_};
    }
    if (walked == 0) {
      break;
    }
    start = (start + walked) % ring_size;
    remaining -= walked;
  }

  return {nullptr};
}

bool LrhLoadBalancer::Ring::updateWeightsForTest(
    absl::Span<const double> capacity_weights_by_host) {
  if (capacity_weights_by_host.size() != host_states_.size()) {
    return false;
  }
  storeCapacityWeights(capacity_weights_by_host);
  return true;
}

bool LrhLoadBalancer::Ring::updateHostWeightsForTest(
    absl::Span<const std::pair<uint32_t, double>> weight_updates) {
  for (const auto& update : weight_updates) {
    if (update.first >= host_states_.size()) {
      return false;
    }
  }
  double total_capacity_weight = total_capacity_weight_.load(std::memory_order_relaxed);
  for (const auto& update : weight_updates) {
    HostState& host_state = *host_states_[update.first];
    const double old_weight = host_state.capacity_weight_.load(std::memory_order_relaxed);
    const double new_weight = sanitizeWeight(update.second);
    host_state.capacity_weight_.store(new_weight, std::memory_order_relaxed);
    total_capacity_weight += new_weight - old_weight;
  }
  total_capacity_weight_.store(sanitizeWeight(total_capacity_weight), std::memory_order_relaxed);
  return true;
}

bool LrhLoadBalancer::Ring::updateWeightsForSameHosts(
    const NormalizedHostWeightVector& normalized_host_weights, bool use_hostname_for_hashing) {
  if (!sameHostsInOrder(normalized_host_weights)) {
    return false;
  }

  absl::InlinedVector<double, 128> capacity_weights;
  capacity_weights.reserve(normalized_host_weights.size());
  for (const auto& entry : normalized_host_weights) {
    const absl::string_view key_to_hash = hashKey(entry.first, use_hostname_for_hashing);
    if (key_to_hash != topology_->host_keys_[capacity_weights.size()]) {
      return false;
    }
    capacity_weights.push_back(entry.second);
  }
  storeCapacityWeights(capacity_weights);
  return true;
}

double LrhLoadBalancer::Ring::capacityWeightAtIndex(size_t host_index) const {
  if (host_index >= host_states_.size()) {
    return MinEffectiveWeight;
  }
  return host_states_[host_index]->capacity_weight_.load(std::memory_order_relaxed);
}

bool LrhLoadBalancer::Ring::sameHostsInOrder(
    const NormalizedHostWeightVector& normalized_host_weights) const {
  if (normalized_host_weights.size() != host_states_.size()) {
    return false;
  }
  for (size_t i = 0; i < normalized_host_weights.size(); ++i) {
    if (normalized_host_weights[i].first != host_states_[i]->host_) {
      return false;
    }
  }
  return true;
}

LrhLoadBalancer::BoundedRing::BoundedRing(RingSharedPtr ring,
                                          const NormalizedHostWeightVector& normalized_host_weights,
                                          uint32_t hash_balance_factor)
    : ring_(std::move(ring)), hash_balance_factor_(hash_balance_factor) {
  ASSERT(ring_ != nullptr);
  ASSERT(hash_balance_factor > 0);
  host_weights_.reserve(normalized_host_weights.size());
  for (const auto& entry : normalized_host_weights) {
    host_index_by_ptr_[entry.first] = host_weights_.size();
    host_weights_.push_back(std::make_shared<HostWeightState>(entry.first));
  }
}

HostSelectionResponse LrhLoadBalancer::BoundedRing::chooseHost(uint64_t hash,
                                                               uint32_t attempt) const {
  if (host_weights_.empty()) {
    return {nullptr};
  }

  HostConstSharedPtr host =
      LoadBalancer::onlyAllowSynchronousHostSelection(ring_->chooseHost(hash, attempt));
  if (host == nullptr) {
    return {nullptr};
  }

  double overload_factor = hostOverloadFactor(*host, normalizedWeightForHost(host));
  if (overload_factor <= 1.0) {
    ENVOY_LOG_MISC(debug, "LrhLoadBalancer::BoundedRing::chooseHost: selected host #{} (attempt:1)",
                   host->address()->asString());
    return host;
  }

  const uint32_t num_hosts = host_weights_.size();
  auto host_index = std::vector<uint32_t>(num_hosts);
  for (uint32_t i = 0; i < num_hosts; i++) {
    host_index[i] = i;
  }

  const uint64_t seed = hash;
  std::mt19937 random(seed);
  auto uniform_int = [](std::mt19937& random, uint32_t k) -> uint32_t {
    uint32_t x = k;
    while (x >= k) {
      x = random() / ((static_cast<uint64_t>(random.max()) + 1u) / k);
    }
    return x;
  };

  HostConstSharedPtr alt_host;
  HostConstSharedPtr least_overloaded_host = host;
  double least_overload_factor = overload_factor;
  for (uint32_t i = 0; i < num_hosts; i++) {
    const uint32_t j = uniform_int(random, num_hosts - i);
    std::swap(host_index[i], host_index[i + j]);

    const uint32_t k = host_index[i];
    const auto& alt_host_state = *host_weights_[k];
    alt_host = alt_host_state.host_;
    if (alt_host == host) {
      continue;
    }

    overload_factor = hostOverloadFactor(*alt_host, ring_->capacityWeightAtIndex(k));
    if (overload_factor <= 1.0) {
      ENVOY_LOG_MISC(debug,
                     "LrhLoadBalancer::BoundedRing::chooseHost: selected host #{}:{} "
                     "(attempt:{})",
                     k, alt_host->address()->asString(), i + 2);
      return alt_host;
    }

    if (least_overload_factor > overload_factor) {
      least_overloaded_host = alt_host;
      least_overload_factor = overload_factor;
    }
  }

  return least_overloaded_host;
}

bool LrhLoadBalancer::BoundedRing::updateWeightsForSameHosts(
    const NormalizedHostWeightVector& normalized_host_weights, bool use_hostname_for_hashing) {
  if (!sameHostsInOrder(normalized_host_weights)) {
    return false;
  }
  if (!ring_->updateWeightsForSameHosts(normalized_host_weights, use_hostname_for_hashing)) {
    return false;
  }
  return true;
}

double LrhLoadBalancer::BoundedRing::hostOverloadFactor(const Host& host, double weight) const {
  const uint32_t overall_active = host.cluster().trafficStats()->upstream_rq_active_.value();
  const uint32_t host_active = host.stats().rq_active_.value();

  const uint32_t total_slots = ((overall_active + 1) * hash_balance_factor_ + 99) / 100;
  const uint32_t slots =
      std::max(static_cast<uint32_t>(std::ceil(total_slots * weight)), static_cast<uint32_t>(1));

  if (host_active > slots) {
    ENVOY_LOG_MISC(debug,
                   "LrhLoadBalancer::BoundedRing::chooseHost: host {} overloaded; "
                   "overall_active {}, host_weight {}, host_active {} > slots {}",
                   host.address()->asString(), overall_active, weight, host_active, slots);
  }
  return static_cast<double>(host_active) / slots;
}

double LrhLoadBalancer::BoundedRing::normalizedWeightForHost(const HostConstSharedPtr& host) const {
  const auto it = host_index_by_ptr_.find(host);
  if (it == host_index_by_ptr_.end()) {
    return 1.0;
  }
  return ring_->capacityWeightAtIndex(it->second);
}

bool LrhLoadBalancer::BoundedRing::sameHostsInOrder(
    const NormalizedHostWeightVector& normalized_host_weights) const {
  if (normalized_host_weights.size() != host_weights_.size()) {
    return false;
  }
  for (size_t i = 0; i < normalized_host_weights.size(); ++i) {
    if (normalized_host_weights[i].first != host_weights_[i]->host_) {
      return false;
    }
  }
  return true;
}

void LrhLoadBalancer::Ring::storeCapacityWeights(absl::Span<const double> capacity_weights) {
  double total_capacity_weight = 0.0;
  for (size_t i = 0; i < capacity_weights.size(); ++i) {
    const double capacity_weight = sanitizeWeight(capacity_weights[i]);
    host_states_[i]->capacity_weight_.store(capacity_weight, std::memory_order_relaxed);
    total_capacity_weight += capacity_weight;
  }
  total_capacity_weight_.store(sanitizeWeight(total_capacity_weight), std::memory_order_relaxed);
}

LrhLoadBalancer::Ring::Ring(const NormalizedHostWeightVector& normalized_host_weights,
                            double /* min_normalized_weight */, uint64_t min_ring_size,
                            uint64_t max_ring_size, uint32_t candidate_count,
                            bool deduplicate_hosts, bool use_hostname_for_hashing,
                            LrhLbProto::WeightPreprocessing weight_preprocessing,
                            TopologySharedPtr cached_topology, LrhLoadBalancerStats& stats)
    : candidate_count_(candidate_count), deduplicate_hosts_(deduplicate_hosts),
      weight_preprocessing_(weight_preprocessing), stats_(stats) {
  std::vector<InitialHostState> initial_host_states;
  initial_host_states.reserve(normalized_host_weights.size());
  host_states_.reserve(normalized_host_weights.size());
  double total_capacity_weight = 0.0;

  for (const auto& entry : normalized_host_weights) {
    const auto& host = entry.first;
    const double capacity_weight = sanitizeWeight(entry.second);
    const absl::string_view key_to_hash = hashKey(host, use_hostname_for_hashing);
    ASSERT(!key_to_hash.empty());
    const std::string hash_key(key_to_hash);
    const uint64_t host_hash = HashUtil::xxHash64(hash_key);

    initial_host_states.push_back({host, hash_key, host_hash, capacity_weight});
    host_states_.push_back(std::make_shared<HostState>(host, host_hash, capacity_weight));
    total_capacity_weight += capacity_weight;
  }
  total_capacity_weight_.store(sanitizeWeight(total_capacity_weight), std::memory_order_relaxed);

  if (cached_topology != nullptr && topologyMatches(*cached_topology, initial_host_states)) {
    topology_ = std::move(cached_topology);
    stats_.topology_cache_hits_.inc();
  } else {
    ENVOY_LOG(trace, "lrh: building ring topology");
    topology_ = buildTopology(initial_host_states, min_ring_size, max_ring_size);
    stats_.topology_cache_misses_.inc();
  }
  ring_ = &topology_->ring_;

  stats_.size_.set(topology_->ring_.size());
  stats_.min_hashes_per_host_.set(topology_->min_hashes_per_host_);
  stats_.max_hashes_per_host_.set(topology_->max_hashes_per_host_);
}

LrhLoadBalancer::Ring::TopologySharedPtr
LrhLoadBalancer::Ring::buildTopology(const std::vector<InitialHostState>& host_states,
                                     uint64_t min_ring_size, uint64_t max_ring_size) {
  auto topology = std::make_shared<Topology>();
  topology->host_keys_.reserve(host_states.size());
  for (const auto& host_state : host_states) {
    topology->host_keys_.push_back(host_state.hash_key_);
  }

  if (host_states.empty()) {
    return topology;
  }

  ASSERT(host_states.size() <= std::numeric_limits<uint32_t>::max());

  const double scale = std::min(
      std::max(static_cast<double>(min_ring_size), static_cast<double>(host_states.size())),
      static_cast<double>(max_ring_size));
  const uint64_t ring_size = std::ceil(scale);
  topology->ring_.reserve(ring_size);

  absl::InlinedVector<char, 196> hash_key_buffer;
  double current_hashes = 0.0;
  double target_hashes = 0.0;
  uint64_t min_hashes_per_host = ring_size;
  uint64_t max_hashes_per_host = 0;

  for (uint32_t host_index = 0; host_index < host_states.size(); ++host_index) {
    const auto& host_state = host_states[host_index];

    hash_key_buffer.assign(host_state.hash_key_.begin(), host_state.hash_key_.end());
    hash_key_buffer.emplace_back('_');
    auto offset_start = hash_key_buffer.end();

    // LRH keeps the ring topology stable and applies dynamic capacity only
    // inside the local rendezvous election, so each host receives equal token
    // share regardless of its current load-balancing weight.
    target_hashes += scale / host_states.size();
    uint64_t hashes_for_host = 0;
    while (current_hashes < target_hashes) {
      const std::string salt = absl::StrCat("", hashes_for_host);
      hash_key_buffer.insert(offset_start, salt.begin(), salt.end());

      absl::string_view hash_key(static_cast<char*>(hash_key_buffer.data()),
                                 hash_key_buffer.size());
      topology->ring_.push_back({HashUtil::xxHash64(hash_key), host_index});

      ++hashes_for_host;
      ++current_hashes;
      hash_key_buffer.erase(offset_start, hash_key_buffer.end());
    }

    min_hashes_per_host = std::min(hashes_for_host, min_hashes_per_host);
    max_hashes_per_host = std::max(hashes_for_host, max_hashes_per_host);
  }

  std::sort(
      topology->ring_.begin(), topology->ring_.end(),
      [](const RingEntry& lhs, const RingEntry& rhs) -> bool { return lhs.hash_ < rhs.hash_; });

  topology->min_hashes_per_host_ = min_hashes_per_host;
  topology->max_hashes_per_host_ = max_hashes_per_host;
  return topology;
}

bool LrhLoadBalancer::Ring::topologyMatches(const Topology& topology,
                                            const std::vector<InitialHostState>& host_states) {
  if (topology.host_keys_.size() != host_states.size()) {
    return false;
  }
  for (size_t i = 0; i < host_states.size(); ++i) {
    if (topology.host_keys_[i] != host_states[i].hash_key_) {
      return false;
    }
  }
  return true;
}

} // namespace Upstream
} // namespace Envoy
