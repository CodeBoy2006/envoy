#include "source/extensions/load_balancing_policies/lrh/lrh_lb.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
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
  HashingLoadBalancerSharedPtr lrh_lb = std::make_shared<Ring>(
      normalized_host_weights, min_normalized_weight, min_ring_size_, max_ring_size_,
      candidate_count_, deduplicate_hosts_, use_hostname_for_hashing_, stats_);

  if (hash_balance_factor_ == 0) {
    return lrh_lb;
  }

  return std::make_shared<BoundedLoadHashingLoadBalancer>(
      lrh_lb, std::move(normalized_host_weights), hash_balance_factor_);
}

LrhLoadBalancerStats LrhLoadBalancer::generateStats(Stats::Scope& scope) {
  return {ALL_LRH_LOAD_BALANCER_STATS(POOL_GAUGE(scope))};
}

size_t LrhLoadBalancer::Ring::lowerBound(uint64_t hash) const {
  auto it =
      std::lower_bound(ring_.begin(), ring_.end(), hash,
                       [](const RingEntry& entry, uint64_t value) { return entry.hash_ < value; });
  if (it == ring_.end()) {
    return 0;
  }
  return std::distance(ring_.begin(), it);
}

double LrhLoadBalancer::Ring::weightedScore(uint64_t request_hash, const RingEntry& entry) {
  const uint64_t score_hash = HashUtil::xxHash64Value(request_hash, entry.host_hash_);

  // Map to (0, 1] and use exponential-race scoring. Lower scores win.
  const double u = (static_cast<double>(score_hash) + 1.0) /
                   (static_cast<double>(std::numeric_limits<uint64_t>::max()) + 1.0);
  return -std::log(u) / entry.normalized_weight_;
}

const LrhLoadBalancer::RingEntry*
LrhLoadBalancer::Ring::bestInCandidateBlock(uint64_t hash, size_t start, size_t max_slots,
                                            size_t& walked) const {
  ASSERT(!ring_.empty());

  const RingEntry* best = nullptr;
  double best_score = std::numeric_limits<double>::infinity();
  absl::InlinedVector<const Host*, MaxCandidateCount> seen_hosts;

  size_t candidates = 0;
  walked = 0;
  while (walked < max_slots && candidates < candidate_count_) {
    const RingEntry& entry = ring_[(start + walked) % ring_.size()];
    ++walked;

    if (deduplicate_hosts_) {
      const Host* host = entry.host_.get();
      if (std::find(seen_hosts.begin(), seen_hosts.end(), host) != seen_hosts.end()) {
        continue;
      }
      seen_hosts.push_back(host);
    }
    ++candidates;

    const double score = weightedScore(hash, entry);
    if (score < best_score) {
      best = &entry;
      best_score = score;
    }
  }

  return best;
}

HostSelectionResponse LrhLoadBalancer::Ring::chooseHost(uint64_t hash, uint32_t attempt) const {
  if (ring_.empty()) {
    return {nullptr};
  }

  const size_t ring_size = ring_.size();
  size_t start = (lowerBound(hash) + static_cast<size_t>(attempt) * candidate_count_) % ring_size;
  size_t remaining = ring_size;

  while (remaining > 0) {
    size_t walked = 0;
    const RingEntry* best = bestInCandidateBlock(hash, start, remaining, walked);
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

LrhLoadBalancer::Ring::Ring(const NormalizedHostWeightVector& normalized_host_weights,
                            double /* min_normalized_weight */, uint64_t min_ring_size,
                            uint64_t max_ring_size, uint32_t candidate_count,
                            bool deduplicate_hosts, bool use_hostname_for_hashing,
                            LrhLoadBalancerStats& stats)
    : candidate_count_(candidate_count), deduplicate_hosts_(deduplicate_hosts), stats_(stats) {
  ENVOY_LOG(trace, "lrh: building ring");

  if (normalized_host_weights.empty()) {
    return;
  }

  const double scale = std::min(std::max(static_cast<double>(min_ring_size),
                                         static_cast<double>(normalized_host_weights.size())),
                                static_cast<double>(max_ring_size));
  const uint64_t ring_size = std::ceil(scale);
  ring_.reserve(ring_size);

  absl::InlinedVector<char, 196> hash_key_buffer;
  double current_hashes = 0.0;
  double target_hashes = 0.0;
  uint64_t min_hashes_per_host = ring_size;
  uint64_t max_hashes_per_host = 0;

  for (const auto& entry : normalized_host_weights) {
    const auto& host = entry.first;
    const double normalized_weight = std::max(entry.second, std::numeric_limits<double>::min());
    const absl::string_view key_to_hash = hashKey(host, use_hostname_for_hashing);
    ASSERT(!key_to_hash.empty());
    const uint64_t host_hash = HashUtil::xxHash64(key_to_hash);

    hash_key_buffer.assign(key_to_hash.begin(), key_to_hash.end());
    hash_key_buffer.emplace_back('_');
    auto offset_start = hash_key_buffer.end();

    // LRH keeps the ring topology stable and applies dynamic capacity only
    // inside the local rendezvous election, so each host receives equal token
    // share regardless of its current load-balancing weight.
    target_hashes += scale / normalized_host_weights.size();
    uint64_t hashes_for_host = 0;
    while (current_hashes < target_hashes) {
      const std::string salt = absl::StrCat("", hashes_for_host);
      hash_key_buffer.insert(offset_start, salt.begin(), salt.end());

      absl::string_view hash_key(static_cast<char*>(hash_key_buffer.data()),
                                 hash_key_buffer.size());
      ring_.push_back({HashUtil::xxHash64(hash_key), host, normalized_weight, host_hash});

      ++hashes_for_host;
      ++current_hashes;
      hash_key_buffer.erase(offset_start, hash_key_buffer.end());
    }

    min_hashes_per_host = std::min(hashes_for_host, min_hashes_per_host);
    max_hashes_per_host = std::max(hashes_for_host, max_hashes_per_host);
  }

  std::sort(ring_.begin(), ring_.end(), [](const RingEntry& lhs, const RingEntry& rhs) -> bool {
    return lhs.hash_ < rhs.hash_;
  });

  stats_.size_.set(ring_.size());
  stats_.min_hashes_per_host_.set(min_hashes_per_host);
  stats_.max_hashes_per_host_.set(max_hashes_per_host);
}

} // namespace Upstream
} // namespace Envoy
