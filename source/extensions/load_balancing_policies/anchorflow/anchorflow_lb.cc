#include "source/extensions/load_balancing_policies/anchorflow/anchorflow_lb.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "envoy/config/cluster/v3/cluster.pb.h"

#include "source/common/common/assert.h"
#include "source/common/common/hash.h"

namespace Envoy {
namespace Upstream {

struct AnchorFlowLoadBalancer::Table::Topology {
  AnchorHashMap anchor_;
  std::vector<uint32_t> token_owner_;
  std::vector<double> anchor_shares_;
  std::vector<std::string> host_keys_;
  uint32_t tokens_per_host_{0};
  uint64_t configured_anchor_buckets_{0};
};

TypedAnchorFlowLbConfig::TypedAnchorFlowLbConfig(const AnchorFlowLbProto& lb_config,
                                                 Regex::Engine& regex_engine,
                                                 absl::Status& creation_status)
    : TypedHashLbConfigBase(lb_config.consistent_hashing_lb_config().hash_policy(), regex_engine,
                            creation_status),
      lb_config_(lb_config) {}

AnchorFlowLoadBalancer::AnchorFlowLoadBalancer(
    const PrioritySet& priority_set, ClusterLbStats& stats, Stats::Scope& scope,
    Runtime::Loader& runtime, Random::RandomGenerator& random, uint32_t healthy_panic_threshold,
    const AnchorFlowLbProto& config, HashPolicySharedPtr hash_policy)
    : ThreadAwareLoadBalancerBase(priority_set, stats, runtime, random, healthy_panic_threshold,
                                  config.has_locality_weighted_lb_config(), std::move(hash_policy)),
      scope_(scope.createScope("anchorflow_lb.")), stats_(generateStats(*scope_)),
      tokens_per_host_(
          PROTOBUF_GET_WRAPPED_OR_DEFAULT(config, tokens_per_host, DefaultTokensPerHost)),
      anchor_buckets_(config.has_anchor_buckets()
                          ? absl::make_optional(config.anchor_buckets().value())
                          : absl::nullopt),
      use_hostname_for_hashing_(config.consistent_hashing_lb_config().use_hostname_for_hashing()),
      movement_seed_(PROTOBUF_GET_WRAPPED_OR_DEFAULT(config, movement_seed, DefaultMovementSeed)),
      receiver_seed_(PROTOBUF_GET_WRAPPED_OR_DEFAULT(config, receiver_seed, DefaultReceiverSeed)),
      hash_balance_factor_(PROTOBUF_GET_WRAPPED_OR_DEFAULT(config.consistent_hashing_lb_config(),
                                                           hash_balance_factor, 0)) {
  if (tokens_per_host_ == 0 || tokens_per_host_ > MaxTokensPerHost) {
    throw EnvoyException(fmt::format("anchorflow: tokens_per_host ({}) must be in range [1, {}]",
                                     tokens_per_host_, MaxTokensPerHost));
  }
  if (anchor_buckets_.has_value() && anchor_buckets_.value() > MaxAnchorBuckets) {
    throw EnvoyException(fmt::format("anchorflow: anchor_buckets ({}) exceeds {}",
                                     anchor_buckets_.value(), MaxAnchorBuckets));
  }
}

ThreadAwareLoadBalancerBase::HashingLoadBalancerSharedPtr
AnchorFlowLoadBalancer::createLoadBalancer(
    const NormalizedHostWeightVector& normalized_host_weights, double /* min_normalized_weight */,
    double /* max_normalized_weight */) {
  auto table = std::make_shared<Table>(normalized_host_weights, tokens_per_host_, anchor_buckets_,
                                       use_hostname_for_hashing_, movement_seed_, receiver_seed_,
                                       cached_topology_, stats_);
  cached_topology_ = table->topology();
  latest_table_for_test_ = table;

  HashingLoadBalancerSharedPtr anchorflow_lb = table;
  if (hash_balance_factor_ == 0) {
    return anchorflow_lb;
  }

  return std::make_shared<BoundedLoadHashingLoadBalancer>(
      anchorflow_lb, std::move(normalized_host_weights), hash_balance_factor_);
}

AnchorFlowLoadBalancerStats AnchorFlowLoadBalancer::generateStats(Stats::Scope& scope) {
  return {ALL_ANCHORFLOW_LOAD_BALANCER_STATS(POOL_COUNTER(scope), POOL_GAUGE(scope))};
}

bool AnchorFlowLoadBalancer::updateWeightsForTest(
    absl::Span<const double> capacity_weights_by_host) {
  auto table = latest_table_for_test_.lock();
  return table != nullptr && table->updateWeightsForTest(capacity_weights_by_host);
}

AnchorFlowLoadBalancer::Table::AnchorHashMap::AnchorHashMap(uint32_t token_count,
                                                            uint64_t anchor_buckets)
    : anchor_len_(static_cast<uint32_t>(anchor_buckets)), working_len_(token_count),
      a_(anchor_len_, 0), k_(anchor_len_), resource_by_bucket_(anchor_len_, InvalidResource) {
  ASSERT(anchor_buckets <= std::numeric_limits<uint32_t>::max());
  ASSERT(token_count <= anchor_buckets);

  for (uint32_t bucket = 0; bucket < anchor_len_; ++bucket) {
    k_[bucket] = bucket;
  }

  for (uint32_t bucket = anchor_len_; bucket > working_len_; --bucket) {
    const uint32_t removed_bucket = bucket - 1;
    a_[removed_bucket] = removed_bucket;
  }

  for (uint32_t token = 0; token < working_len_; ++token) {
    resource_by_bucket_[token] = token;
  }
}

uint32_t AnchorFlowLoadBalancer::Table::AnchorHashMap::assign(uint64_t key) const {
  if (working_len_ == 0) {
    return InvalidResource;
  }

  uint32_t bucket = bucketForHash(key);
  while (a_[bucket] > 0) {
    const uint32_t removed_size = a_[bucket];
    uint32_t next = removedSetHash(key, bucket, removed_size);

    while (a_[next] >= a_[bucket]) {
      next = k_[next];
    }
    bucket = next;
  }

  return resource_by_bucket_[bucket];
}

uint32_t AnchorFlowLoadBalancer::Table::AnchorHashMap::bucketForHash(uint64_t key) const {
  return static_cast<uint32_t>(HashUtil::xxHash64Value(key, AnchorBucketHashTag) %
                               static_cast<uint64_t>(anchor_len_));
}

uint32_t AnchorFlowLoadBalancer::Table::AnchorHashMap::removedSetHash(uint64_t key, uint32_t bucket,
                                                                      uint32_t modulus) const {
  const uint64_t seed = AnchorRemovedHashTag ^ static_cast<uint64_t>(bucket + 1);
  return static_cast<uint32_t>(HashUtil::xxHash64Value(key, seed) % modulus);
}

std::vector<AnchorFlowLoadBalancer::Table::HostState>
AnchorFlowLoadBalancer::Table::sortedHostStates(
    const NormalizedHostWeightVector& normalized_host_weights,
    bool use_hostname_for_hashing) const {
  std::vector<HostState> host_states;
  host_states.reserve(normalized_host_weights.size());
  uint32_t original_host_index = 0;
  for (const auto& entry : normalized_host_weights) {
    const absl::string_view key_to_hash = hashKey(entry.first, use_hostname_for_hashing);
    ASSERT(!key_to_hash.empty());
    host_states.emplace_back(entry.first, std::string(key_to_hash), entry.second,
                             original_host_index++);
  }

  std::sort(host_states.begin(), host_states.end(), [](const HostState& lhs, const HostState& rhs) {
    return lhs.hash_key_ < rhs.hash_key_;
  });
  return host_states;
}

AnchorFlowLoadBalancer::Table::TopologySharedPtr
AnchorFlowLoadBalancer::Table::buildTopology(const std::vector<HostState>& host_states,
                                             uint32_t tokens_per_host,
                                             absl::optional<uint64_t> configured_anchor_buckets) {
  auto topology = std::make_shared<Topology>();
  topology->tokens_per_host_ = tokens_per_host;
  topology->host_keys_.reserve(host_states.size());
  for (const auto& host_state : host_states) {
    topology->host_keys_.push_back(host_state.hash_key_);
  }

  if (host_states.empty()) {
    return topology;
  }

  const uint64_t token_count =
      static_cast<uint64_t>(host_states.size()) * static_cast<uint64_t>(tokens_per_host);
  if (token_count > std::numeric_limits<uint32_t>::max()) {
    throw EnvoyException(
        fmt::format("anchorflow: token_count ({}) exceeds u32 limit", token_count));
  }

  const uint64_t anchor_buckets = configured_anchor_buckets.value_or(token_count);
  topology->configured_anchor_buckets_ = configured_anchor_buckets.value_or(0);
  if (anchor_buckets < token_count) {
    throw EnvoyException(fmt::format("anchorflow: anchor_buckets ({}) must be >= token_count ({})",
                                     anchor_buckets, token_count));
  }
  if (anchor_buckets > AnchorFlowLoadBalancer::MaxAnchorBuckets) {
    throw EnvoyException(fmt::format("anchorflow: anchor_buckets ({}) exceeds {}", anchor_buckets,
                                     AnchorFlowLoadBalancer::MaxAnchorBuckets));
  }

  topology->token_owner_.reserve(token_count);
  topology->anchor_shares_.assign(host_states.size(), 0.0);
  for (uint32_t host_index = 0; host_index < host_states.size(); ++host_index) {
    for (uint32_t token = 0; token < tokens_per_host; ++token) {
      topology->token_owner_.push_back(host_index);
    }
    topology->anchor_shares_[host_index] =
        static_cast<double>(tokens_per_host) / static_cast<double>(token_count);
  }
  topology->anchor_ = AnchorHashMap(static_cast<uint32_t>(token_count), anchor_buckets);
  return topology;
}

bool AnchorFlowLoadBalancer::Table::topologyMatches(
    const Topology& topology, const std::vector<HostState>& host_states, uint32_t tokens_per_host,
    absl::optional<uint64_t> configured_anchor_buckets) {
  if (topology.tokens_per_host_ != tokens_per_host ||
      topology.configured_anchor_buckets_ != configured_anchor_buckets.value_or(0) ||
      topology.host_keys_.size() != host_states.size()) {
    return false;
  }

  for (size_t i = 0; i < host_states.size(); ++i) {
    if (topology.host_keys_[i] != host_states[i].hash_key_) {
      return false;
    }
  }
  return true;
}

AnchorFlowLoadBalancer::Table::CapacityState
AnchorFlowLoadBalancer::Table::buildCapacityState(const Topology& topology,
                                                  const std::vector<HostState>& host_states) {
  CapacityState state;
  const size_t host_count = host_states.size();
  state.target_.assign(host_count, 0.0);
  state.alpha_.assign(host_count, 0.0);
  state.rho_.assign(host_count, 0.0);

  if (host_count == 0) {
    return state;
  }

  double total_weight = 0.0;
  for (const auto& host_state : host_states) {
    if (std::isfinite(host_state.target_weight_) && host_state.target_weight_ > 0.0) {
      total_weight += host_state.target_weight_;
    }
  }
  if (total_weight <= NormalizationTolerance) {
    return state;
  }

  std::vector<double> sigma(host_count, 0.0);
  std::vector<double> delta(host_count, 0.0);
  double delta_sum = 0.0;

  for (size_t host = 0; host < host_count; ++host) {
    state.target_[host] = host_states[host].target_weight_ / total_weight;
    const double anchor_share = topology.anchor_shares_[host];
    if (anchor_share > state.target_[host] + NormalizationTolerance) {
      sigma[host] = anchor_share - state.target_[host];
      state.alpha_[host] = sigma[host] / anchor_share;
    } else if (state.target_[host] > anchor_share + NormalizationTolerance) {
      delta[host] = state.target_[host] - anchor_share;
      delta_sum += delta[host];
    }
  }

  state.residual_mass_ = delta_sum <= NormalizationTolerance ? 0.0 : delta_sum;
  if (state.residual_mass_ <= NormalizationTolerance) {
    return state;
  }

  double cumulative = 0.0;
  for (size_t host = 0; host < host_count; ++host) {
    if (delta[host] <= NormalizationTolerance) {
      continue;
    }
    state.rho_[host] = delta[host] / delta_sum;
    cumulative += state.rho_[host];
    state.cumulative_receivers_.push_back({static_cast<uint32_t>(host), cumulative});
  }
  if (!state.cumulative_receivers_.empty()) {
    state.cumulative_receivers_.back().second = 1.0;
  }
  return state;
}

AnchorFlowLoadBalancer::Table::Table(const NormalizedHostWeightVector& normalized_host_weights,
                                     uint32_t tokens_per_host,
                                     absl::optional<uint64_t> configured_anchor_buckets,
                                     bool use_hostname_for_hashing, uint64_t movement_seed,
                                     uint64_t receiver_seed, TopologySharedPtr cached_topology,
                                     AnchorFlowLoadBalancerStats& stats)
    : movement_seed_(movement_seed), receiver_seed_(receiver_seed), stats_(stats) {
  host_states_ = sortedHostStates(normalized_host_weights, use_hostname_for_hashing);
  if (cached_topology != nullptr &&
      topologyMatches(*cached_topology, host_states_, tokens_per_host, configured_anchor_buckets)) {
    topology_ = std::move(cached_topology);
    stats_.topology_cache_hits_.inc();
  } else {
    ENVOY_LOG(trace, "anchorflow: building token topology");
    topology_ = buildTopology(host_states_, tokens_per_host, configured_anchor_buckets);
    stats_.topology_cache_misses_.inc();
  }

  capacity_ = buildCapacityState(*topology_, host_states_);
  stats_.tokens_.set(topology_->token_owner_.size());
  stats_.anchor_buckets_.set(topology_->anchor_.anchorLen());
}

size_t AnchorFlowLoadBalancer::Table::tokenCountForTest() const {
  return topology_->token_owner_.size();
}

size_t AnchorFlowLoadBalancer::Table::anchorBucketCountForTest() const {
  return topology_->anchor_.anchorLen();
}

bool AnchorFlowLoadBalancer::Table::updateWeightsForTest(
    absl::Span<const double> capacity_weights_by_host) {
  if (capacity_weights_by_host.size() != host_states_.size()) {
    return false;
  }

  for (auto& host_state : host_states_) {
    if (host_state.original_host_index_ >= capacity_weights_by_host.size()) {
      return false;
    }
    host_state.target_weight_ = capacity_weights_by_host[host_state.original_host_index_];
  }
  capacity_ = buildCapacityState(*topology_, host_states_);
  return true;
}

HostSelectionResponse AnchorFlowLoadBalancer::Table::chooseHost(uint64_t hash,
                                                                uint32_t attempt) const {
  if (host_states_.empty() || topology_->token_owner_.empty()) {
    return {nullptr};
  }

  const uint64_t selection_hash =
      attempt == 0 ? hash : HashUtil::xxHash64Value(hash, AttemptHashTag ^ attempt);
  const uint32_t token = topology_->anchor_.assign(selection_hash);
  if (token >= topology_->token_owner_.size()) {
    return {nullptr};
  }

  const uint32_t anchor_owner = topology_->token_owner_[token];
  uint32_t final_owner = anchor_owner;

  if (isSurplus(anchor_owner) &&
      movementValue(selection_hash, anchor_owner) < capacity_.alpha_[anchor_owner]) {
    final_owner = sampleReceiver(selection_hash, anchor_owner);
  }

  if (final_owner >= host_states_.size()) {
    return {nullptr};
  }
  return {host_states_[final_owner].host_};
}

bool AnchorFlowLoadBalancer::Table::isSurplus(uint32_t owner) const {
  return owner < capacity_.alpha_.size() && capacity_.alpha_[owner] > 0.0 &&
         !capacity_.cumulative_receivers_.empty();
}

double AnchorFlowLoadBalancer::Table::movementValue(uint64_t hash, uint32_t donor) const {
  return unitInterval(domainHash(movement_seed_, hash, donor, MovementHashTag));
}

uint32_t AnchorFlowLoadBalancer::Table::sampleReceiver(uint64_t hash, uint32_t donor) const {
  const double sample = unitInterval(domainHash(receiver_seed_, hash, donor, ReceiverHashTag));
  for (const auto& receiver : capacity_.cumulative_receivers_) {
    if (sample < receiver.second) {
      return receiver.first;
    }
  }

  return capacity_.cumulative_receivers_.empty() ? donor
                                                 : capacity_.cumulative_receivers_.back().first;
}

double AnchorFlowLoadBalancer::Table::unitInterval(uint64_t hash) {
  constexpr double Inv2Pow53 = 1.0 / 9007199254740992.0;
  return static_cast<double>(hash >> 11) * Inv2Pow53;
}

uint64_t AnchorFlowLoadBalancer::Table::domainHash(uint64_t seed, uint64_t key, uint32_t owner,
                                                   uint64_t tag) {
  uint64_t h = HashUtil::xxHash64Value(key, seed ^ tag);
  h = HashUtil::xxHash64Value(static_cast<uint64_t>(owner), h);
  return h;
}

} // namespace Upstream
} // namespace Envoy
