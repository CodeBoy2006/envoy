#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "envoy/config/cluster/v3/cluster.pb.h"
#include "envoy/extensions/load_balancing_policies/anchorflow/v3/anchorflow.pb.h"
#include "envoy/extensions/load_balancing_policies/anchorflow/v3/anchorflow.pb.validate.h"
#include "envoy/runtime/runtime.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"
#include "envoy/upstream/load_balancer.h"

#include "source/common/common/logger.h"
#include "source/extensions/load_balancing_policies/common/thread_aware_lb_impl.h"

#include "absl/types/optional.h"
#include "absl/types/span.h"

namespace Envoy {
namespace Upstream {

using AnchorFlowLbProto = envoy::extensions::load_balancing_policies::anchorflow::v3::AnchorFlow;
using ClusterProto = envoy::config::cluster::v3::Cluster;

class TypedAnchorFlowLbConfig : public Upstream::TypedHashLbConfigBase {
public:
  TypedAnchorFlowLbConfig(const AnchorFlowLbProto& lb_config, Regex::Engine& regex_engine,
                          absl::Status& creation_status);

  AnchorFlowLbProto lb_config_;
};

#define ALL_ANCHORFLOW_LOAD_BALANCER_STATS(COUNTER, GAUGE)                                         \
  COUNTER(topology_cache_hits)                                                                     \
  COUNTER(topology_cache_misses)                                                                   \
  GAUGE(anchor_buckets, Accumulate)                                                                \
  GAUGE(tokens, Accumulate)

struct AnchorFlowLoadBalancerStats {
  ALL_ANCHORFLOW_LOAD_BALANCER_STATS(GENERATE_COUNTER_STRUCT, GENERATE_GAUGE_STRUCT)
};

class AnchorFlowLoadBalancer : public ThreadAwareLoadBalancerBase {
public:
  AnchorFlowLoadBalancer(const PrioritySet& priority_set, ClusterLbStats& stats,
                         Stats::Scope& scope, Runtime::Loader& runtime,
                         Random::RandomGenerator& random, uint32_t healthy_panic_threshold,
                         const AnchorFlowLbProto& config, HashPolicySharedPtr hash_policy);

  const AnchorFlowLoadBalancerStats& stats() const { return stats_; }

  static AnchorFlowLoadBalancerStats generateStats(Stats::Scope& scope);
  bool updateWeightsForTest(absl::Span<const double> capacity_weights_by_host);

  static constexpr uint32_t DefaultTokensPerHost = 64;
  static constexpr uint32_t MaxTokensPerHost = 4096;
  static constexpr uint64_t DefaultMovementSeed = 0xA807D6C25F394E11ULL;
  static constexpr uint64_t DefaultReceiverSeed = 0xC2416F918D7B2A35ULL;
  static constexpr uint64_t MaxAnchorBuckets = 1024 * 1024 * 8;

private:
  class Table : public HashingLoadBalancer {
  public:
    struct Topology;
    using TopologySharedPtr = std::shared_ptr<const Topology>;

    Table(const NormalizedHostWeightVector& normalized_host_weights, uint32_t tokens_per_host,
          absl::optional<uint64_t> configured_anchor_buckets, bool use_hostname_for_hashing,
          uint64_t movement_seed, uint64_t receiver_seed, TopologySharedPtr cached_topology,
          AnchorFlowLoadBalancerStats& stats);

    HostSelectionResponse chooseHost(uint64_t hash, uint32_t attempt) const override;

    TopologySharedPtr topology() const { return topology_; }
    size_t tokenCountForTest() const;
    size_t anchorBucketCountForTest() const;
    bool updateWeightsForTest(absl::Span<const double> capacity_weights_by_host);

  private:
    class AnchorHashMap {
    public:
      AnchorHashMap() = default;
      AnchorHashMap(uint32_t token_count, uint64_t anchor_buckets);

      uint32_t assign(uint64_t key) const;
      size_t anchorLen() const { return anchor_len_; }

    private:
      static constexpr uint32_t InvalidResource = std::numeric_limits<uint32_t>::max();

      uint32_t bucketForHash(uint64_t key) const;
      uint32_t removedSetHash(uint64_t key, uint32_t bucket, uint32_t modulus) const;

      uint32_t anchor_len_{0};
      uint32_t working_len_{0};
      std::vector<uint32_t> a_;
      std::vector<uint32_t> k_;
      std::vector<uint32_t> resource_by_bucket_;
    };

    struct HostState {
      HostState(HostConstSharedPtr host, std::string hash_key, double target_weight,
                uint32_t original_host_index)
          : host_(std::move(host)), hash_key_(std::move(hash_key)), target_weight_(target_weight),
            original_host_index_(original_host_index) {}

      HostConstSharedPtr host_;
      std::string hash_key_;
      double target_weight_;
      uint32_t original_host_index_;
    };

    struct CapacityState {
      std::vector<double> target_;
      std::vector<double> alpha_;
      std::vector<double> rho_;
      std::vector<std::pair<uint32_t, double>> cumulative_receivers_;
      double residual_mass_{0.0};
    };

    static constexpr double NormalizationTolerance = 1.0e-9;
    static constexpr uint64_t MovementHashTag = 0xA807D6C25F394E11ULL;
    static constexpr uint64_t ReceiverHashTag = 0xC2416F918D7B2A35ULL;
    static constexpr uint64_t AttemptHashTag = 0x6F5D9B71C3A20E4BULL;
    static constexpr uint64_t AnchorBucketHashTag = 0x9E3779B97F4A7C15ULL;
    static constexpr uint64_t AnchorRemovedHashTag = 0xD1B54A32D192ED03ULL;

    std::vector<HostState>
    sortedHostStates(const NormalizedHostWeightVector& normalized_host_weights,
                     bool use_hostname_for_hashing) const;
    static TopologySharedPtr buildTopology(const std::vector<HostState>& host_states,
                                           uint32_t tokens_per_host,
                                           absl::optional<uint64_t> configured_anchor_buckets);
    static bool topologyMatches(const Topology& topology, const std::vector<HostState>& host_states,
                                uint32_t tokens_per_host,
                                absl::optional<uint64_t> configured_anchor_buckets);
    static CapacityState buildCapacityState(const Topology& topology,
                                            const std::vector<HostState>& host_states);
    static double unitInterval(uint64_t hash);
    static uint64_t domainHash(uint64_t seed, uint64_t key, uint32_t owner, uint64_t tag);

    bool isSurplus(uint32_t owner) const;
    double movementValue(uint64_t hash, uint32_t donor) const;
    uint32_t sampleReceiver(uint64_t hash, uint32_t donor) const;

    TopologySharedPtr topology_;
    std::vector<HostState> host_states_;
    CapacityState capacity_;
    const uint64_t movement_seed_;
    const uint64_t receiver_seed_;
    AnchorFlowLoadBalancerStats& stats_;
  };

  HashingLoadBalancerSharedPtr
  createLoadBalancer(const NormalizedHostWeightVector& normalized_host_weights,
                     double /* min_normalized_weight */,
                     double /* max_normalized_weight */) override;

  Stats::ScopeSharedPtr scope_;
  AnchorFlowLoadBalancerStats stats_;
  const uint32_t tokens_per_host_;
  const absl::optional<uint64_t> anchor_buckets_;
  const bool use_hostname_for_hashing_;
  const uint64_t movement_seed_;
  const uint64_t receiver_seed_;
  const uint32_t hash_balance_factor_;
  Table::TopologySharedPtr cached_topology_;
  std::weak_ptr<Table> latest_table_for_test_;
};

} // namespace Upstream
} // namespace Envoy
