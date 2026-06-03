#pragma once

#include <atomic>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "envoy/config/cluster/v3/cluster.pb.h"
#include "envoy/extensions/load_balancing_policies/lrh/v3/lrh.pb.h"
#include "envoy/extensions/load_balancing_policies/lrh/v3/lrh.pb.validate.h"
#include "envoy/runtime/runtime.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"
#include "envoy/upstream/load_balancer.h"

#include "source/common/common/logger.h"
#include "source/extensions/load_balancing_policies/common/thread_aware_lb_impl.h"

#include "absl/types/span.h"

namespace Envoy {
namespace Upstream {

using LrhLbProto = envoy::extensions::load_balancing_policies::lrh::v3::LocalRendezvousHashing;
using ClusterProto = envoy::config::cluster::v3::Cluster;

/**
 * Parsed typed LRH load balancer config.
 */
class TypedLrhLbConfig : public Upstream::TypedHashLbConfigBase {
public:
  TypedLrhLbConfig(const LrhLbProto& lb_config, Regex::Engine& regex_engine,
                   absl::Status& creation_status);

  LrhLbProto lb_config_;
};

/**
 * All LRH load balancer stats. @see stats_macros.h
 */
#define ALL_LRH_LOAD_BALANCER_STATS(COUNTER, GAUGE)                                                \
  COUNTER(topology_cache_hits)                                                                     \
  COUNTER(topology_cache_misses)                                                                   \
  COUNTER(weight_only_updates)                                                                     \
  GAUGE(max_hashes_per_host, Accumulate)                                                           \
  GAUGE(min_hashes_per_host, Accumulate)                                                           \
  GAUGE(size, Accumulate)

/**
 * Struct definition for all LRH load balancer stats. @see stats_macros.h
 */
struct LrhLoadBalancerStats {
  ALL_LRH_LOAD_BALANCER_STATS(GENERATE_COUNTER_STRUCT, GENERATE_GAUGE_STRUCT)
};

/**
 * A thread-aware load balancer implementing Local Rendezvous Hashing:
 * lower-bound into a stable token ring, scan a local candidate window, then
 * elect the winner with weighted rendezvous scoring.
 */
class LrhLoadBalancer : public ThreadAwareLoadBalancerBase {
public:
  LrhLoadBalancer(const PrioritySet& priority_set, ClusterLbStats& stats, Stats::Scope& scope,
                  Runtime::Loader& runtime, Random::RandomGenerator& random,
                  uint32_t healthy_panic_threshold, const LrhLbProto& config,
                  HashPolicySharedPtr hash_policy);

  const LrhLoadBalancerStats& stats() const { return stats_; }

  static LrhLoadBalancerStats generateStats(Stats::Scope& scope);
  bool updateWeightsForTest(absl::Span<const double> capacity_weights_by_host);
  bool updateHostWeightsForTest(absl::Span<const std::pair<uint32_t, double>> weight_updates);

  static constexpr uint32_t DefaultCandidateCount = 8;
  static constexpr uint32_t MaxCandidateCount = 64;
  static constexpr uint64_t DefaultMinRingSize = 1024;
  static constexpr uint64_t DefaultMaxRingSize = 1024 * 1024 * 8;

private:
  struct RingEntry {
    uint64_t hash_;
    uint32_t host_index_;
  };

  class Ring : public HashingLoadBalancer {
  public:
    struct Topology;
    using TopologySharedPtr = std::shared_ptr<const Topology>;

    Ring(const NormalizedHostWeightVector& normalized_host_weights, double min_normalized_weight,
         uint64_t min_ring_size, uint64_t max_ring_size, uint32_t candidate_count,
         bool deduplicate_hosts, bool use_hostname_for_hashing,
         LrhLbProto::WeightPreprocessing weight_preprocessing, TopologySharedPtr cached_topology,
         LrhLoadBalancerStats& stats);

    // ThreadAwareLoadBalancerBase::HashingLoadBalancer
    HostSelectionResponse chooseHost(uint64_t hash, uint32_t attempt) const override;

    size_t ringSizeForTest() const { return ring_->size(); }
    size_t candidateCountForTest() const { return candidate_count_; }
    TopologySharedPtr topology() const { return topology_; }
    double capacityWeightAtIndex(size_t host_index) const;
    bool updateWeightsForSameHosts(const NormalizedHostWeightVector& normalized_host_weights,
                                   bool use_hostname_for_hashing);
    bool updateWeightsForTest(absl::Span<const double> capacity_weights_by_host);
    bool updateHostWeightsForTest(absl::Span<const std::pair<uint32_t, double>> weight_updates);

  private:
    struct HostState {
      HostState(HostConstSharedPtr host, uint64_t host_hash, double capacity_weight)
          : host_(std::move(host)), host_hash_(host_hash), capacity_weight_(capacity_weight) {}

      HostConstSharedPtr host_;
      uint64_t host_hash_;
      std::atomic<double> capacity_weight_;
    };
    using HostStateSharedPtr = std::shared_ptr<HostState>;

    struct InitialHostState {
      HostConstSharedPtr host_;
      std::string hash_key_;
      uint64_t host_hash_;
      double capacity_weight_;
    };

    static constexpr double MinEffectiveWeight = std::numeric_limits<double>::min();
    static constexpr double WindowDebiasedEpsilon = 1.0e-9;

    size_t lowerBound(uint64_t hash) const;
    const HostState* bestInCandidateBlock(uint64_t hash, size_t start, size_t max_slots,
                                          size_t& walked) const;
    double weightedScore(uint64_t request_hash, const HostState& host_state) const;
    static TopologySharedPtr buildTopology(const std::vector<InitialHostState>& host_states,
                                           uint64_t min_ring_size, uint64_t max_ring_size);
    static bool topologyMatches(const Topology& topology,
                                const std::vector<InitialHostState>& host_states);
    bool sameHostsInOrder(const NormalizedHostWeightVector& normalized_host_weights) const;
    void storeCapacityWeights(absl::Span<const double> capacity_weights);
    static double sanitizeWeight(double effective_weight);
    static double windowDebiasedWeight(double capacity_weight, double total_capacity_weight,
                                       size_t host_count, uint32_t candidate_count);

    TopologySharedPtr topology_;
    const std::vector<RingEntry>* ring_{};
    std::vector<HostStateSharedPtr> host_states_;
    std::atomic<double> total_capacity_weight_{MinEffectiveWeight};
    const uint32_t candidate_count_;
    const bool deduplicate_hosts_;
    const LrhLbProto::WeightPreprocessing weight_preprocessing_;
    LrhLoadBalancerStats& stats_;
  };
  using RingSharedPtr = std::shared_ptr<Ring>;

  class BoundedRing : public HashingLoadBalancer {
  public:
    BoundedRing(RingSharedPtr ring, const NormalizedHostWeightVector& normalized_host_weights,
                uint32_t hash_balance_factor);

    HostSelectionResponse chooseHost(uint64_t hash, uint32_t attempt) const override;

    bool updateWeightsForSameHosts(const NormalizedHostWeightVector& normalized_host_weights,
                                   bool use_hostname_for_hashing);
    RingSharedPtr ring() const { return ring_; }

  private:
    struct HostWeightState {
      explicit HostWeightState(HostConstSharedPtr host) : host_(std::move(host)) {}

      HostConstSharedPtr host_;
    };
    using HostWeightStateSharedPtr = std::shared_ptr<HostWeightState>;

    double hostOverloadFactor(const Host& host, double weight) const;
    double normalizedWeightForHost(const HostConstSharedPtr& host) const;
    bool sameHostsInOrder(const NormalizedHostWeightVector& normalized_host_weights) const;

    RingSharedPtr ring_;
    std::vector<HostWeightStateSharedPtr> host_weights_;
    std::map<HostConstSharedPtr, size_t> host_index_by_ptr_;
    const uint32_t hash_balance_factor_;
  };
  using BoundedRingSharedPtr = std::shared_ptr<BoundedRing>;

  // ThreadAwareLoadBalancerBase
  HashingLoadBalancerSharedPtr
  createLoadBalancer(const NormalizedHostWeightVector& normalized_host_weights,
                     double min_normalized_weight, double /* max_normalized_weight */) override;

  Stats::ScopeSharedPtr scope_;
  LrhLoadBalancerStats stats_;
  const uint64_t min_ring_size_;
  const uint64_t max_ring_size_;
  const uint32_t candidate_count_;
  const bool deduplicate_hosts_;
  const bool use_hostname_for_hashing_;
  const LrhLbProto::WeightPreprocessing weight_preprocessing_;
  const uint32_t hash_balance_factor_;
  Ring::TopologySharedPtr cached_topology_;
  std::vector<std::weak_ptr<Ring>> cached_rings_;
  std::vector<std::weak_ptr<BoundedRing>> cached_bounded_rings_;
  std::weak_ptr<Ring> latest_ring_for_test_;
};

} // namespace Upstream
} // namespace Envoy
