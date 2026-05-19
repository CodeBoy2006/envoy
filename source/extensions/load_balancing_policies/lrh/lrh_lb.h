#pragma once

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
#define ALL_LRH_LOAD_BALANCER_STATS(GAUGE)                                                         \
  GAUGE(max_hashes_per_host, Accumulate)                                                           \
  GAUGE(min_hashes_per_host, Accumulate)                                                           \
  GAUGE(size, Accumulate)

/**
 * Struct definition for all LRH load balancer stats. @see stats_macros.h
 */
struct LrhLoadBalancerStats {
  ALL_LRH_LOAD_BALANCER_STATS(GENERATE_GAUGE_STRUCT)
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

  static constexpr uint32_t DefaultCandidateCount = 8;
  static constexpr uint32_t MaxCandidateCount = 64;
  static constexpr uint64_t DefaultMinRingSize = 1024;
  static constexpr uint64_t DefaultMaxRingSize = 1024 * 1024 * 8;

private:
  struct RingEntry {
    uint64_t hash_;
    HostConstSharedPtr host_;
    double normalized_weight_;
    uint64_t host_hash_;
  };

  class Ring : public HashingLoadBalancer {
  public:
    Ring(const NormalizedHostWeightVector& normalized_host_weights, double min_normalized_weight,
         uint64_t min_ring_size, uint64_t max_ring_size, uint32_t candidate_count,
         bool deduplicate_hosts, bool use_hostname_for_hashing, LrhLoadBalancerStats& stats);

    // ThreadAwareLoadBalancerBase::HashingLoadBalancer
    HostSelectionResponse chooseHost(uint64_t hash, uint32_t attempt) const override;

    size_t ringSizeForTest() const { return ring_.size(); }
    size_t candidateCountForTest() const { return candidate_count_; }

  private:
    size_t lowerBound(uint64_t hash) const;
    const RingEntry* bestInCandidateBlock(uint64_t hash, size_t start, size_t max_slots,
                                          size_t& walked) const;
    static double weightedScore(uint64_t request_hash, const RingEntry& entry);

    std::vector<RingEntry> ring_;
    const uint32_t candidate_count_;
    const bool deduplicate_hosts_;
    LrhLoadBalancerStats& stats_;
  };

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
  const uint32_t hash_balance_factor_;
};

} // namespace Upstream
} // namespace Envoy
