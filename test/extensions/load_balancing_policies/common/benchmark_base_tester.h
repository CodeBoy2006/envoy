#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <vector>

#include "envoy/config/cluster/v3/cluster.pb.h"

#include "source/common/common/random_generator.h"
#include "source/common/memory/stats.h"
#include "source/common/upstream/upstream_impl.h"

#include "test/common/upstream/utility.h"
#include "test/mocks/upstream/cluster_info.h"
#include "test/test_common/simulated_time_system.h"

#include "absl/container/node_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "benchmark/benchmark.h"

namespace Envoy {
namespace Upstream {

class BaseTester : public Event::TestUsingSimulatedTime {
public:
  static constexpr absl::string_view metadata_key = "key";
  // We weight a deterministic subset of hosts with weight.
  BaseTester(uint64_t num_hosts, uint32_t weighted_subset_percent = 0, uint32_t weight = 0,
             bool attach_metadata = false);
  void updateWeightedHosts(uint32_t weighted_subset_percent, uint32_t weight,
                           uint64_t weighted_subset_offset = 0);

  Envoy::Thread::MutexBasicLockable lock_;
  // Reduce default log level to warn while running this benchmark to avoid problems due to
  // excessive debug logging in upstream_impl.cc
  Envoy::Logger::Context logging_context_{spdlog::level::warn,
                                          Envoy::Logger::Logger::DEFAULT_LOG_FORMAT, lock_, false};

  Upstream::PrioritySetImpl priority_set_;
  Upstream::PrioritySetImpl local_priority_set_;

  // The following are needed to create a load balancer by the load balancer factory.
  Upstream::LoadBalancerParams lb_params_{priority_set_, &local_priority_set_};

  Stats::IsolatedStoreImpl stats_store_;
  Stats::Scope& stats_scope_{*stats_store_.rootScope()};
  Upstream::ClusterLbStatNames stat_names_{stats_store_.symbolTable()};
  Upstream::ClusterLbStats stats_{stat_names_, stats_scope_};
  NiceMock<Runtime::MockLoader> runtime_;
  Random::RandomGeneratorImpl random_;
  std::shared_ptr<Upstream::MockClusterInfo> info_{new NiceMock<Upstream::MockClusterInfo>()};

private:
  uint64_t num_hosts_;
  bool attach_metadata_;
};

class TestLoadBalancerContext : public Upstream::LoadBalancerContextBase {
public:
  // Upstream::LoadBalancerContext
  absl::optional<uint64_t> computeHashKey() override { return hash_key_; }

  absl::optional<uint64_t> hash_key_;
};

class TestHashPolicy : public Http::HashPolicy {
public:
  absl::optional<uint64_t> generateHash(OptRef<const Http::RequestHeaderMap>,
                                        OptRef<const StreamInfo::StreamInfo>,
                                        AddCookieCallback) const override {
    return hash_key_;
  }

  absl::optional<uint64_t> hash_key_;
};

inline void computeHitStats(::benchmark::State& state,
                            const absl::node_hash_map<std::string, uint64_t>& hit_counter,
                            absl::optional<uint64_t> expected_hosts = absl::nullopt,
                            absl::string_view counter_prefix = "") {
  const auto counter_name = [counter_prefix](absl::string_view name) -> std::string {
    return counter_prefix.empty() ? std::string(name) : absl::StrCat(counter_prefix, name);
  };
  const uint64_t observed_hosts = hit_counter.size();
  const uint64_t host_count =
      std::max<uint64_t>(expected_hosts.value_or(observed_hosts), observed_hosts);
  double total_hits = 0;
  for (const auto& pair : hit_counter) {
    total_hits += pair.second;
  }

  if (host_count == 0) {
    state.counters[counter_name("active_hosts")] = 0;
    state.counters[counter_name("host_count")] = 0;
    state.counters[counter_name("total_hits")] = 0;
    state.counters[counter_name("mean_hits")] = 0;
    state.counters[counter_name("stddev_hits")] = 0;
    state.counters[counter_name("relative_stddev_hits")] = 0;
    state.counters[counter_name("cv_hits")] = 0;
    state.counters[counter_name("min_hits")] = 0;
    state.counters[counter_name("max_hits")] = 0;
    state.counters[counter_name("min_over_avg")] = 0;
    state.counters[counter_name("max_over_avg")] = 0;
    state.counters[counter_name("p50_over_avg")] = 0;
    state.counters[counter_name("p95_over_avg")] = 0;
    state.counters[counter_name("p99_over_avg")] = 0;
    return;
  }

  const double mean = total_hits / host_count;
  uint64_t min_hits = observed_hosts < host_count ? 0 : std::numeric_limits<uint64_t>::max();
  uint64_t max_hits = 0;
  std::vector<uint64_t> hits;
  hits.reserve(host_count);

  double variance = 0;
  for (const auto& pair : hit_counter) {
    min_hits = std::min(min_hits, pair.second);
    max_hits = std::max(max_hits, pair.second);
    hits.push_back(pair.second);
    variance += std::pow(pair.second - mean, 2);
  }
  const uint64_t missing_hosts = host_count - observed_hosts;
  if (missing_hosts > 0) {
    hits.insert(hits.end(), missing_hosts, 0);
    variance += missing_hosts * std::pow(mean, 2);
  }
  variance /= host_count;
  const double stddev = std::sqrt(variance);
  const double cv = mean > 0 ? stddev / mean : 0;

  std::sort(hits.begin(), hits.end());
  const auto percentile = [&hits](double percentile) -> uint64_t {
    ASSERT(!hits.empty(), "hit stats percentile requires at least one host");
    const size_t index =
        std::min(static_cast<size_t>(std::ceil(percentile * hits.size())) - 1, hits.size() - 1);
    return hits[index];
  };

  state.counters[counter_name("active_hosts")] = observed_hosts;
  state.counters[counter_name("host_count")] = host_count;
  state.counters[counter_name("total_hits")] = total_hits;
  state.counters[counter_name("mean_hits")] = mean;
  state.counters[counter_name("stddev_hits")] = stddev;
  state.counters[counter_name("relative_stddev_hits")] = cv;
  state.counters[counter_name("cv_hits")] = cv;
  state.counters[counter_name("min_hits")] = min_hits;
  state.counters[counter_name("max_hits")] = max_hits;
  state.counters[counter_name("min_over_avg")] = mean > 0 ? min_hits / mean : 0;
  state.counters[counter_name("max_over_avg")] = mean > 0 ? max_hits / mean : 0;
  state.counters[counter_name("p50_over_avg")] = mean > 0 ? percentile(0.50) / mean : 0;
  state.counters[counter_name("p95_over_avg")] = mean > 0 ? percentile(0.95) / mean : 0;
  state.counters[counter_name("p99_over_avg")] = mean > 0 ? percentile(0.99) / mean : 0;
}

inline uint64_t hashInt(uint64_t i) {
  // Hack to hash an integer.
  return HashUtil::xxHash64(absl::string_view(reinterpret_cast<const char*>(&i), sizeof(i)));
}

} // namespace Upstream
} // namespace Envoy
