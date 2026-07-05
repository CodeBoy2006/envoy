#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "source/extensions/load_balancing_policies/anchorflow/anchorflow_lb.h"

#include "test/benchmark/main.h"
#include "test/extensions/load_balancing_policies/common/benchmark_base_tester.h"

#include "absl/container/flat_hash_set.h"
#include "absl/types/optional.h"

namespace Envoy {
namespace Upstream {
namespace {

class AnchorFlowTester : public BaseTester {
public:
  AnchorFlowTester(uint64_t num_hosts, uint32_t tokens_per_host, uint32_t anchor_multiplier = 1,
                   uint32_t weighted_subset_percent = 0, uint32_t weight = 0)
      : BaseTester(num_hosts, weighted_subset_percent, weight) {
    envoy::extensions::load_balancing_policies::anchorflow::v3::AnchorFlow config;
    config.mutable_tokens_per_host()->set_value(tokens_per_host);
    const uint64_t anchor_buckets = static_cast<uint64_t>(num_hosts) * tokens_per_host *
                                    std::max<uint32_t>(1, anchor_multiplier);
    config.mutable_anchor_buckets()->set_value(anchor_buckets);
    anchorflow_lb_ = std::make_unique<AnchorFlowLoadBalancer>(
        priority_set_, stats_, stats_scope_, runtime_, random_, 50, config, hash_policy_);
  }

  std::shared_ptr<TestHashPolicy> hash_policy_ = std::make_shared<TestHashPolicy>();
  std::unique_ptr<AnchorFlowLoadBalancer> anchorflow_lb_;
};

struct HitDistributionStats {
  double cv;
  double max_over_avg;
  double p99_over_avg;
};

uint64_t weightedSubsetHostCount(uint64_t num_hosts, uint32_t weighted_subset_percent) {
  return static_cast<uint64_t>(num_hosts * (weighted_subset_percent / 100.0));
}

double weightedSubsetGlobalTargetPercent(uint64_t num_hosts, uint64_t weighted_hosts,
                                         uint32_t weight) {
  const uint64_t unweighted_hosts = num_hosts - weighted_hosts;
  const double weighted_capacity = static_cast<double>(weighted_hosts) * weight;
  const double unweighted_capacity = static_cast<double>(unweighted_hosts);
  const double total_capacity = weighted_capacity + unweighted_capacity;
  return total_capacity > 0.0 ? 100.0 * weighted_capacity / total_capacity : 0.0;
}

uint64_t weightedSubsetOffsetForEpoch(uint64_t epoch, uint64_t num_hosts, uint64_t weighted_hosts,
                                      uint32_t pattern) {
  if (pattern == 1) {
    return (epoch * std::max<uint64_t>(1, weighted_hosts)) % num_hosts;
  }
  return 0;
}

uint32_t weightForEpoch(uint64_t epoch, uint32_t max_weight, uint32_t pattern) {
  if (pattern == 0) {
    return epoch % 2 == 0 ? 1 : max_weight;
  }
  return max_weight;
}

absl::flat_hash_set<std::string> weightedSubsetAddresses(BaseTester& tester,
                                                         uint64_t weighted_hosts,
                                                         uint64_t weighted_subset_offset = 0) {
  absl::flat_hash_set<std::string> addresses;
  const auto& hosts = tester.priority_set_.hostSetsPerPriority()[0]->hosts();
  ASSERT(weighted_hosts <= hosts.size());
  addresses.reserve(weighted_hosts);
  for (uint64_t i = 0; i < hosts.size(); ++i) {
    const uint64_t offset_index =
        (i + hosts.size() - (weighted_subset_offset % hosts.size())) % hosts.size();
    if (offset_index < weighted_hosts) {
      addresses.insert(hosts[i]->address()->asString());
    }
  }
  return addresses;
}

bool isWeightedSubsetAddress(const absl::flat_hash_set<std::string>& weighted_subset_addresses,
                             const std::string& address) {
  return weighted_subset_addresses.contains(address);
}

void recordLookupRate(::benchmark::State& state, uint64_t keys_to_simulate) {
  state.counters["lookups_per_second"] =
      ::benchmark::Counter(keys_to_simulate, ::benchmark::Counter::kIsIterationInvariantRate);
}

void recordAnchorFlowShape(::benchmark::State& state, const AnchorFlowLoadBalancer& lb,
                           uint64_t num_hosts, uint32_t tokens_per_host,
                           uint32_t anchor_multiplier) {
  const uint64_t tokens = lb.stats().tokens_.value();
  const uint64_t anchor_buckets = lb.stats().anchor_buckets_.value();
  state.counters["hosts"] = num_hosts;
  state.counters["tokens_per_host"] = tokens_per_host;
  state.counters["anchor_multiplier"] = anchor_multiplier;
  state.counters["tokens"] = tokens;
  state.counters["anchor_buckets"] = anchor_buckets;
  state.counters["anchor_buckets_per_token"] =
      tokens > 0 ? static_cast<double>(anchor_buckets) / static_cast<double>(tokens) : 0.0;
  state.counters["topology_cache_hits"] = lb.stats().topology_cache_hits_.value();
  state.counters["topology_cache_misses"] = lb.stats().topology_cache_misses_.value();
}

void collectChoices(LoadBalancer& lb, TestHashPolicy& hash_policy, uint64_t keys_to_simulate,
                    const absl::flat_hash_set<std::string>& weighted_subset_addresses,
                    absl::node_hash_map<std::string, uint64_t>& hit_counter,
                    std::vector<std::string>* assignments, uint64_t& weighted_subset_hits,
                    const std::vector<std::string>* baseline_assignments = nullptr,
                    uint64_t* changed_assignments = nullptr) {
  TestLoadBalancerContext context;
  for (uint64_t i = 0; i < keys_to_simulate; ++i) {
    hash_policy.hash_key_ = hashInt(i);
    const HostConstSharedPtr host = lb.chooseHost(&context).host;
    ASSERT(host != nullptr);
    const std::string address = host->address()->asString();
    hit_counter[address] += 1;
    if (assignments != nullptr) {
      assignments->push_back(address);
    }
    if (isWeightedSubsetAddress(weighted_subset_addresses, address)) {
      ++weighted_subset_hits;
    }
    if (baseline_assignments != nullptr && changed_assignments != nullptr &&
        address != (*baseline_assignments)[i]) {
      ++(*changed_assignments);
    }
  }
}

HitDistributionStats
summarizeHitDistribution(const absl::node_hash_map<std::string, uint64_t>& hit_counter,
                         uint64_t expected_hosts) {
  const uint64_t observed_hosts = hit_counter.size();
  const uint64_t host_count = std::max<uint64_t>(expected_hosts, observed_hosts);
  if (host_count == 0) {
    return {0.0, 0.0, 0.0};
  }

  double total_hits = 0.0;
  for (const auto& pair : hit_counter) {
    total_hits += pair.second;
  }

  const double mean = total_hits / host_count;
  uint64_t max_hits = 0;
  std::vector<uint64_t> hits;
  hits.reserve(host_count);
  double variance = 0.0;

  for (const auto& pair : hit_counter) {
    max_hits = std::max(max_hits, pair.second);
    hits.push_back(pair.second);
    variance += std::pow(pair.second - mean, 2);
  }

  const uint64_t missing_hosts = host_count - observed_hosts;
  if (missing_hosts > 0) {
    hits.insert(hits.end(), missing_hosts, 0);
    variance += missing_hosts * std::pow(mean, 2);
  }

  std::sort(hits.begin(), hits.end());
  const auto percentile = [&hits](double percentile) -> uint64_t {
    ASSERT(!hits.empty(), "hit stats percentile requires at least one host");
    const size_t index =
        std::min(static_cast<size_t>(std::ceil(percentile * hits.size())) - 1, hits.size() - 1);
    return hits[index];
  };

  variance /= host_count;
  const double stddev = std::sqrt(variance);
  const double cv = mean > 0.0 ? stddev / mean : 0.0;
  return {cv, mean > 0.0 ? max_hits / mean : 0.0, mean > 0.0 ? percentile(0.99) / mean : 0.0};
}

void recordWeightedStats(::benchmark::State& state, uint64_t num_hosts,
                         uint32_t weighted_subset_percent, uint32_t weight, uint64_t weighted_hosts,
                         uint64_t keys_to_simulate, uint64_t weighted_subset_hits,
                         uint64_t changed_assignments,
                         const absl::node_hash_map<std::string, uint64_t>& hit_counter) {
  const double target_percent =
      weightedSubsetGlobalTargetPercent(num_hosts, weighted_hosts, weight);
  const double observed_percent = 100.0 * weighted_subset_hits / keys_to_simulate;
  state.counters["weighted_hosts"] = weighted_hosts;
  state.counters["weighted_subset_percent"] = weighted_subset_percent;
  state.counters["updated_weight"] = weight;
  state.counters["weighted_subset_global_target_percent"] = target_percent;
  state.counters["weighted_subset_observed_percent"] = observed_percent;
  state.counters["target_error_percent"] = std::abs(observed_percent - target_percent);
  state.counters["percent_different"] =
      100.0 * changed_assignments / static_cast<double>(keys_to_simulate);
  computeHitStats(state, hit_counter, num_hosts);
  recordLookupRate(state, keys_to_simulate);
}

void benchmarkAnchorFlowLoadBalancerBuildTable(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint32_t tokens_per_host = state.range(1);
    const uint32_t anchor_multiplier = state.range(2);
    AnchorFlowTester tester(num_hosts, tokens_per_host, anchor_multiplier);
    const size_t start_mem = Memory::Stats::totalCurrentlyAllocated();

    state.ResumeTiming();
    ASSERT_TRUE(tester.anchorflow_lb_->initialize().ok());
    state.PauseTiming();

    const size_t end_mem = Memory::Stats::totalCurrentlyAllocated();
    const uint64_t memory = end_mem - start_mem;
    recordAnchorFlowShape(state, *tester.anchorflow_lb_, num_hosts, tokens_per_host,
                          anchor_multiplier);
    state.counters["memory"] = memory;
    state.counters["memory_per_host"] =
        num_hosts > 0 ? static_cast<double>(memory) / static_cast<double>(num_hosts) : 0.0;
    state.counters["memory_per_token"] =
        tester.anchorflow_lb_->stats().tokens_.value() > 0
            ? static_cast<double>(memory) /
                  static_cast<double>(tester.anchorflow_lb_->stats().tokens_.value())
            : 0.0;
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkAnchorFlowLoadBalancerBuildTable)
    ->Args({100, 16, 1})
    ->Args({100, 64, 1})
    ->Args({100, 256, 1})
    ->Args({500, 16, 1})
    ->Args({500, 64, 1})
    ->Args({500, 256, 1})
    ->Args({1000, 64, 1})
    ->Args({1000, 256, 1})
    ->Args({2000, 64, 1})
    ->Args({500, 64, 2})
    ->Args({1000, 64, 2})
    ->Unit(::benchmark::kMillisecond);

void benchmarkAnchorFlowLoadBalancerChooseHost(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint32_t tokens_per_host = state.range(1);
    const uint32_t anchor_multiplier = state.range(2);
    const uint64_t keys_to_simulate = state.range(3);
    AnchorFlowTester tester(num_hosts, tokens_per_host, anchor_multiplier);
    ASSERT_TRUE(tester.anchorflow_lb_->initialize().ok());
    LoadBalancerPtr lb = tester.anchorflow_lb_->factory()->create(tester.lb_params_);
    const absl::flat_hash_set<std::string> no_weighted_addresses;
    absl::node_hash_map<std::string, uint64_t> hit_counter;
    uint64_t weighted_subset_hits = 0;

    state.ResumeTiming();
    collectChoices(*lb, *tester.hash_policy_, keys_to_simulate, no_weighted_addresses, hit_counter,
                   nullptr, weighted_subset_hits);
    state.PauseTiming();

    recordAnchorFlowShape(state, *tester.anchorflow_lb_, num_hosts, tokens_per_host,
                          anchor_multiplier);
    computeHitStats(state, hit_counter, num_hosts);
    recordLookupRate(state, keys_to_simulate);
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkAnchorFlowLoadBalancerChooseHost)
    ->Args({100, 16, 1, 100000})
    ->Args({100, 64, 1, 100000})
    ->Args({100, 256, 1, 100000})
    ->Args({500, 16, 1, 100000})
    ->Args({500, 64, 1, 100000})
    ->Args({500, 256, 1, 100000})
    ->Args({1000, 64, 1, 100000})
    ->Args({1000, 256, 1, 100000})
    ->Args({500, 64, 2, 100000})
    ->Args({1000, 64, 2, 100000})
    ->Unit(::benchmark::kMillisecond);

void benchmarkAnchorFlowLoadBalancerWeightUpdate(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint32_t tokens_per_host = state.range(1);
    const uint32_t anchor_multiplier = state.range(2);
    const uint32_t weighted_subset_percent = state.range(3);
    const uint32_t updated_weight = state.range(4);
    const uint64_t keys_to_simulate = state.range(5);
    const uint64_t weighted_hosts = weightedSubsetHostCount(num_hosts, weighted_subset_percent);

    AnchorFlowTester before_tester(num_hosts, tokens_per_host, anchor_multiplier);
    ASSERT_TRUE(before_tester.anchorflow_lb_->initialize().ok());
    LoadBalancerPtr before_lb =
        before_tester.anchorflow_lb_->factory()->create(before_tester.lb_params_);
    const auto weighted_addresses = weightedSubsetAddresses(before_tester, weighted_hosts);
    absl::node_hash_map<std::string, uint64_t> before_hits;
    std::vector<std::string> before_assignments;
    before_assignments.reserve(keys_to_simulate);
    uint64_t before_weighted_hits = 0;
    collectChoices(*before_lb, *before_tester.hash_policy_, keys_to_simulate, weighted_addresses,
                   before_hits, &before_assignments, before_weighted_hits);

    AnchorFlowTester after_tester(num_hosts, tokens_per_host, anchor_multiplier,
                                  weighted_subset_percent, updated_weight);
    ASSERT_TRUE(after_tester.anchorflow_lb_->initialize().ok());
    LoadBalancerPtr after_lb =
        after_tester.anchorflow_lb_->factory()->create(after_tester.lb_params_);
    absl::node_hash_map<std::string, uint64_t> after_hits;
    uint64_t after_weighted_hits = 0;
    uint64_t changed_assignments = 0;

    state.ResumeTiming();
    collectChoices(*after_lb, *after_tester.hash_policy_, keys_to_simulate, weighted_addresses,
                   after_hits, nullptr, after_weighted_hits, &before_assignments,
                   &changed_assignments);
    state.PauseTiming();

    recordAnchorFlowShape(state, *after_tester.anchorflow_lb_, num_hosts, tokens_per_host,
                          anchor_multiplier);
    recordWeightedStats(state, num_hosts, weighted_subset_percent, updated_weight, weighted_hosts,
                        keys_to_simulate, after_weighted_hits, changed_assignments, after_hits);
    state.counters["before_weighted_subset_percent"] =
        100.0 * before_weighted_hits / keys_to_simulate;
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkAnchorFlowLoadBalancerWeightUpdate)
    ->Args({100, 64, 1, 10, 32, 100000})
    ->Args({100, 256, 1, 10, 32, 100000})
    ->Args({500, 64, 1, 10, 32, 100000})
    ->Args({500, 256, 1, 10, 32, 100000})
    ->Args({1000, 64, 1, 10, 32, 100000})
    ->Args({500, 64, 1, 20, 8, 100000})
    ->Args({500, 256, 1, 20, 8, 100000})
    ->Args({1000, 64, 1, 20, 8, 100000})
    ->Unit(::benchmark::kMillisecond);

void benchmarkAnchorFlowLoadBalancerWeightUpdateSequence(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint32_t tokens_per_host = state.range(1);
    const uint32_t anchor_multiplier = state.range(2);
    const uint32_t epochs = state.range(3);
    const uint64_t keys_per_epoch = state.range(4);
    const uint32_t weighted_subset_percent = state.range(5);
    const uint32_t max_weight = state.range(6);
    const uint32_t pattern = state.range(7);
    const uint64_t weighted_hosts = weightedSubsetHostCount(num_hosts, weighted_subset_percent);

    AnchorFlowTester tester(num_hosts, tokens_per_host, anchor_multiplier);
    ASSERT_TRUE(tester.anchorflow_lb_->initialize().ok());
    std::vector<std::string> previous_assignments;
    previous_assignments.reserve(keys_per_epoch);

    double update_ms_total = 0.0;
    double lookup_ms_total = 0.0;
    double changed_percent_total = 0.0;
    double changed_percent_max = 0.0;
    double weighted_subset_percent_total = 0.0;
    double target_error_percent_total = 0.0;
    double cv_total = 0.0;
    double max_over_avg_total = 0.0;
    double p99_over_avg_total = 0.0;

    state.ResumeTiming();
    for (uint32_t epoch = 0; epoch < epochs; ++epoch) {
      const uint32_t weight = weightForEpoch(epoch, max_weight, pattern);
      const uint64_t weighted_subset_offset =
          weightedSubsetOffsetForEpoch(epoch, num_hosts, weighted_hosts, pattern);

      const auto update_start = std::chrono::steady_clock::now();
      tester.updateWeightedHosts(weighted_subset_percent, weight, weighted_subset_offset);
      const auto update_end = std::chrono::steady_clock::now();
      update_ms_total +=
          std::chrono::duration<double, std::milli>(update_end - update_start).count();

      LoadBalancerPtr lb = tester.anchorflow_lb_->factory()->create(tester.lb_params_);
      const auto weighted_addresses =
          weightedSubsetAddresses(tester, weighted_hosts, weighted_subset_offset);
      absl::node_hash_map<std::string, uint64_t> hit_counter;
      std::vector<std::string> assignments;
      assignments.reserve(keys_per_epoch);
      uint64_t weighted_subset_hits = 0;
      uint64_t changed_assignments = 0;

      const auto lookup_start = std::chrono::steady_clock::now();
      collectChoices(*lb, *tester.hash_policy_, keys_per_epoch, weighted_addresses, hit_counter,
                     &assignments, weighted_subset_hits,
                     previous_assignments.empty() ? nullptr : &previous_assignments,
                     previous_assignments.empty() ? nullptr : &changed_assignments);
      const auto lookup_end = std::chrono::steady_clock::now();
      lookup_ms_total +=
          std::chrono::duration<double, std::milli>(lookup_end - lookup_start).count();

      if (!previous_assignments.empty()) {
        const double changed_percent =
            100.0 * changed_assignments / static_cast<double>(keys_per_epoch);
        changed_percent_total += changed_percent;
        changed_percent_max = std::max(changed_percent_max, changed_percent);
      }
      previous_assignments = std::move(assignments);

      const double epoch_weighted_percent =
          100.0 * weighted_subset_hits / static_cast<double>(keys_per_epoch);
      weighted_subset_percent_total += epoch_weighted_percent;
      target_error_percent_total +=
          std::abs(epoch_weighted_percent -
                   weightedSubsetGlobalTargetPercent(num_hosts, weighted_hosts, weight));
      const HitDistributionStats hit_stats = summarizeHitDistribution(hit_counter, num_hosts);
      cv_total += hit_stats.cv;
      max_over_avg_total += hit_stats.max_over_avg;
      p99_over_avg_total += hit_stats.p99_over_avg;
    }
    state.PauseTiming();

    const double comparable_transitions = epochs > 1 ? epochs - 1 : 1;
    recordAnchorFlowShape(state, *tester.anchorflow_lb_, num_hosts, tokens_per_host,
                          anchor_multiplier);
    state.counters["epochs"] = epochs;
    state.counters["keys_per_epoch"] = keys_per_epoch;
    state.counters["weighted_hosts"] = weighted_hosts;
    state.counters["weighted_subset_percent"] = weighted_subset_percent;
    state.counters["max_weight"] = max_weight;
    state.counters["pattern"] = pattern;
    state.counters["update_ms_avg"] = update_ms_total / epochs;
    state.counters["lookup_ms_avg"] = lookup_ms_total / epochs;
    state.counters["update_epochs_per_second"] =
        update_ms_total > 0.0 ? 1000.0 * epochs / update_ms_total : 0.0;
    state.counters["full_epochs_per_second"] =
        update_ms_total + lookup_ms_total > 0.0
            ? 1000.0 * epochs / (update_ms_total + lookup_ms_total)
            : 0.0;
    state.counters["changed_avg_percent"] = changed_percent_total / comparable_transitions;
    state.counters["changed_max_percent"] = changed_percent_max;
    state.counters["weighted_subset_avg_percent"] = weighted_subset_percent_total / epochs;
    state.counters["target_error_avg_percent"] = target_error_percent_total / epochs;
    state.counters["cv_avg"] = cv_total / epochs;
    state.counters["max_over_avg_avg"] = max_over_avg_total / epochs;
    state.counters["p99_over_avg_avg"] = p99_over_avg_total / epochs;
    recordLookupRate(state, static_cast<uint64_t>(epochs) * keys_per_epoch);
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkAnchorFlowLoadBalancerWeightUpdateSequence)
    ->Args({100, 64, 1, 20, 10000, 10, 32, 0})
    ->Args({100, 64, 1, 20, 10000, 10, 32, 1})
    ->Args({500, 64, 1, 20, 10000, 10, 32, 0})
    ->Args({500, 64, 1, 20, 10000, 10, 32, 1})
    ->Args({500, 256, 1, 20, 5000, 20, 8, 0})
    ->Args({500, 256, 1, 20, 5000, 20, 8, 1})
    ->Args({1000, 64, 1, 20, 5000, 10, 32, 0})
    ->Args({1000, 64, 1, 20, 5000, 10, 32, 1})
    ->Args({1000, 256, 1, 20, 2500, 20, 8, 0})
    ->Unit(::benchmark::kMillisecond);

} // namespace
} // namespace Upstream
} // namespace Envoy
