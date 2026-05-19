#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <string>
#include <vector>

#include "source/extensions/load_balancing_policies/lrh/lrh_lb.h"
#include "source/extensions/load_balancing_policies/maglev/maglev_lb.h"
#include "source/extensions/load_balancing_policies/ring_hash/ring_hash_lb.h"

#include "test/benchmark/main.h"
#include "test/extensions/load_balancing_policies/common/benchmark_base_tester.h"

#include "absl/container/flat_hash_set.h"
#include "absl/types/optional.h"

namespace Envoy {
namespace Upstream {
namespace {

class LrhTester : public BaseTester {
public:
  LrhTester(uint64_t num_hosts, uint64_t min_ring_size, uint32_t candidate_count,
            uint32_t weighted_subset_percent = 0, uint32_t weight = 0)
      : BaseTester(num_hosts, weighted_subset_percent, weight) {
    envoy::extensions::load_balancing_policies::lrh::v3::LocalRendezvousHashing config;
    config.mutable_minimum_ring_size()->set_value(min_ring_size);
    config.mutable_candidate_count()->set_value(candidate_count);
    lrh_lb_ = std::make_unique<LrhLoadBalancer>(priority_set_, stats_, stats_scope_, runtime_,
                                                random_, 50, config, hash_policy_);
  }

  std::shared_ptr<TestHashPolicy> hash_policy_ = std::make_shared<TestHashPolicy>();
  std::unique_ptr<LrhLoadBalancer> lrh_lb_;
};

class RingHashTester : public BaseTester {
public:
  RingHashTester(uint64_t num_hosts, uint64_t min_ring_size, uint32_t weighted_subset_percent = 0,
                 uint32_t weight = 0)
      : BaseTester(num_hosts, weighted_subset_percent, weight) {
    envoy::extensions::load_balancing_policies::ring_hash::v3::RingHash config;
    config.mutable_minimum_ring_size()->set_value(min_ring_size);
    ring_hash_lb_ = std::make_unique<RingHashLoadBalancer>(
        priority_set_, stats_, stats_scope_, runtime_, random_, 50, config, hash_policy_);
  }

  std::shared_ptr<TestHashPolicy> hash_policy_ = std::make_shared<TestHashPolicy>();
  std::unique_ptr<RingHashLoadBalancer> ring_hash_lb_;
};

class MaglevTester : public BaseTester {
public:
  MaglevTester(uint64_t num_hosts, uint64_t table_size, uint32_t weighted_subset_percent = 0,
               uint32_t weight = 0)
      : BaseTester(num_hosts, weighted_subset_percent, weight) {
    envoy::extensions::load_balancing_policies::maglev::v3::Maglev config;
    config.mutable_table_size()->set_value(table_size);
    maglev_lb_ = std::make_unique<MaglevLoadBalancer>(priority_set_, stats_, stats_scope_, runtime_,
                                                      random_, 50, config, hash_policy_);
  }

  std::shared_ptr<TestHashPolicy> hash_policy_ = std::make_shared<TestHashPolicy>();
  std::unique_ptr<MaglevLoadBalancer> maglev_lb_;
};

void recordLookupRate(::benchmark::State& state, uint64_t keys_to_simulate) {
  state.counters["lookups_per_second"] =
      ::benchmark::Counter(keys_to_simulate, ::benchmark::Counter::kIsIterationInvariantRate);
}

struct SlotStats {
  uint64_t slots;
  uint64_t min_slots_per_host;
  uint64_t max_slots_per_host;
};

struct HitDistributionStats {
  double cv;
  double min_over_avg;
  double max_over_avg;
  double p99_over_avg;
};

uint64_t weightedSubsetHostCount(uint64_t num_hosts, uint32_t weighted_subset_percent) {
  uint64_t count = 0;
  for (uint64_t i = 0; i < num_hosts; ++i) {
    if (i < num_hosts * (weighted_subset_percent / 100.0)) {
      ++count;
    }
  }
  return count;
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

double weightedSubsetGlobalTargetPercent(uint64_t num_hosts, uint64_t weighted_hosts,
                                         uint32_t weight) {
  const uint64_t unweighted_hosts = num_hosts - weighted_hosts;
  return weighted_hosts == 0
             ? 0.0
             : 100.0 * (static_cast<double>(weighted_hosts) * weight) /
                   (static_cast<double>(weighted_hosts) * weight + unweighted_hosts);
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

bool isWeightedSubsetAddress(const absl::flat_hash_set<std::string>& weighted_subset_addresses,
                             const std::string& address) {
  return weighted_subset_addresses.contains(address);
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
    return {0.0, 0.0, 0.0, 0.0};
  }

  double total_hits = 0;
  for (const auto& pair : hit_counter) {
    total_hits += pair.second;
  }

  const double mean = total_hits / host_count;
  uint64_t min_hits = observed_hosts < host_count ? 0 : std::numeric_limits<uint64_t>::max();
  uint64_t max_hits = 0;
  std::vector<uint64_t> hits;
  hits.reserve(host_count);
  double variance = 0.0;

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

  std::sort(hits.begin(), hits.end());
  const auto percentile = [&hits](double percentile) -> uint64_t {
    ASSERT(!hits.empty(), "hit stats percentile requires at least one host");
    const size_t index =
        std::min(static_cast<size_t>(std::ceil(percentile * hits.size())) - 1, hits.size() - 1);
    return hits[index];
  };

  variance /= host_count;
  const double stddev = std::sqrt(variance);
  const double cv = mean > 0 ? stddev / mean : 0.0;
  return {cv, mean > 0 ? min_hits / mean : 0.0, mean > 0 ? max_hits / mean : 0.0,
          mean > 0 ? percentile(0.99) / mean : 0.0};
}

void recordWeightUpdateStats(::benchmark::State& state, uint64_t num_hosts,
                             uint32_t weighted_subset_percent, uint32_t weight,
                             uint64_t weighted_hosts, uint64_t keys_to_simulate,
                             uint64_t before_weighted_hits, uint64_t after_weighted_hits,
                             uint64_t changed_assignments,
                             const absl::node_hash_map<std::string, uint64_t>& before_hits,
                             const absl::node_hash_map<std::string, uint64_t>& after_hits,
                             absl::optional<uint32_t> candidate_count = absl::nullopt) {
  const double target_percent =
      weightedSubsetGlobalTargetPercent(num_hosts, weighted_hosts, weight);

  state.counters["weighted_hosts"] = weighted_hosts;
  state.counters["weighted_subset_percent"] = weighted_subset_percent;
  state.counters["updated_weight"] = weight;
  state.counters["weighted_subset_global_target_percent"] = target_percent;
  state.counters["weighted_subset_before_percent"] =
      100.0 * before_weighted_hits / keys_to_simulate;
  state.counters["weighted_subset_after_percent"] = 100.0 * after_weighted_hits / keys_to_simulate;
  state.counters["weighted_subset_delta_percent"] =
      100.0 * (static_cast<double>(after_weighted_hits) - before_weighted_hits) / keys_to_simulate;
  state.counters["percent_different"] =
      100.0 * changed_assignments / static_cast<double>(keys_to_simulate);
  if (candidate_count.has_value()) {
    const double weighted_fraction = static_cast<double>(weighted_hosts) / num_hosts;
    state.counters["candidate_count"] = candidate_count.value();
    state.counters["weighted_subset_window_exposure_approx_percent"] =
        100.0 * (1.0 - std::pow(1.0 - weighted_fraction, candidate_count.value()));
  }

  computeHitStats(state, before_hits, num_hosts, "before_");
  computeHitStats(state, after_hits, num_hosts, "after_");
  computeHitStats(state, after_hits, num_hosts);
  recordLookupRate(state, keys_to_simulate);
}

void recordSlotStats(::benchmark::State& state, const SlotStats& before, const SlotStats& after) {
  state.counters["before_slots"] = before.slots;
  state.counters["after_slots"] = after.slots;
  state.counters["slots_delta"] = static_cast<double>(after.slots) - before.slots;
  state.counters["before_min_slots_per_host"] = before.min_slots_per_host;
  state.counters["before_max_slots_per_host"] = before.max_slots_per_host;
  state.counters["after_min_slots_per_host"] = after.min_slots_per_host;
  state.counters["after_max_slots_per_host"] = after.max_slots_per_host;
  state.counters["min_slots_per_host_delta"] =
      static_cast<double>(after.min_slots_per_host) - before.min_slots_per_host;
  state.counters["max_slots_per_host_delta"] =
      static_cast<double>(after.max_slots_per_host) - before.max_slots_per_host;
}

void recordSequenceStats(::benchmark::State& state, uint64_t num_hosts, uint32_t epochs,
                         uint64_t keys_per_epoch, uint32_t weighted_subset_percent,
                         uint32_t max_weight, uint32_t pattern, uint64_t weighted_hosts,
                         double update_ms_total, double lookup_ms_total,
                         double changed_percent_total, double changed_percent_max,
                         double weighted_subset_percent_total, double target_error_percent_total,
                         double cv_total, double max_over_avg_total, double p99_over_avg_total,
                         double slot_shape_change_epochs, double slots_delta_abs_total,
                         double min_slots_delta_abs_total, double max_slots_delta_abs_total,
                         absl::optional<uint32_t> candidate_count = absl::nullopt) {
  const uint64_t total_keys = static_cast<uint64_t>(epochs) * keys_per_epoch;
  const double comparable_transitions = epochs > 1 ? epochs - 1 : 1;

  state.counters["epochs"] = epochs;
  state.counters["keys_per_epoch"] = keys_per_epoch;
  state.counters["weighted_hosts"] = weighted_hosts;
  state.counters["weighted_subset_percent"] = weighted_subset_percent;
  state.counters["max_weight"] = max_weight;
  state.counters["pattern"] = pattern;
  state.counters["update_ms_avg"] = update_ms_total / epochs;
  state.counters["lookup_ms_avg"] = lookup_ms_total / epochs;
  state.counters["update_to_lookup_ms_ratio"] =
      lookup_ms_total > 0 ? update_ms_total / lookup_ms_total : 0.0;
  state.counters["changed_avg_percent"] = changed_percent_total / comparable_transitions;
  state.counters["changed_max_percent"] = changed_percent_max;
  state.counters["weighted_subset_avg_percent"] = weighted_subset_percent_total / epochs;
  state.counters["target_error_avg_percent"] = target_error_percent_total / epochs;
  state.counters["cv_avg"] = cv_total / epochs;
  state.counters["max_over_avg_avg"] = max_over_avg_total / epochs;
  state.counters["p99_over_avg_avg"] = p99_over_avg_total / epochs;
  state.counters["slot_shape_change_epochs"] = slot_shape_change_epochs;
  state.counters["slot_shape_change_percent"] = 100.0 * slot_shape_change_epochs / epochs;
  state.counters["slots_delta_abs_avg"] = slots_delta_abs_total / epochs;
  state.counters["min_slots_delta_abs_avg"] = min_slots_delta_abs_total / epochs;
  state.counters["max_slots_delta_abs_avg"] = max_slots_delta_abs_total / epochs;
  if (candidate_count.has_value()) {
    const double weighted_fraction = static_cast<double>(weighted_hosts) / num_hosts;
    state.counters["candidate_count"] = candidate_count.value();
    state.counters["weighted_subset_window_exposure_approx_percent"] =
        100.0 * (1.0 - std::pow(1.0 - weighted_fraction, candidate_count.value()));
  }
  recordLookupRate(state, total_keys);
}

void benchmarkLrhLoadBalancerBuildRing(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint64_t min_ring_size = state.range(1);
    const uint32_t candidate_count = state.range(2);
    LrhTester tester(num_hosts, min_ring_size, candidate_count);

    const size_t start_mem = Memory::Stats::totalCurrentlyAllocated();

    state.ResumeTiming();
    ASSERT_TRUE(tester.lrh_lb_->initialize().ok());
    state.PauseTiming();
    const size_t end_mem = Memory::Stats::totalCurrentlyAllocated();
    state.counters["memory"] = end_mem - start_mem;
    state.counters["memory_per_host"] = (end_mem - start_mem) / num_hosts;
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkLrhLoadBalancerBuildRing)
    ->Args({100, 65536, 4})
    ->Args({200, 65536, 8})
    ->Args({500, 65536, 8})
    ->Args({100, 256000, 4})
    ->Args({200, 256000, 8})
    ->Args({500, 256000, 8})
    ->Unit(::benchmark::kMillisecond);

void benchmarkRingHashLoadBalancerBuildRing(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint64_t min_ring_size = state.range(1);
    RingHashTester tester(num_hosts, min_ring_size);

    const size_t start_mem = Memory::Stats::totalCurrentlyAllocated();

    state.ResumeTiming();
    ASSERT_TRUE(tester.ring_hash_lb_->initialize().ok());
    state.PauseTiming();
    const size_t end_mem = Memory::Stats::totalCurrentlyAllocated();
    state.counters["memory"] = end_mem - start_mem;
    state.counters["memory_per_host"] = (end_mem - start_mem) / num_hosts;
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkRingHashLoadBalancerBuildRing)
    ->Args({100, 65536})
    ->Args({200, 65536})
    ->Args({500, 65536})
    ->Args({100, 256000})
    ->Args({200, 256000})
    ->Args({500, 256000})
    ->Unit(::benchmark::kMillisecond);

void benchmarkMaglevLoadBalancerBuildTable(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint64_t table_size = state.range(1);
    MaglevTester tester(num_hosts, table_size);

    const size_t start_mem = Memory::Stats::totalCurrentlyAllocated();

    state.ResumeTiming();
    ASSERT_TRUE(tester.maglev_lb_->initialize().ok());
    state.PauseTiming();
    const size_t end_mem = Memory::Stats::totalCurrentlyAllocated();
    state.counters["memory"] = end_mem - start_mem;
    state.counters["memory_per_host"] = (end_mem - start_mem) / num_hosts;
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkMaglevLoadBalancerBuildTable)
    ->Args({100, 65537})
    ->Args({200, 65537})
    ->Args({500, 65537})
    ->Args({100, 262147})
    ->Args({200, 262147})
    ->Args({500, 262147})
    ->Unit(::benchmark::kMillisecond);

void benchmarkLrhLoadBalancerChooseHost(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint64_t min_ring_size = state.range(1);
    const uint32_t candidate_count = state.range(2);
    const uint64_t keys_to_simulate = state.range(3);
    LrhTester tester(num_hosts, min_ring_size, candidate_count);
    ASSERT_TRUE(tester.lrh_lb_->initialize().ok());
    LoadBalancerPtr lb = tester.lrh_lb_->factory()->create(tester.lb_params_);
    absl::node_hash_map<std::string, uint64_t> hit_counter;
    TestLoadBalancerContext context;
    state.ResumeTiming();

    for (uint64_t i = 0; i < keys_to_simulate; i++) {
      tester.hash_policy_->hash_key_ = hashInt(i);
      hit_counter[lb->chooseHost(&context).host->address()->asString()] += 1;
    }

    state.PauseTiming();
    computeHitStats(state, hit_counter, num_hosts);
    recordLookupRate(state, keys_to_simulate);
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkLrhLoadBalancerChooseHost)
    ->Args({100, 65536, 4, 100000})
    ->Args({200, 65536, 8, 100000})
    ->Args({500, 65536, 8, 100000})
    ->Args({100, 256000, 4, 100000})
    ->Args({200, 256000, 8, 100000})
    ->Args({500, 256000, 8, 100000})
    ->Unit(::benchmark::kMillisecond);

void benchmarkRingHashLoadBalancerChooseHost(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint64_t min_ring_size = state.range(1);
    const uint64_t keys_to_simulate = state.range(2);
    RingHashTester tester(num_hosts, min_ring_size);
    ASSERT_TRUE(tester.ring_hash_lb_->initialize().ok());
    LoadBalancerPtr lb = tester.ring_hash_lb_->factory()->create(tester.lb_params_);
    absl::node_hash_map<std::string, uint64_t> hit_counter;
    TestLoadBalancerContext context;
    state.ResumeTiming();

    for (uint64_t i = 0; i < keys_to_simulate; i++) {
      tester.hash_policy_->hash_key_ = hashInt(i);
      hit_counter[lb->chooseHost(&context).host->address()->asString()] += 1;
    }

    state.PauseTiming();
    computeHitStats(state, hit_counter, num_hosts);
    recordLookupRate(state, keys_to_simulate);
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkRingHashLoadBalancerChooseHost)
    ->Args({100, 65536, 100000})
    ->Args({200, 65536, 100000})
    ->Args({500, 65536, 100000})
    ->Args({100, 256000, 100000})
    ->Args({200, 256000, 100000})
    ->Args({500, 256000, 100000})
    ->Unit(::benchmark::kMillisecond);

void benchmarkMaglevLoadBalancerChooseHost(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint64_t table_size = state.range(1);
    const uint64_t keys_to_simulate = state.range(2);
    MaglevTester tester(num_hosts, table_size);
    ASSERT_TRUE(tester.maglev_lb_->initialize().ok());
    LoadBalancerPtr lb = tester.maglev_lb_->factory()->create(tester.lb_params_);
    absl::node_hash_map<std::string, uint64_t> hit_counter;
    TestLoadBalancerContext context;
    state.ResumeTiming();

    for (uint64_t i = 0; i < keys_to_simulate; i++) {
      tester.hash_policy_->hash_key_ = hashInt(i);
      hit_counter[lb->chooseHost(&context).host->address()->asString()] += 1;
    }

    state.PauseTiming();
    computeHitStats(state, hit_counter, num_hosts);
    recordLookupRate(state, keys_to_simulate);
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkMaglevLoadBalancerChooseHost)
    ->Args({100, 65537, 100000})
    ->Args({200, 65537, 100000})
    ->Args({500, 65537, 100000})
    ->Args({100, 262147, 100000})
    ->Args({200, 262147, 100000})
    ->Args({500, 262147, 100000})
    ->Unit(::benchmark::kMillisecond);

void benchmarkLrhLoadBalancerWeightUpdate(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint64_t min_ring_size = state.range(1);
    const uint32_t candidate_count = state.range(2);
    const uint32_t weighted_subset_percent = state.range(3);
    const uint32_t updated_weight = state.range(4);
    const uint64_t keys_to_simulate = state.range(5);
    const uint64_t weighted_hosts = weightedSubsetHostCount(num_hosts, weighted_subset_percent);

    LrhTester before_tester(num_hosts, min_ring_size, candidate_count);
    ASSERT_TRUE(before_tester.lrh_lb_->initialize().ok());
    const SlotStats before_slots{before_tester.lrh_lb_->stats().size_.value(),
                                 before_tester.lrh_lb_->stats().min_hashes_per_host_.value(),
                                 before_tester.lrh_lb_->stats().max_hashes_per_host_.value()};
    LoadBalancerPtr before_lb = before_tester.lrh_lb_->factory()->create(before_tester.lb_params_);
    const auto weighted_addresses = weightedSubsetAddresses(before_tester, weighted_hosts);
    absl::node_hash_map<std::string, uint64_t> before_hits;
    std::vector<std::string> before_assignments;
    before_assignments.reserve(keys_to_simulate);
    uint64_t before_weighted_hits = 0;
    collectChoices(*before_lb, *before_tester.hash_policy_, keys_to_simulate, weighted_addresses,
                   before_hits, &before_assignments, before_weighted_hits);

    LrhTester after_tester(num_hosts, min_ring_size, candidate_count, weighted_subset_percent,
                           updated_weight);
    ASSERT_TRUE(after_tester.lrh_lb_->initialize().ok());
    const SlotStats after_slots{after_tester.lrh_lb_->stats().size_.value(),
                                after_tester.lrh_lb_->stats().min_hashes_per_host_.value(),
                                after_tester.lrh_lb_->stats().max_hashes_per_host_.value()};
    LoadBalancerPtr after_lb = after_tester.lrh_lb_->factory()->create(after_tester.lb_params_);
    absl::node_hash_map<std::string, uint64_t> after_hits;
    uint64_t after_weighted_hits = 0;
    uint64_t changed_assignments = 0;

    state.ResumeTiming();
    collectChoices(*after_lb, *after_tester.hash_policy_, keys_to_simulate, weighted_addresses,
                   after_hits, nullptr, after_weighted_hits, &before_assignments,
                   &changed_assignments);
    state.PauseTiming();

    recordWeightUpdateStats(state, num_hosts, weighted_subset_percent, updated_weight,
                            weighted_hosts, keys_to_simulate, before_weighted_hits,
                            after_weighted_hits, changed_assignments, before_hits, after_hits,
                            candidate_count);
    recordSlotStats(state, before_slots, after_slots);
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkLrhLoadBalancerWeightUpdate)
    ->Args({100, 65536, 8, 10, 32, 100000})
    ->Args({100, 65536, 8, 20, 8, 100000})
    ->Args({500, 65536, 8, 10, 32, 100000})
    ->Args({500, 65536, 8, 20, 8, 100000})
    ->Args({500, 256000, 8, 10, 32, 100000})
    ->Args({500, 256000, 8, 20, 8, 100000})
    ->Unit(::benchmark::kMillisecond);

void benchmarkRingHashLoadBalancerWeightUpdate(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint64_t min_ring_size = state.range(1);
    const uint32_t weighted_subset_percent = state.range(2);
    const uint32_t updated_weight = state.range(3);
    const uint64_t keys_to_simulate = state.range(4);
    const uint64_t weighted_hosts = weightedSubsetHostCount(num_hosts, weighted_subset_percent);

    RingHashTester before_tester(num_hosts, min_ring_size);
    ASSERT_TRUE(before_tester.ring_hash_lb_->initialize().ok());
    const SlotStats before_slots{before_tester.ring_hash_lb_->stats().size_.value(),
                                 before_tester.ring_hash_lb_->stats().min_hashes_per_host_.value(),
                                 before_tester.ring_hash_lb_->stats().max_hashes_per_host_.value()};
    LoadBalancerPtr before_lb =
        before_tester.ring_hash_lb_->factory()->create(before_tester.lb_params_);
    const auto weighted_addresses = weightedSubsetAddresses(before_tester, weighted_hosts);
    absl::node_hash_map<std::string, uint64_t> before_hits;
    std::vector<std::string> before_assignments;
    before_assignments.reserve(keys_to_simulate);
    uint64_t before_weighted_hits = 0;
    collectChoices(*before_lb, *before_tester.hash_policy_, keys_to_simulate, weighted_addresses,
                   before_hits, &before_assignments, before_weighted_hits);

    RingHashTester after_tester(num_hosts, min_ring_size, weighted_subset_percent, updated_weight);
    ASSERT_TRUE(after_tester.ring_hash_lb_->initialize().ok());
    const SlotStats after_slots{after_tester.ring_hash_lb_->stats().size_.value(),
                                after_tester.ring_hash_lb_->stats().min_hashes_per_host_.value(),
                                after_tester.ring_hash_lb_->stats().max_hashes_per_host_.value()};
    LoadBalancerPtr after_lb =
        after_tester.ring_hash_lb_->factory()->create(after_tester.lb_params_);
    absl::node_hash_map<std::string, uint64_t> after_hits;
    uint64_t after_weighted_hits = 0;
    uint64_t changed_assignments = 0;

    state.ResumeTiming();
    collectChoices(*after_lb, *after_tester.hash_policy_, keys_to_simulate, weighted_addresses,
                   after_hits, nullptr, after_weighted_hits, &before_assignments,
                   &changed_assignments);
    state.PauseTiming();

    recordWeightUpdateStats(state, num_hosts, weighted_subset_percent, updated_weight,
                            weighted_hosts, keys_to_simulate, before_weighted_hits,
                            after_weighted_hits, changed_assignments, before_hits, after_hits);
    recordSlotStats(state, before_slots, after_slots);
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkRingHashLoadBalancerWeightUpdate)
    ->Args({100, 65536, 10, 32, 100000})
    ->Args({100, 65536, 20, 8, 100000})
    ->Args({500, 65536, 10, 32, 100000})
    ->Args({500, 65536, 20, 8, 100000})
    ->Args({500, 256000, 10, 32, 100000})
    ->Args({500, 256000, 20, 8, 100000})
    ->Unit(::benchmark::kMillisecond);

void benchmarkMaglevLoadBalancerWeightUpdate(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint64_t table_size = state.range(1);
    const uint32_t weighted_subset_percent = state.range(2);
    const uint32_t updated_weight = state.range(3);
    const uint64_t keys_to_simulate = state.range(4);
    const uint64_t weighted_hosts = weightedSubsetHostCount(num_hosts, weighted_subset_percent);

    MaglevTester before_tester(num_hosts, table_size);
    ASSERT_TRUE(before_tester.maglev_lb_->initialize().ok());
    const SlotStats before_slots{before_tester.maglev_lb_->tableSize(),
                                 before_tester.maglev_lb_->stats().min_entries_per_host_.value(),
                                 before_tester.maglev_lb_->stats().max_entries_per_host_.value()};
    LoadBalancerPtr before_lb =
        before_tester.maglev_lb_->factory()->create(before_tester.lb_params_);
    const auto weighted_addresses = weightedSubsetAddresses(before_tester, weighted_hosts);
    absl::node_hash_map<std::string, uint64_t> before_hits;
    std::vector<std::string> before_assignments;
    before_assignments.reserve(keys_to_simulate);
    uint64_t before_weighted_hits = 0;
    collectChoices(*before_lb, *before_tester.hash_policy_, keys_to_simulate, weighted_addresses,
                   before_hits, &before_assignments, before_weighted_hits);

    MaglevTester after_tester(num_hosts, table_size, weighted_subset_percent, updated_weight);
    ASSERT_TRUE(after_tester.maglev_lb_->initialize().ok());
    const SlotStats after_slots{after_tester.maglev_lb_->tableSize(),
                                after_tester.maglev_lb_->stats().min_entries_per_host_.value(),
                                after_tester.maglev_lb_->stats().max_entries_per_host_.value()};
    LoadBalancerPtr after_lb = after_tester.maglev_lb_->factory()->create(after_tester.lb_params_);
    absl::node_hash_map<std::string, uint64_t> after_hits;
    uint64_t after_weighted_hits = 0;
    uint64_t changed_assignments = 0;

    state.ResumeTiming();
    collectChoices(*after_lb, *after_tester.hash_policy_, keys_to_simulate, weighted_addresses,
                   after_hits, nullptr, after_weighted_hits, &before_assignments,
                   &changed_assignments);
    state.PauseTiming();

    recordWeightUpdateStats(state, num_hosts, weighted_subset_percent, updated_weight,
                            weighted_hosts, keys_to_simulate, before_weighted_hits,
                            after_weighted_hits, changed_assignments, before_hits, after_hits);
    recordSlotStats(state, before_slots, after_slots);
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkMaglevLoadBalancerWeightUpdate)
    ->Args({100, 65537, 10, 32, 100000})
    ->Args({100, 65537, 20, 8, 100000})
    ->Args({500, 65537, 10, 32, 100000})
    ->Args({500, 65537, 20, 8, 100000})
    ->Args({500, 262147, 10, 32, 100000})
    ->Args({500, 262147, 20, 8, 100000})
    ->Unit(::benchmark::kMillisecond);

void benchmarkLrhLoadBalancerWeightUpdateSequence(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint64_t min_ring_size = state.range(1);
    const uint32_t candidate_count = state.range(2);
    const uint32_t epochs = state.range(3);
    const uint64_t keys_per_epoch = state.range(4);
    const uint32_t weighted_subset_percent = state.range(5);
    const uint32_t max_weight = state.range(6);
    const uint32_t pattern = state.range(7);
    const uint64_t weighted_hosts = weightedSubsetHostCount(num_hosts, weighted_subset_percent);

    LrhTester tester(num_hosts, min_ring_size, candidate_count);
    ASSERT_TRUE(tester.lrh_lb_->initialize().ok());
    std::vector<std::string> previous_assignments;
    previous_assignments.reserve(keys_per_epoch);
    SlotStats previous_slots{tester.lrh_lb_->stats().size_.value(),
                             tester.lrh_lb_->stats().min_hashes_per_host_.value(),
                             tester.lrh_lb_->stats().max_hashes_per_host_.value()};

    double update_ms_total = 0.0;
    double lookup_ms_total = 0.0;
    double changed_percent_total = 0.0;
    double changed_percent_max = 0.0;
    double weighted_subset_percent_total = 0.0;
    double target_error_percent_total = 0.0;
    double cv_total = 0.0;
    double max_over_avg_total = 0.0;
    double p99_over_avg_total = 0.0;
    double slot_shape_change_epochs = 0.0;
    double slots_delta_abs_total = 0.0;
    double min_slots_delta_abs_total = 0.0;
    double max_slots_delta_abs_total = 0.0;

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

      const SlotStats current_slots{tester.lrh_lb_->stats().size_.value(),
                                    tester.lrh_lb_->stats().min_hashes_per_host_.value(),
                                    tester.lrh_lb_->stats().max_hashes_per_host_.value()};
      const int64_t slots_delta =
          static_cast<int64_t>(current_slots.slots) - static_cast<int64_t>(previous_slots.slots);
      const int64_t min_slots_delta = static_cast<int64_t>(current_slots.min_slots_per_host) -
                                      static_cast<int64_t>(previous_slots.min_slots_per_host);
      const int64_t max_slots_delta = static_cast<int64_t>(current_slots.max_slots_per_host) -
                                      static_cast<int64_t>(previous_slots.max_slots_per_host);
      slots_delta_abs_total += std::abs(slots_delta);
      min_slots_delta_abs_total += std::abs(min_slots_delta);
      max_slots_delta_abs_total += std::abs(max_slots_delta);
      if (slots_delta != 0 || min_slots_delta != 0 || max_slots_delta != 0) {
        slot_shape_change_epochs += 1.0;
      }
      previous_slots = current_slots;

      LoadBalancerPtr lb = tester.lrh_lb_->factory()->create(tester.lb_params_);
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

    recordSequenceStats(state, num_hosts, epochs, keys_per_epoch, weighted_subset_percent,
                        max_weight, pattern, weighted_hosts, update_ms_total, lookup_ms_total,
                        changed_percent_total, changed_percent_max, weighted_subset_percent_total,
                        target_error_percent_total, cv_total, max_over_avg_total,
                        p99_over_avg_total, slot_shape_change_epochs, slots_delta_abs_total,
                        min_slots_delta_abs_total, max_slots_delta_abs_total, candidate_count);
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkLrhLoadBalancerWeightUpdateSequence)
    ->Args({100, 65536, 8, 20, 10000, 10, 32, 0})
    ->Args({100, 65536, 8, 20, 10000, 10, 32, 1})
    ->Args({500, 65536, 8, 20, 10000, 10, 32, 0})
    ->Args({500, 65536, 8, 20, 10000, 10, 32, 1})
    ->Args({500, 256000, 8, 20, 10000, 10, 32, 0})
    ->Args({500, 256000, 8, 20, 10000, 10, 32, 1})
    ->Unit(::benchmark::kMillisecond);

void benchmarkRingHashLoadBalancerWeightUpdateSequence(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint64_t min_ring_size = state.range(1);
    const uint32_t epochs = state.range(2);
    const uint64_t keys_per_epoch = state.range(3);
    const uint32_t weighted_subset_percent = state.range(4);
    const uint32_t max_weight = state.range(5);
    const uint32_t pattern = state.range(6);
    const uint64_t weighted_hosts = weightedSubsetHostCount(num_hosts, weighted_subset_percent);

    RingHashTester tester(num_hosts, min_ring_size);
    ASSERT_TRUE(tester.ring_hash_lb_->initialize().ok());
    std::vector<std::string> previous_assignments;
    previous_assignments.reserve(keys_per_epoch);
    SlotStats previous_slots{tester.ring_hash_lb_->stats().size_.value(),
                             tester.ring_hash_lb_->stats().min_hashes_per_host_.value(),
                             tester.ring_hash_lb_->stats().max_hashes_per_host_.value()};

    double update_ms_total = 0.0;
    double lookup_ms_total = 0.0;
    double changed_percent_total = 0.0;
    double changed_percent_max = 0.0;
    double weighted_subset_percent_total = 0.0;
    double target_error_percent_total = 0.0;
    double cv_total = 0.0;
    double max_over_avg_total = 0.0;
    double p99_over_avg_total = 0.0;
    double slot_shape_change_epochs = 0.0;
    double slots_delta_abs_total = 0.0;
    double min_slots_delta_abs_total = 0.0;
    double max_slots_delta_abs_total = 0.0;

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

      const SlotStats current_slots{tester.ring_hash_lb_->stats().size_.value(),
                                    tester.ring_hash_lb_->stats().min_hashes_per_host_.value(),
                                    tester.ring_hash_lb_->stats().max_hashes_per_host_.value()};
      const int64_t slots_delta =
          static_cast<int64_t>(current_slots.slots) - static_cast<int64_t>(previous_slots.slots);
      const int64_t min_slots_delta = static_cast<int64_t>(current_slots.min_slots_per_host) -
                                      static_cast<int64_t>(previous_slots.min_slots_per_host);
      const int64_t max_slots_delta = static_cast<int64_t>(current_slots.max_slots_per_host) -
                                      static_cast<int64_t>(previous_slots.max_slots_per_host);
      slots_delta_abs_total += std::abs(slots_delta);
      min_slots_delta_abs_total += std::abs(min_slots_delta);
      max_slots_delta_abs_total += std::abs(max_slots_delta);
      if (slots_delta != 0 || min_slots_delta != 0 || max_slots_delta != 0) {
        slot_shape_change_epochs += 1.0;
      }
      previous_slots = current_slots;

      LoadBalancerPtr lb = tester.ring_hash_lb_->factory()->create(tester.lb_params_);
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

    recordSequenceStats(state, num_hosts, epochs, keys_per_epoch, weighted_subset_percent,
                        max_weight, pattern, weighted_hosts, update_ms_total, lookup_ms_total,
                        changed_percent_total, changed_percent_max, weighted_subset_percent_total,
                        target_error_percent_total, cv_total, max_over_avg_total,
                        p99_over_avg_total, slot_shape_change_epochs, slots_delta_abs_total,
                        min_slots_delta_abs_total, max_slots_delta_abs_total);
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkRingHashLoadBalancerWeightUpdateSequence)
    ->Args({100, 65536, 20, 10000, 10, 32, 0})
    ->Args({100, 65536, 20, 10000, 10, 32, 1})
    ->Args({500, 65536, 20, 10000, 10, 32, 0})
    ->Args({500, 65536, 20, 10000, 10, 32, 1})
    ->Args({500, 256000, 20, 10000, 10, 32, 0})
    ->Args({500, 256000, 20, 10000, 10, 32, 1})
    ->Unit(::benchmark::kMillisecond);

void benchmarkMaglevLoadBalancerWeightUpdateSequence(::benchmark::State& state) {
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    state.PauseTiming();
    const uint64_t num_hosts = state.range(0);
    const uint64_t table_size = state.range(1);
    const uint32_t epochs = state.range(2);
    const uint64_t keys_per_epoch = state.range(3);
    const uint32_t weighted_subset_percent = state.range(4);
    const uint32_t max_weight = state.range(5);
    const uint32_t pattern = state.range(6);
    const uint64_t weighted_hosts = weightedSubsetHostCount(num_hosts, weighted_subset_percent);

    MaglevTester tester(num_hosts, table_size);
    ASSERT_TRUE(tester.maglev_lb_->initialize().ok());
    std::vector<std::string> previous_assignments;
    previous_assignments.reserve(keys_per_epoch);
    SlotStats previous_slots{tester.maglev_lb_->tableSize(),
                             tester.maglev_lb_->stats().min_entries_per_host_.value(),
                             tester.maglev_lb_->stats().max_entries_per_host_.value()};

    double update_ms_total = 0.0;
    double lookup_ms_total = 0.0;
    double changed_percent_total = 0.0;
    double changed_percent_max = 0.0;
    double weighted_subset_percent_total = 0.0;
    double target_error_percent_total = 0.0;
    double cv_total = 0.0;
    double max_over_avg_total = 0.0;
    double p99_over_avg_total = 0.0;
    double slot_shape_change_epochs = 0.0;
    double slots_delta_abs_total = 0.0;
    double min_slots_delta_abs_total = 0.0;
    double max_slots_delta_abs_total = 0.0;

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

      const SlotStats current_slots{tester.maglev_lb_->tableSize(),
                                    tester.maglev_lb_->stats().min_entries_per_host_.value(),
                                    tester.maglev_lb_->stats().max_entries_per_host_.value()};
      const int64_t slots_delta =
          static_cast<int64_t>(current_slots.slots) - static_cast<int64_t>(previous_slots.slots);
      const int64_t min_slots_delta = static_cast<int64_t>(current_slots.min_slots_per_host) -
                                      static_cast<int64_t>(previous_slots.min_slots_per_host);
      const int64_t max_slots_delta = static_cast<int64_t>(current_slots.max_slots_per_host) -
                                      static_cast<int64_t>(previous_slots.max_slots_per_host);
      slots_delta_abs_total += std::abs(slots_delta);
      min_slots_delta_abs_total += std::abs(min_slots_delta);
      max_slots_delta_abs_total += std::abs(max_slots_delta);
      if (slots_delta != 0 || min_slots_delta != 0 || max_slots_delta != 0) {
        slot_shape_change_epochs += 1.0;
      }
      previous_slots = current_slots;

      LoadBalancerPtr lb = tester.maglev_lb_->factory()->create(tester.lb_params_);
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

    recordSequenceStats(state, num_hosts, epochs, keys_per_epoch, weighted_subset_percent,
                        max_weight, pattern, weighted_hosts, update_ms_total, lookup_ms_total,
                        changed_percent_total, changed_percent_max, weighted_subset_percent_total,
                        target_error_percent_total, cv_total, max_over_avg_total,
                        p99_over_avg_total, slot_shape_change_epochs, slots_delta_abs_total,
                        min_slots_delta_abs_total, max_slots_delta_abs_total);
    state.ResumeTiming();
  }
}
BENCHMARK(benchmarkMaglevLoadBalancerWeightUpdateSequence)
    ->Args({100, 65537, 20, 10000, 10, 32, 0})
    ->Args({100, 65537, 20, 10000, 10, 32, 1})
    ->Args({500, 65537, 20, 10000, 10, 32, 0})
    ->Args({500, 65537, 20, 10000, 10, 32, 1})
    ->Args({500, 262147, 20, 10000, 10, 32, 0})
    ->Args({500, 262147, 20, 10000, 10, 32, 1})
    ->Unit(::benchmark::kMillisecond);

} // namespace
} // namespace Upstream
} // namespace Envoy
