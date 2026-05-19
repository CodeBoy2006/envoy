#include "source/extensions/load_balancing_policies/lrh/lrh_lb.h"
#include "source/extensions/load_balancing_policies/maglev/maglev_lb.h"
#include "source/extensions/load_balancing_policies/ring_hash/ring_hash_lb.h"

#include "test/benchmark/main.h"
#include "test/extensions/load_balancing_policies/common/benchmark_base_tester.h"

namespace Envoy {
namespace Upstream {
namespace {

class LrhTester : public BaseTester {
public:
  LrhTester(uint64_t num_hosts, uint64_t min_ring_size, uint32_t candidate_count)
      : BaseTester(num_hosts) {
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
  RingHashTester(uint64_t num_hosts, uint64_t min_ring_size) : BaseTester(num_hosts) {
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
  MaglevTester(uint64_t num_hosts, uint64_t table_size) : BaseTester(num_hosts) {
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

} // namespace
} // namespace Upstream
} // namespace Envoy
