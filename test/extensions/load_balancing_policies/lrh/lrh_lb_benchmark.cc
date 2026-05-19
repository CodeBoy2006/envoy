#include "source/extensions/load_balancing_policies/lrh/lrh_lb.h"

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
    computeHitStats(state, hit_counter);
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

} // namespace
} // namespace Upstream
} // namespace Envoy
