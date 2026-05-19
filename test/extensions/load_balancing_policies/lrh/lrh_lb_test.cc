#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "envoy/config/cluster/v3/cluster.pb.h"

#include "source/common/upstream/load_balancer_context_base.h"
#include "source/extensions/load_balancing_policies/lrh/lrh_lb.h"

#include "test/common/upstream/utility.h"
#include "test/mocks/server/server_factory_context.h"
#include "test/mocks/upstream/cluster_info.h"
#include "test/mocks/upstream/host_set.h"
#include "test/mocks/upstream/priority_set.h"
#include "test/test_common/simulated_time_system.h"
#include "test/test_common/test_runtime.h"

#include "absl/types/optional.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using testing::HasSubstr;
using testing::NiceMock;
using testing::Not;

namespace Envoy {
namespace Upstream {
namespace {

class TestLoadBalancerContext : public LoadBalancerContextBase {
public:
  using HostPredicate = std::function<bool(const Host&)>;

  explicit TestLoadBalancerContext(uint64_t hash_key) : hash_key_(hash_key) {}
  TestLoadBalancerContext(uint64_t hash_key, uint32_t retry_count,
                          HostPredicate should_select_another_host)
      : hash_key_(hash_key), retry_count_(retry_count),
        should_select_another_host_(std::move(should_select_another_host)) {}

  absl::optional<uint64_t> computeHashKey() override { return hash_key_; }
  uint32_t hostSelectionRetryCount() const override { return retry_count_; }
  bool shouldSelectAnotherHost(const Host& host) override {
    return should_select_another_host_(host);
  }

  absl::optional<uint64_t> hash_key_;
  uint32_t retry_count_{0};
  HostPredicate should_select_another_host_ = [](const Host&) { return false; };
};

class LrhLoadBalancerTest : public Event::TestUsingSimulatedTime, public testing::Test {
public:
  LrhLoadBalancerTest()
      : stat_names_(stats_store_.symbolTable()), stats_(stat_names_, *stats_store_.rootScope()) {}

  void init() {
    absl::Status creation_status;
    TypedLrhLbConfig typed_config(config_, context_.regex_engine_, creation_status);
    ASSERT_TRUE(creation_status.ok());

    lb_ = std::make_unique<LrhLoadBalancer>(priority_set_, stats_, *stats_store_.rootScope(),
                                            context_.runtime_loader_, context_.api_.random_, 50,
                                            typed_config.lb_config_, typed_config.hash_policy_);
    ASSERT_TRUE(lb_->initialize().ok());
  }

  LoadBalancerPtr workerLb() { return lb_->factory()->create(lb_params_); }

  void setHosts(const HostVector& hosts) {
    host_set_.hosts_ = hosts;
    host_set_.healthy_hosts_ = hosts;
    host_set_.runCallbacks({}, {});
  }

  HostConstSharedPtr choose(LoadBalancer& lb, uint64_t hash) {
    TestLoadBalancerContext context(hash);
    return lb.chooseHost(&context).host;
  }

  std::string chooseAddress(LoadBalancer& lb, uint64_t hash) {
    auto host = choose(lb, hash);
    return host == nullptr ? "" : host->address()->asString();
  }

  NiceMock<MockPrioritySet> priority_set_;
  NiceMock<MockPrioritySet> worker_priority_set_;
  LoadBalancerParams lb_params_{worker_priority_set_, {}};
  MockHostSet& host_set_ = *priority_set_.getMockHostSet(0);
  std::shared_ptr<MockClusterInfo> info_{new NiceMock<MockClusterInfo>()};
  Stats::IsolatedStoreImpl stats_store_;
  ClusterLbStatNames stat_names_;
  ClusterLbStats stats_;
  envoy::extensions::load_balancing_policies::lrh::v3::LocalRendezvousHashing config_;
  NiceMock<Server::Configuration::MockServerFactoryContext> context_;
  std::unique_ptr<LrhLoadBalancer> lb_;
};

TEST_F(LrhLoadBalancerTest, NoHost) {
  init();
  EXPECT_EQ(nullptr, workerLb()->chooseHost(nullptr).host);
}

TEST_F(LrhLoadBalancerTest, BadRingSizeBounds) {
  config_.mutable_minimum_ring_size()->set_value(20);
  config_.mutable_maximum_ring_size()->set_value(10);
  EXPECT_THROW_WITH_MESSAGE(init(), EnvoyException,
                            "lrh: minimum_ring_size (20) > maximum_ring_size (10)");
}

TEST_F(LrhLoadBalancerTest, DeterministicSelectionForFixedHash) {
  setHosts({makeTestHost(info_, "tcp://127.0.0.1:90"), makeTestHost(info_, "tcp://127.0.0.1:91"),
            makeTestHost(info_, "tcp://127.0.0.1:92"), makeTestHost(info_, "tcp://127.0.0.1:93")});
  config_.mutable_minimum_ring_size()->set_value(32);
  config_.mutable_candidate_count()->set_value(4);

  init();
  auto lb = workerLb();
  const std::string first = chooseAddress(*lb, 1234567);
  EXPECT_FALSE(first.empty());
  for (uint32_t i = 0; i < 20; ++i) {
    EXPECT_EQ(first, chooseAddress(*lb, 1234567));
  }
  EXPECT_EQ(32, lb_->stats().size_.value());
}

TEST_F(LrhLoadBalancerTest, RetryPredicateFallsBackToDifferentCandidateBlock) {
  setHosts({makeTestHost(info_, "tcp://127.0.0.1:90"), makeTestHost(info_, "tcp://127.0.0.1:91"),
            makeTestHost(info_, "tcp://127.0.0.1:92"), makeTestHost(info_, "tcp://127.0.0.1:93")});
  config_.mutable_minimum_ring_size()->set_value(32);
  config_.mutable_candidate_count()->set_value(4);

  init();
  auto lb = workerLb();
  bool found_alternate = false;
  for (uint64_t key = 0; key < 100 && !found_alternate; ++key) {
    TestLoadBalancerContext first_context(key);
    auto first = lb->chooseHost(&first_context).host;
    ASSERT_NE(nullptr, first);

    TestLoadBalancerContext retry_context(key, 16, [first](const Host& host) {
      return host.address()->asString() == first->address()->asString();
    });
    auto retry = lb->chooseHost(&retry_context).host;
    ASSERT_NE(nullptr, retry);
    found_alternate = first->address()->asString() != retry->address()->asString();
  }
  EXPECT_TRUE(found_alternate);
}

TEST_F(LrhLoadBalancerTest, MembershipRemovalRebuildsRing) {
  HostVector hosts = {
      makeTestHost(info_, "tcp://127.0.0.1:90"), makeTestHost(info_, "tcp://127.0.0.1:91"),
      makeTestHost(info_, "tcp://127.0.0.1:92"), makeTestHost(info_, "tcp://127.0.0.1:93")};
  setHosts(hosts);
  config_.mutable_minimum_ring_size()->set_value(64);
  config_.mutable_candidate_count()->set_value(4);

  init();
  auto before = workerLb();
  std::vector<std::string> assignments_before;
  for (uint64_t key = 0; key < 200; ++key) {
    assignments_before.push_back(chooseAddress(*before, key));
  }

  setHosts({hosts[0], hosts[1], hosts[2]});
  auto after = workerLb();
  uint64_t changed = 0;
  for (uint64_t key = 0; key < assignments_before.size(); ++key) {
    const HostConstSharedPtr next_host = choose(*after, key);
    ASSERT_NE(nullptr, next_host);
    const std::string next = next_host->address()->asString();
    EXPECT_THAT(next, Not(HasSubstr(":93")));
    if (next != assignments_before[key]) {
      ++changed;
    }
  }
  EXPECT_GT(changed, 0);
  EXPECT_LT(changed, assignments_before.size());
}

TEST_F(LrhLoadBalancerTest, WeightedHostReceivesMoreTraffic) {
  HostVector hosts = {
      makeTestHost(info_, "tcp://127.0.0.1:90", 32), makeTestHost(info_, "tcp://127.0.0.1:91", 1),
      makeTestHost(info_, "tcp://127.0.0.1:92", 1), makeTestHost(info_, "tcp://127.0.0.1:93", 1)};
  setHosts(hosts);
  config_.mutable_minimum_ring_size()->set_value(128);
  config_.mutable_candidate_count()->set_value(8);

  init();
  auto lb = workerLb();
  uint64_t heavy = 0;
  for (uint64_t key = 0; key < 1000; ++key) {
    if (choose(*lb, key) == hosts[0]) {
      ++heavy;
    }
  }
  EXPECT_GT(heavy, 650);
}

TEST_F(LrhLoadBalancerTest, HealthFilteringSkipsUnhealthyHosts) {
  HostVector hosts = {
      makeTestHost(info_, "tcp://127.0.0.1:90"), makeTestHost(info_, "tcp://127.0.0.1:91"),
      makeTestHost(info_, "tcp://127.0.0.1:92"), makeTestHost(info_, "tcp://127.0.0.1:93")};
  host_set_.hosts_ = hosts;
  host_set_.healthy_hosts_ = {hosts[0], hosts[1], hosts[2]};
  host_set_.runCallbacks({}, {});
  config_.mutable_minimum_ring_size()->set_value(64);
  config_.mutable_candidate_count()->set_value(4);

  init();
  auto lb = workerLb();
  for (uint64_t key = 0; key < 500; ++key) {
    EXPECT_NE(hosts[3], choose(*lb, key));
  }
}

TEST_F(LrhLoadBalancerTest, RawAndDeduplicatedCandidateWindowsCanDiffer) {
  HostVector hosts = {makeTestHost(info_, "tcp://127.0.0.1:90"),
                      makeTestHost(info_, "tcp://127.0.0.1:91"),
                      makeTestHost(info_, "tcp://127.0.0.1:92")};
  setHosts(hosts);
  config_.mutable_minimum_ring_size()->set_value(96);
  config_.mutable_candidate_count()->set_value(2);

  init();
  auto raw_lb = workerLb();

  config_.set_deduplicate_hosts(true);
  init();
  auto dedup_lb = workerLb();

  bool found_difference = false;
  for (uint64_t key = 0; key < 5000; ++key) {
    if (chooseAddress(*raw_lb, key) != chooseAddress(*dedup_lb, key)) {
      found_difference = true;
      break;
    }
  }
  EXPECT_TRUE(found_difference);
}

} // namespace
} // namespace Upstream
} // namespace Envoy
