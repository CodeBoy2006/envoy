#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "envoy/config/cluster/v3/cluster.pb.h"

#include "source/common/common/hash.h"
#include "source/common/upstream/load_balancer_context_base.h"
#include "source/extensions/load_balancing_policies/anchorflow/anchorflow_lb.h"

#include "test/common/upstream/utility.h"
#include "test/mocks/server/server_factory_context.h"
#include "test/mocks/upstream/cluster_info.h"
#include "test/mocks/upstream/host_set.h"
#include "test/mocks/upstream/priority_set.h"
#include "test/test_common/simulated_time_system.h"
#include "test/test_common/test_runtime.h"

#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace Envoy {
namespace Upstream {
namespace {

class TestLoadBalancerContext : public LoadBalancerContextBase {
public:
  explicit TestLoadBalancerContext(uint64_t hash_key) : hash_key_(hash_key) {}

  absl::optional<uint64_t> computeHashKey() override { return hash_key_; }

  absl::optional<uint64_t> hash_key_;
};

class AnchorFlowLoadBalancerTest : public Event::TestUsingSimulatedTime, public testing::Test {
public:
  AnchorFlowLoadBalancerTest()
      : stat_names_(stats_store_.symbolTable()), stats_(stat_names_, *stats_store_.rootScope()) {}

  void init() {
    absl::Status creation_status;
    TypedAnchorFlowLbConfig typed_config(config_, context_.regex_engine_, creation_status);
    ASSERT_TRUE(creation_status.ok());

    lb_ = std::make_unique<AnchorFlowLoadBalancer>(
        priority_set_, stats_, *stats_store_.rootScope(), context_.runtime_loader_,
        context_.api_.random_, 50, typed_config.lb_config_, typed_config.hash_policy_);
    ASSERT_TRUE(lb_->initialize().ok());
  }

  LoadBalancerPtr workerLb() { return lb_->factory()->create(lb_params_); }

  void setHosts(const HostVector& hosts) {
    host_set_.hosts_ = hosts;
    host_set_.healthy_hosts_ = hosts;
    host_set_.runCallbacks({}, {});
  }

  HostVector makeWeightedHosts(const std::vector<uint32_t>& weights) {
    HostVector hosts;
    hosts.reserve(weights.size());
    for (size_t i = 0; i < weights.size(); ++i) {
      hosts.push_back(makeTestHost(info_, absl::StrCat("tcp://127.0.0.1:", 90 + i), weights[i]));
    }
    return hosts;
  }

  void mutateWeights(const HostVector& hosts, const std::vector<uint32_t>& weights) {
    ASSERT_EQ(hosts.size(), weights.size());
    for (size_t i = 0; i < hosts.size(); ++i) {
      hosts[i]->weight(weights[i]);
    }
  }

  HostConstSharedPtr choose(LoadBalancer& lb, uint64_t hash) {
    TestLoadBalancerContext context(hash);
    return lb.chooseHost(&context).host;
  }

  HostConstSharedPtr chooseKey(LoadBalancer& lb, uint64_t key) {
    return choose(lb, HashUtil::xxHash64Value(key));
  }

  std::string chooseKeyAddress(LoadBalancer& lb, uint64_t key) {
    auto host = chooseKey(lb, key);
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
  envoy::extensions::load_balancing_policies::anchorflow::v3::AnchorFlow config_;
  NiceMock<Server::Configuration::MockServerFactoryContext> context_;
  std::unique_ptr<AnchorFlowLoadBalancer> lb_;
};

TEST_F(AnchorFlowLoadBalancerTest, NoHost) {
  init();
  EXPECT_EQ(nullptr, workerLb()->chooseHost(nullptr).host);
}

TEST_F(AnchorFlowLoadBalancerTest, BadAnchorBucketBounds) {
  setHosts(makeWeightedHosts({1, 1, 1}));
  config_.mutable_tokens_per_host()->set_value(4);
  config_.mutable_anchor_buckets()->set_value(8);

  EXPECT_THROW_WITH_MESSAGE(init(), EnvoyException,
                            "anchorflow: anchor_buckets (8) must be >= token_count (12)");
}

TEST_F(AnchorFlowLoadBalancerTest, WeightedTargetTracksHostWeights) {
  HostVector hosts = makeWeightedHosts({1, 1, 1, 9});
  setHosts(hosts);
  config_.mutable_tokens_per_host()->set_value(32);

  init();
  auto lb = workerLb();
  uint64_t heavy = 0;
  constexpr uint64_t Keys = 50'000;
  for (uint64_t key = 0; key < Keys; ++key) {
    const HostConstSharedPtr host = chooseKey(*lb, key);
    ASSERT_NE(nullptr, host);
    if (host->address()->asString() == hosts[3]->address()->asString()) {
      ++heavy;
    }
  }

  const double heavy_fraction = static_cast<double>(heavy) / static_cast<double>(Keys);
  EXPECT_GT(heavy_fraction, 0.70);
  EXPECT_LT(heavy_fraction, 0.80);
}

TEST_F(AnchorFlowLoadBalancerTest, OnlySurplusAnchorOwnersMoveOnDrain) {
  HostVector hosts = makeWeightedHosts({10, 10, 10, 10});
  setHosts(hosts);
  config_.mutable_tokens_per_host()->set_value(32);

  init();
  auto equal_lb = workerLb();
  std::vector<std::string> equal_assignments;
  constexpr uint64_t Keys = 20'000;
  equal_assignments.reserve(Keys);
  for (uint64_t key = 0; key < Keys; ++key) {
    equal_assignments.push_back(chooseKeyAddress(*equal_lb, key));
  }

  mutateWeights(hosts, {1, 3, 3, 3});
  setHosts(hosts);
  auto drained_lb = workerLb();

  uint64_t moved_from_drained = 0;
  const std::string drained_host = hosts[0]->address()->asString();
  for (uint64_t key = 0; key < Keys; ++key) {
    const std::string after = chooseKeyAddress(*drained_lb, key);
    ASSERT_FALSE(after.empty());
    if (equal_assignments[key] == drained_host && after != drained_host) {
      ++moved_from_drained;
    }
    if (equal_assignments[key] != drained_host) {
      EXPECT_EQ(equal_assignments[key], after);
    }
  }
  EXPECT_GT(moved_from_drained, 1000);
}

TEST_F(AnchorFlowLoadBalancerTest, StableReceiverSeedKeepsReceiverChoiceAcrossEpochs) {
  HostVector hosts = makeWeightedHosts({10, 10, 10, 10});
  setHosts(hosts);
  config_.mutable_tokens_per_host()->set_value(32);

  init();
  auto equal_lb = workerLb();
  std::vector<std::string> anchor_assignments;
  constexpr uint64_t Keys = 30'000;
  anchor_assignments.reserve(Keys);
  for (uint64_t key = 0; key < Keys; ++key) {
    anchor_assignments.push_back(chooseKeyAddress(*equal_lb, key));
  }

  mutateWeights(hosts, {1, 3, 3, 1});
  setHosts(hosts);
  auto epoch_one_lb = workerLb();
  std::vector<std::string> epoch_one_assignments;
  epoch_one_assignments.reserve(Keys);
  for (uint64_t key = 0; key < Keys; ++key) {
    epoch_one_assignments.push_back(chooseKeyAddress(*epoch_one_lb, key));
  }

  mutateWeights(hosts, {1, 6, 6, 1});
  setHosts(hosts);
  auto epoch_two_lb = workerLb();

  uint64_t stable_receiver_checks = 0;
  const std::string donor = hosts[0]->address()->asString();
  for (uint64_t key = 0; key < Keys; ++key) {
    if (anchor_assignments[key] != donor || epoch_one_assignments[key] == donor) {
      continue;
    }

    const std::string epoch_two = chooseKeyAddress(*epoch_two_lb, key);
    if (epoch_two == donor) {
      continue;
    }

    EXPECT_EQ(epoch_one_assignments[key], epoch_two);
    ++stable_receiver_checks;
  }

  EXPECT_GT(stable_receiver_checks, 1000);
}

TEST_F(AnchorFlowLoadBalancerTest, DirectCapacityStateUpdateMovesTrafficWithoutRefresh) {
  const uint64_t keys = 5000;
  const std::string heavy_address = "127.0.0.1:90";
  config_.mutable_tokens_per_host()->set_value(32);

  setHosts(makeWeightedHosts({1, 1, 1, 1}));
  init();
  const uint64_t tokens = lb_->stats().tokens_.value();
  const uint64_t anchor_buckets = lb_->stats().anchor_buckets_.value();
  const double base_weight = 1.0 / 4.0;

  auto lb = workerLb();
  std::vector<std::string> before_assignments;
  before_assignments.reserve(keys);
  uint64_t before_heavy = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    const std::string address = chooseKeyAddress(*lb, key);
    before_assignments.push_back(address);
    if (address == heavy_address) {
      ++before_heavy;
    }
  }

  const std::vector<double> direct_weights{32.0 * base_weight, base_weight, base_weight,
                                           base_weight};
  ASSERT_TRUE(lb_->updateWeightsForTest(direct_weights));
  EXPECT_EQ(tokens, lb_->stats().tokens_.value());
  EXPECT_EQ(anchor_buckets, lb_->stats().anchor_buckets_.value());

  std::vector<std::string> after_assignments;
  after_assignments.reserve(keys);
  uint64_t after_heavy = 0;
  uint64_t changed = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    const std::string address = chooseKeyAddress(*lb, key);
    after_assignments.push_back(address);
    if (address == heavy_address) {
      ++after_heavy;
    }
    if (address != before_assignments[key]) {
      ++changed;
    }
  }

  EXPECT_GT(after_heavy, before_heavy * 2);
  EXPECT_GT(after_heavy, keys * 70 / 100);
  EXPECT_GT(changed, keys / 4);
  EXPECT_LT(changed, keys);

  const std::vector<double> scaled_weights{320.0 * base_weight, 10.0 * base_weight,
                                           10.0 * base_weight, 10.0 * base_weight};
  ASSERT_TRUE(lb_->updateWeightsForTest(scaled_weights));
  for (uint64_t key = 0; key < keys; ++key) {
    EXPECT_EQ(after_assignments[key], chooseKeyAddress(*lb, key))
        << "scaled direct weights changed winner for key " << key;
  }
}

} // namespace
} // namespace Upstream
} // namespace Envoy
