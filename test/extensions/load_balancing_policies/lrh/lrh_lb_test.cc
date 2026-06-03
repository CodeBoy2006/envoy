#include <cmath>
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

#include "absl/strings/str_cat.h"
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

  bool containsHostPtr(const HostVector& hosts, const HostConstSharedPtr& host) {
    if (host == nullptr) {
      return false;
    }
    for (const auto& candidate : hosts) {
      if (candidate.get() == host.get()) {
        return true;
      }
    }
    return false;
  }

  HostConstSharedPtr choose(LoadBalancer& lb, uint64_t hash) {
    TestLoadBalancerContext context(hash);
    return lb.chooseHost(&context).host;
  }

  HostConstSharedPtr chooseKey(LoadBalancer& lb, uint64_t key) {
    return choose(lb, HashUtil::xxHash64Value(key));
  }

  std::string chooseAddress(LoadBalancer& lb, uint64_t hash) {
    auto host = choose(lb, hash);
    return host == nullptr ? "" : host->address()->asString();
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
    assignments_before.push_back(chooseKeyAddress(*before, key));
  }

  setHosts({hosts[0], hosts[1], hosts[2]});
  auto after = workerLb();
  uint64_t changed = 0;
  for (uint64_t key = 0; key < assignments_before.size(); ++key) {
    const HostConstSharedPtr next_host = chooseKey(*after, key);
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
    const HostConstSharedPtr host = chooseKey(*lb, key);
    ASSERT_NE(nullptr, host);
    if (host->address()->asString() == hosts[0]->address()->asString()) {
      ++heavy;
    }
  }
  EXPECT_GT(heavy, 650);
}

TEST_F(LrhLoadBalancerTest, DynamicWeightUpdateKeepsRingTopologyAndMovesTraffic) {
  const uint64_t keys = 5000;
  const std::string heavy_address = "127.0.0.1:90";
  config_.mutable_minimum_ring_size()->set_value(128);
  config_.mutable_candidate_count()->set_value(8);

  setHosts(makeWeightedHosts({1, 1, 1, 1}));
  init();
  const uint64_t ring_size = lb_->stats().size_.value();
  const uint64_t min_hashes = lb_->stats().min_hashes_per_host_.value();
  const uint64_t max_hashes = lb_->stats().max_hashes_per_host_.value();

  auto before_lb = workerLb();
  std::vector<std::string> before_assignments;
  before_assignments.reserve(keys);
  uint64_t before_heavy = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    const std::string address = chooseKeyAddress(*before_lb, key);
    before_assignments.push_back(address);
    if (address == heavy_address) {
      ++before_heavy;
    }
  }

  setHosts(makeWeightedHosts({32, 1, 1, 1}));
  EXPECT_EQ(ring_size, lb_->stats().size_.value());
  EXPECT_EQ(min_hashes, lb_->stats().min_hashes_per_host_.value());
  EXPECT_EQ(max_hashes, lb_->stats().max_hashes_per_host_.value());

  auto after_lb = workerLb();
  std::vector<std::string> after_assignments;
  after_assignments.reserve(keys);
  uint64_t after_heavy = 0;
  uint64_t changed = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    const std::string address = chooseKeyAddress(*after_lb, key);
    after_assignments.push_back(address);
    if (address == heavy_address) {
      ++after_heavy;
    }
    if (address != before_assignments[key]) {
      ++changed;
    }
  }

  EXPECT_GT(after_heavy, before_heavy * 2);
  EXPECT_GT(after_heavy, keys * 65 / 100);
  EXPECT_GT(changed, keys / 4);
  EXPECT_LT(changed, keys);
  EXPECT_EQ(0, lb_->stats().weight_only_updates_.value());
  EXPECT_EQ(1, lb_->stats().topology_cache_hits_.value());

  setHosts(makeWeightedHosts({320, 10, 10, 10}));
  EXPECT_EQ(ring_size, lb_->stats().size_.value());
  EXPECT_EQ(min_hashes, lb_->stats().min_hashes_per_host_.value());
  EXPECT_EQ(max_hashes, lb_->stats().max_hashes_per_host_.value());

  auto scaled_lb = workerLb();
  for (uint64_t key = 0; key < keys; ++key) {
    EXPECT_EQ(after_assignments[key], chooseKeyAddress(*scaled_lb, key))
        << "scaled weights changed winner for key " << key;
  }
}

TEST_F(LrhLoadBalancerTest, WeightOnlyHostSetRefreshUpdatesPublishedRing) {
  const uint64_t keys = 5000;
  const std::string heavy_address = "127.0.0.1:90";
  config_.mutable_minimum_ring_size()->set_value(128);
  config_.mutable_candidate_count()->set_value(8);

  HostVector hosts = makeWeightedHosts({1, 1, 1, 1});
  setHosts(hosts);
  init();
  EXPECT_EQ(0, lb_->stats().weight_only_updates_.value());
  EXPECT_EQ(1, lb_->stats().topology_cache_misses_.value());

  auto lb = workerLb();
  uint64_t before_heavy = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    if (chooseKeyAddress(*lb, key) == heavy_address) {
      ++before_heavy;
    }
  }

  mutateWeights(hosts, {32, 1, 1, 1});
  host_set_.runCallbacks({}, {});

  EXPECT_EQ(1, lb_->stats().weight_only_updates_.value());
  EXPECT_EQ(1, lb_->stats().topology_cache_misses_.value());
  EXPECT_EQ(0, lb_->stats().topology_cache_hits_.value());

  uint64_t after_heavy = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    if (chooseKeyAddress(*lb, key) == heavy_address) {
      ++after_heavy;
    }
  }
  EXPECT_GT(after_heavy, before_heavy * 2);
  EXPECT_GT(after_heavy, keys * 65 / 100);
}

TEST_F(LrhLoadBalancerTest, BoundedLoadWeightOnlyRefreshUpdatesPublishedRingAndBounds) {
  const uint64_t keys = 5000;
  const std::string heavy_address = "127.0.0.1:90";
  config_.mutable_minimum_ring_size()->set_value(128);
  config_.mutable_candidate_count()->set_value(8);
  config_.mutable_consistent_hashing_lb_config()->mutable_hash_balance_factor()->set_value(150);

  HostVector hosts = makeWeightedHosts({1, 1, 1, 1});
  setHosts(hosts);
  init();

  auto lb = workerLb();
  uint64_t before_heavy = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    if (chooseKeyAddress(*lb, key) == heavy_address) {
      ++before_heavy;
    }
  }

  mutateWeights(hosts, {32, 1, 1, 1});
  host_set_.runCallbacks({}, {});

  EXPECT_EQ(1, lb_->stats().weight_only_updates_.value());
  EXPECT_EQ(0, lb_->stats().topology_cache_hits_.value());

  uint64_t after_heavy = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    if (chooseKeyAddress(*lb, key) == heavy_address) {
      ++after_heavy;
    }
  }
  EXPECT_GT(after_heavy, before_heavy * 2);
  EXPECT_GT(after_heavy, keys * 65 / 100);
}

TEST_F(LrhLoadBalancerTest, EquivalentNewHostsReuseTopologyWithoutMutatingExistingWorkerRing) {
  config_.mutable_minimum_ring_size()->set_value(128);
  config_.mutable_candidate_count()->set_value(8);

  HostVector old_hosts = makeWeightedHosts({1, 1, 1, 1});
  setHosts(old_hosts);
  init();
  auto old_worker_lb = workerLb();
  ASSERT_EQ(1, lb_->stats().topology_cache_misses_.value());

  const uint64_t key = 17;
  const HostConstSharedPtr old_choice_before = chooseKey(*old_worker_lb, key);
  ASSERT_TRUE(containsHostPtr(old_hosts, old_choice_before));

  HostVector new_hosts = makeWeightedHosts({32, 1, 1, 1});
  setHosts(new_hosts);
  EXPECT_EQ(0, lb_->stats().weight_only_updates_.value());
  EXPECT_EQ(1, lb_->stats().topology_cache_hits_.value());
  EXPECT_EQ(1, lb_->stats().topology_cache_misses_.value());

  const HostConstSharedPtr old_choice_after = chooseKey(*old_worker_lb, key);
  EXPECT_TRUE(containsHostPtr(old_hosts, old_choice_after));

  auto new_worker_lb = workerLb();
  const HostConstSharedPtr new_choice = chooseKey(*new_worker_lb, key);
  EXPECT_TRUE(containsHostPtr(new_hosts, new_choice));
  EXPECT_FALSE(containsHostPtr(old_hosts, new_choice));
}

TEST_F(LrhLoadBalancerTest, ReorderedSameHostsDoesNotUseWeightOnlyFastPath) {
  config_.mutable_minimum_ring_size()->set_value(128);
  config_.mutable_candidate_count()->set_value(8);

  HostVector hosts = makeWeightedHosts({1, 1, 1, 1});
  setHosts(hosts);
  init();
  ASSERT_EQ(1, lb_->stats().topology_cache_misses_.value());

  mutateWeights(hosts, {32, 1, 1, 1});
  setHosts({hosts[1], hosts[0], hosts[2], hosts[3]});

  EXPECT_EQ(0, lb_->stats().weight_only_updates_.value());
  EXPECT_EQ(0, lb_->stats().topology_cache_hits_.value());
  EXPECT_EQ(2, lb_->stats().topology_cache_misses_.value());
}

TEST_F(LrhLoadBalancerTest, DirectWeightTableUpdateMovesTrafficWithoutRefresh) {
  const uint64_t keys = 5000;
  const std::string heavy_address = "127.0.0.1:90";
  config_.mutable_minimum_ring_size()->set_value(128);
  config_.mutable_candidate_count()->set_value(8);

  setHosts(makeWeightedHosts({1, 1, 1, 1}));
  init();
  const uint64_t ring_size = lb_->stats().size_.value();
  const uint64_t min_hashes = lb_->stats().min_hashes_per_host_.value();
  const uint64_t max_hashes = lb_->stats().max_hashes_per_host_.value();
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

  const std::vector<std::pair<uint32_t, double>> updates{{0, 32.0 * base_weight}};
  ASSERT_TRUE(lb_->updateHostWeightsForTest(updates));
  EXPECT_EQ(ring_size, lb_->stats().size_.value());
  EXPECT_EQ(min_hashes, lb_->stats().min_hashes_per_host_.value());
  EXPECT_EQ(max_hashes, lb_->stats().max_hashes_per_host_.value());

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
  EXPECT_GT(after_heavy, keys * 65 / 100);
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

TEST_F(LrhLoadBalancerTest, WindowDebiasedWeightsImproveCapacityFitBelowExposureCap) {
  const uint64_t keys = 20000;
  const std::string heavy_address = "127.0.0.1:90";
  config_.mutable_minimum_ring_size()->set_value(128);
  config_.mutable_candidate_count()->set_value(4);

  setHosts(makeWeightedHosts({4, 1, 1, 1, 1, 1, 1, 1}));
  init();
  auto direct_lb = workerLb();
  uint64_t direct_heavy = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    if (chooseKeyAddress(*direct_lb, key) == heavy_address) {
      ++direct_heavy;
    }
  }

  config_.set_weight_preprocessing(LrhLbProto::WINDOW_DEBIASED);
  init();
  auto debiased_lb = workerLb();
  uint64_t debiased_heavy = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    if (chooseKeyAddress(*debiased_lb, key) == heavy_address) {
      ++debiased_heavy;
    }
  }

  const double target_share = 4.0 / 11.0;
  const double direct_error = std::abs(static_cast<double>(direct_heavy) / keys - target_share);
  const double debiased_error = std::abs(static_cast<double>(debiased_heavy) / keys - target_share);
  EXPECT_LT(debiased_error, direct_error)
      << "direct=" << direct_heavy << " debiased=" << debiased_heavy;
}

TEST_F(LrhLoadBalancerTest, WindowDebiasedWeightOnlyRefreshUpdatesPublishedRing) {
  const uint64_t keys = 5000;
  const std::string heavy_address = "127.0.0.1:90";
  config_.mutable_minimum_ring_size()->set_value(128);
  config_.mutable_candidate_count()->set_value(4);
  config_.set_weight_preprocessing(LrhLbProto::WINDOW_DEBIASED);

  HostVector hosts = makeWeightedHosts({1, 1, 1, 1, 1, 1, 1, 1});
  setHosts(hosts);
  init();
  auto lb = workerLb();

  uint64_t before_heavy = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    if (chooseKeyAddress(*lb, key) == heavy_address) {
      ++before_heavy;
    }
  }

  mutateWeights(hosts, {4, 1, 1, 1, 1, 1, 1, 1});
  host_set_.runCallbacks({}, {});

  EXPECT_EQ(1, lb_->stats().weight_only_updates_.value());
  EXPECT_EQ(1, lb_->stats().topology_cache_misses_.value());
  EXPECT_EQ(0, lb_->stats().topology_cache_hits_.value());

  uint64_t after_heavy = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    if (chooseKeyAddress(*lb, key) == heavy_address) {
      ++after_heavy;
    }
  }
  EXPECT_GT(after_heavy, before_heavy * 2);
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

TEST_F(LrhLoadBalancerTest, HealthySubsetWeightOnlyRefreshUsesOnlyHealthyHosts) {
  const uint64_t keys = 5000;
  const std::string heavy_address = "127.0.0.1:90";
  HostVector hosts = makeWeightedHosts({1, 1, 1, 1});
  host_set_.hosts_ = hosts;
  host_set_.healthy_hosts_ = {hosts[0], hosts[1], hosts[2]};
  host_set_.runCallbacks({}, {});
  config_.mutable_minimum_ring_size()->set_value(128);
  config_.mutable_candidate_count()->set_value(8);

  init();
  auto lb = workerLb();
  uint64_t before_heavy = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    const HostConstSharedPtr host = chooseKey(*lb, key);
    ASSERT_NE(nullptr, host);
    EXPECT_NE(hosts[3], host);
    if (host->address()->asString() == heavy_address) {
      ++before_heavy;
    }
  }

  mutateWeights(hosts, {32, 1, 1, 1});
  host_set_.runCallbacks({}, {});
  EXPECT_EQ(1, lb_->stats().weight_only_updates_.value());
  EXPECT_EQ(1, lb_->stats().topology_cache_misses_.value());

  uint64_t after_heavy = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    const HostConstSharedPtr host = chooseKey(*lb, key);
    ASSERT_NE(nullptr, host);
    EXPECT_NE(hosts[3], host);
    if (host->address()->asString() == heavy_address) {
      ++after_heavy;
    }
  }
  EXPECT_GT(after_heavy, before_heavy * 2);

  host_set_.healthy_hosts_ = hosts;
  host_set_.runCallbacks({}, {});
  EXPECT_EQ(1, lb_->stats().weight_only_updates_.value());
  EXPECT_EQ(2, lb_->stats().topology_cache_misses_.value());
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
