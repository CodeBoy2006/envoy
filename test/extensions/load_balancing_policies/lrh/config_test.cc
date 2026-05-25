#include "envoy/config/core/v3/extension.pb.h"
#include "envoy/extensions/load_balancing_policies/lrh/v3/lrh.pb.h"
#include "envoy/extensions/load_balancing_policies/lrh/v3/lrh.pb.validate.h"

#include "source/extensions/load_balancing_policies/lrh/config.h"

#include "test/mocks/server/server_factory_context.h"
#include "test/mocks/upstream/cluster_info.h"
#include "test/mocks/upstream/priority_set.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using testing::HasSubstr;

namespace Envoy {
namespace Extensions {
namespace LoadBalancingPolicies {
namespace Lrh {
namespace {

TEST(LrhConfigTest, Validate) {
  NiceMock<Server::Configuration::MockServerFactoryContext> context;
  NiceMock<Upstream::MockClusterInfo> cluster_info;
  NiceMock<Upstream::MockPrioritySet> main_thread_priority_set;
  NiceMock<Upstream::MockPrioritySet> thread_local_priority_set;

  {
    envoy::config::core::v3::TypedExtensionConfig config;
    config.set_name("envoy.load_balancing_policies.lrh");
    envoy::extensions::load_balancing_policies::lrh::v3::LocalRendezvousHashing config_msg;
    config.mutable_typed_config()->PackFrom(config_msg);

    auto& factory = Config::Utility::getAndCheckFactory<Upstream::TypedLoadBalancerFactory>(config);
    EXPECT_EQ("envoy.load_balancing_policies.lrh", factory.name());

    auto lb_config = factory.loadConfig(context, *factory.createEmptyConfigProto()).value();
    auto thread_aware_lb =
        factory.create(*lb_config, cluster_info, main_thread_priority_set, context.runtime_loader_,
                       context.api_.random_, context.time_system_);
    EXPECT_NE(nullptr, thread_aware_lb);

    ASSERT_TRUE(thread_aware_lb->initialize().ok());

    auto thread_local_lb_factory = thread_aware_lb->factory();
    EXPECT_NE(nullptr, thread_local_lb_factory);

    auto thread_local_lb = thread_local_lb_factory->create({thread_local_priority_set, nullptr});
    EXPECT_NE(nullptr, thread_local_lb);
  }

  {
    envoy::extensions::load_balancing_policies::lrh::v3::LocalRendezvousHashing config_msg;
    config_msg.mutable_minimum_ring_size()->set_value(4);
    config_msg.mutable_maximum_ring_size()->set_value(2);

    auto& factory = Config::Utility::getAndCheckFactoryByName<Upstream::TypedLoadBalancerFactory>(
        "envoy.load_balancing_policies.lrh");
    auto message_ptr = factory.createEmptyConfigProto();
    message_ptr->MergeFrom(config_msg);
    auto lb_config = factory.loadConfig(context, *message_ptr).value();

    EXPECT_THROW_WITH_MESSAGE(factory.create(*lb_config, cluster_info, main_thread_priority_set,
                                             context.runtime_loader_, context.api_.random_,
                                             context.time_system_),
                              EnvoyException, "lrh: minimum_ring_size (4) > maximum_ring_size (2)");
  }

  {
    envoy::extensions::load_balancing_policies::lrh::v3::LocalRendezvousHashing config_msg;
    config_msg.mutable_candidate_count()->set_value(0);
    std::string err;
    EXPECT_FALSE(Validate(config_msg, &err));
    EXPECT_THAT(err, HasSubstr("LocalRendezvousHashingValidationError.CandidateCount"));
    EXPECT_THAT(err, HasSubstr("range [1, 64]"));
  }

  {
    envoy::extensions::load_balancing_policies::lrh::v3::LocalRendezvousHashing config_msg;
    config_msg.mutable_candidate_count()->set_value(65);
    std::string err;
    EXPECT_FALSE(Validate(config_msg, &err));
    EXPECT_THAT(err, HasSubstr("LocalRendezvousHashingValidationError.CandidateCount"));
    EXPECT_THAT(err, HasSubstr("range [1, 64]"));
  }

  {
    envoy::extensions::load_balancing_policies::lrh::v3::LocalRendezvousHashing config_msg;
    config_msg.mutable_maximum_ring_size()->set_value(8388609);
    std::string err;
    EXPECT_FALSE(Validate(config_msg, &err));
    EXPECT_THAT(err, HasSubstr("LocalRendezvousHashingValidationError.MaximumRingSize"));
    EXPECT_THAT(err, HasSubstr("8388608"));
  }

  {
    envoy::extensions::load_balancing_policies::lrh::v3::LocalRendezvousHashing config_msg;
    config_msg.set_weight_preprocessing(
        static_cast<envoy::extensions::load_balancing_policies::lrh::v3::LocalRendezvousHashing::
                        WeightPreprocessing>(42));
    std::string err;
    EXPECT_FALSE(Validate(config_msg, &err));
    EXPECT_THAT(err, HasSubstr("LocalRendezvousHashingValidationError.WeightPreprocessing"));
  }
}

} // namespace
} // namespace Lrh
} // namespace LoadBalancingPolicies
} // namespace Extensions
} // namespace Envoy
