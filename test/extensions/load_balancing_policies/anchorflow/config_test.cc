#include "envoy/config/core/v3/extension.pb.h"
#include "envoy/extensions/load_balancing_policies/anchorflow/v3/anchorflow.pb.h"
#include "envoy/extensions/load_balancing_policies/anchorflow/v3/anchorflow.pb.validate.h"

#include "source/extensions/load_balancing_policies/anchorflow/config.h"

#include "test/mocks/server/server_factory_context.h"
#include "test/mocks/upstream/cluster_info.h"
#include "test/mocks/upstream/priority_set.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using testing::HasSubstr;

namespace Envoy {
namespace Extensions {
namespace LoadBalancingPolicies {
namespace AnchorFlow {
namespace {

TEST(AnchorFlowConfigTest, Validate) {
  NiceMock<Server::Configuration::MockServerFactoryContext> context;
  NiceMock<Upstream::MockClusterInfo> cluster_info;
  NiceMock<Upstream::MockPrioritySet> main_thread_priority_set;
  NiceMock<Upstream::MockPrioritySet> thread_local_priority_set;

  {
    envoy::config::core::v3::TypedExtensionConfig config;
    config.set_name("envoy.load_balancing_policies.anchorflow");
    envoy::extensions::load_balancing_policies::anchorflow::v3::AnchorFlow config_msg;
    config.mutable_typed_config()->PackFrom(config_msg);

    auto& factory = Config::Utility::getAndCheckFactory<Upstream::TypedLoadBalancerFactory>(config);
    EXPECT_EQ("envoy.load_balancing_policies.anchorflow", factory.name());

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
    envoy::extensions::load_balancing_policies::anchorflow::v3::AnchorFlow config_msg;
    config_msg.mutable_tokens_per_host()->set_value(0);
    std::string err;
    EXPECT_FALSE(Validate(config_msg, &err));
    EXPECT_THAT(err, HasSubstr("AnchorFlowValidationError.TokensPerHost"));
    EXPECT_THAT(err, HasSubstr("range [1, 4096]"));
  }

  {
    envoy::extensions::load_balancing_policies::anchorflow::v3::AnchorFlow config_msg;
    config_msg.mutable_tokens_per_host()->set_value(4097);
    std::string err;
    EXPECT_FALSE(Validate(config_msg, &err));
    EXPECT_THAT(err, HasSubstr("AnchorFlowValidationError.TokensPerHost"));
    EXPECT_THAT(err, HasSubstr("range [1, 4096]"));
  }
}

} // namespace
} // namespace AnchorFlow
} // namespace LoadBalancingPolicies
} // namespace Extensions
} // namespace Envoy
