#pragma once

#include <memory>

#include "envoy/extensions/load_balancing_policies/anchorflow/v3/anchorflow.pb.h"
#include "envoy/extensions/load_balancing_policies/anchorflow/v3/anchorflow.pb.validate.h"
#include "envoy/upstream/load_balancer.h"

#include "source/common/upstream/load_balancer_factory_base.h"
#include "source/extensions/load_balancing_policies/anchorflow/anchorflow_lb.h"
#include "source/extensions/load_balancing_policies/common/factory_base.h"

namespace Envoy {
namespace Extensions {
namespace LoadBalancingPolicies {
namespace AnchorFlow {

using AnchorFlowLbProto = envoy::extensions::load_balancing_policies::anchorflow::v3::AnchorFlow;

class Factory : public Upstream::TypedLoadBalancerFactoryBase<AnchorFlowLbProto> {
public:
  Factory() : TypedLoadBalancerFactoryBase("envoy.load_balancing_policies.anchorflow") {}

  Upstream::ThreadAwareLoadBalancerPtr create(OptRef<const Upstream::LoadBalancerConfig> lb_config,
                                              const Upstream::ClusterInfo& cluster_info,
                                              const Upstream::PrioritySet& priority_set,
                                              Runtime::Loader& runtime,
                                              Random::RandomGenerator& random,
                                              TimeSource& time_source) override;

  absl::StatusOr<Upstream::LoadBalancerConfigPtr>
  loadConfig(Server::Configuration::ServerFactoryContext& context,
             const Protobuf::Message& config) override {
    ASSERT(dynamic_cast<const AnchorFlowLbProto*>(&config) != nullptr);
    const AnchorFlowLbProto& typed_proto = dynamic_cast<const AnchorFlowLbProto&>(config);
    absl::Status creation_status = absl::OkStatus();
    auto typed_config = std::make_unique<Upstream::TypedAnchorFlowLbConfig>(
        typed_proto, context.regexEngine(), creation_status);
    RETURN_IF_NOT_OK_REF(creation_status);
    return typed_config;
  }
};

} // namespace AnchorFlow
} // namespace LoadBalancingPolicies
} // namespace Extensions
} // namespace Envoy
