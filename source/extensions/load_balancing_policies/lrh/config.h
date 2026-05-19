#pragma once

#include <memory>

#include "envoy/extensions/load_balancing_policies/lrh/v3/lrh.pb.h"
#include "envoy/extensions/load_balancing_policies/lrh/v3/lrh.pb.validate.h"
#include "envoy/upstream/load_balancer.h"

#include "source/common/upstream/load_balancer_factory_base.h"
#include "source/extensions/load_balancing_policies/common/factory_base.h"
#include "source/extensions/load_balancing_policies/lrh/lrh_lb.h"

namespace Envoy {
namespace Extensions {
namespace LoadBalancingPolicies {
namespace Lrh {

using LrhLbProto = envoy::extensions::load_balancing_policies::lrh::v3::LocalRendezvousHashing;

class Factory : public Upstream::TypedLoadBalancerFactoryBase<LrhLbProto> {
public:
  Factory() : TypedLoadBalancerFactoryBase("envoy.load_balancing_policies.lrh") {}

  Upstream::ThreadAwareLoadBalancerPtr create(OptRef<const Upstream::LoadBalancerConfig> lb_config,
                                              const Upstream::ClusterInfo& cluster_info,
                                              const Upstream::PrioritySet& priority_set,
                                              Runtime::Loader& runtime,
                                              Random::RandomGenerator& random,
                                              TimeSource& time_source) override;

  absl::StatusOr<Upstream::LoadBalancerConfigPtr>
  loadConfig(Server::Configuration::ServerFactoryContext& context,
             const Protobuf::Message& config) override {
    ASSERT(dynamic_cast<const LrhLbProto*>(&config) != nullptr);
    const LrhLbProto& typed_proto = dynamic_cast<const LrhLbProto&>(config);
    absl::Status creation_status = absl::OkStatus();
    auto typed_config = std::make_unique<Upstream::TypedLrhLbConfig>(
        typed_proto, context.regexEngine(), creation_status);
    RETURN_IF_NOT_OK_REF(creation_status);
    return typed_config;
  }
};

} // namespace Lrh
} // namespace LoadBalancingPolicies
} // namespace Extensions
} // namespace Envoy
