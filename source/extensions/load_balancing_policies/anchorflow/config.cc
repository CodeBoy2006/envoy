#include "source/extensions/load_balancing_policies/anchorflow/config.h"

#include "source/extensions/load_balancing_policies/anchorflow/anchorflow_lb.h"

namespace Envoy {
namespace Extensions {
namespace LoadBalancingPolicies {
namespace AnchorFlow {

Upstream::ThreadAwareLoadBalancerPtr
Factory::create(OptRef<const Upstream::LoadBalancerConfig> lb_config,
                const Upstream::ClusterInfo& cluster_info,
                const Upstream::PrioritySet& priority_set, Runtime::Loader& runtime,
                Random::RandomGenerator& random, TimeSource&) {

  const auto typed_lb_config =
      dynamic_cast<const Upstream::TypedAnchorFlowLbConfig*>(lb_config.ptr());
  ASSERT(typed_lb_config != nullptr, "Invalid AnchorFlow load balancer config");

  return std::make_unique<Upstream::AnchorFlowLoadBalancer>(
      priority_set, cluster_info.lbStats(), cluster_info.statsScope(), runtime, random,
      static_cast<uint32_t>(PROTOBUF_PERCENT_TO_ROUNDED_INTEGER_OR_DEFAULT(
          cluster_info.lbConfig(), healthy_panic_threshold, 100, 50)),
      typed_lb_config->lb_config_, typed_lb_config->hash_policy_);
}

REGISTER_FACTORY(Factory, Upstream::TypedLoadBalancerFactory);

} // namespace AnchorFlow
} // namespace LoadBalancingPolicies
} // namespace Extensions
} // namespace Envoy
