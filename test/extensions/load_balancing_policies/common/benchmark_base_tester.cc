#include "test/extensions/load_balancing_policies/common/benchmark_base_tester.h"

namespace Envoy {
namespace Upstream {

BaseTester::BaseTester(uint64_t num_hosts, uint32_t weighted_subset_percent, uint32_t weight,
                       bool attach_metadata)
    : num_hosts_(num_hosts), attach_metadata_(attach_metadata) {
  updateWeightedHosts(weighted_subset_percent, weight);
}

void BaseTester::updateWeightedHosts(uint32_t weighted_subset_percent, uint32_t weight,
                                     uint64_t weighted_subset_offset, uint32_t unweighted_weight) {
  Upstream::HostVector hosts;
  ASSERT(num_hosts_ < 65536);
  const uint64_t weighted_hosts =
      static_cast<uint64_t>(num_hosts_ * (weighted_subset_percent / 100.0));
  for (uint64_t i = 0; i < num_hosts_; i++) {
    const uint64_t offset_index =
        (i + num_hosts_ - (weighted_subset_offset % num_hosts_)) % num_hosts_;
    const bool should_weight = offset_index < weighted_hosts;
    const std::string url = fmt::format("tcp://10.0.{}.{}:6379", i / 256, i % 256);
    const auto effective_weight = should_weight ? weight : unweighted_weight;
    if (attach_metadata_) {
      envoy::config::core::v3::Metadata metadata;
      Protobuf::Value value;
      value.set_number_value(i);
      Protobuf::Struct& map =
          (*metadata.mutable_filter_metadata())[Config::MetadataFilters::get().ENVOY_LB];
      (*map.mutable_fields())[std::string(metadata_key)] = value;

      hosts.push_back(Upstream::makeTestHost(info_, url, metadata, effective_weight));
    } else {
      hosts.push_back(Upstream::makeTestHost(info_, url, effective_weight));
    }
  }

  Upstream::HostVectorConstSharedPtr updated_hosts = std::make_shared<Upstream::HostVector>(hosts);
  Upstream::HostsPerLocalityConstSharedPtr hosts_per_locality =
      Upstream::makeHostsPerLocality({hosts});
  priority_set_.updateHosts(
      0, Upstream::HostSetImpl::partitionHosts(updated_hosts, hosts_per_locality), {}, hosts, {},
      absl::nullopt);
  local_priority_set_.updateHosts(
      0, Upstream::HostSetImpl::partitionHosts(updated_hosts, hosts_per_locality), {}, hosts, {},
      absl::nullopt);
}

} // namespace Upstream
} // namespace Envoy
