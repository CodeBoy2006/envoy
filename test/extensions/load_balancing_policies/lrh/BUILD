load(
    "//bazel:envoy_build_system.bzl",
    "envoy_benchmark_test",
    "envoy_cc_benchmark_binary",
    "envoy_package",
)
load(
    "//test/extensions:extensions_build_system.bzl",
    "envoy_extension_cc_test",
)

licenses(["notice"])  # Apache 2

envoy_package()

envoy_extension_cc_test(
    name = "lrh_lb_test",
    srcs = ["lrh_lb_test.cc"],
    extension_names = ["envoy.load_balancing_policies.lrh"],
    rbe_pool = "6gig",
    deps = [
        "//source/common/upstream:load_balancer_context_base_lib",
        "//source/extensions/load_balancing_policies/lrh:lrh_lb_lib",
        "//test/common/upstream:utility_lib",
        "//test/mocks:common_lib",
        "//test/mocks/server:server_factory_context_mocks",
        "//test/mocks/upstream:cluster_info_mocks",
        "//test/mocks/upstream:host_set_mocks",
        "//test/mocks/upstream:priority_set_mocks",
        "//test/test_common:simulated_time_system_lib",
        "//test/test_common:test_runtime_lib",
        "@envoy_api//envoy/config/cluster/v3:pkg_cc_proto",
    ],
)

envoy_extension_cc_test(
    name = "config_test",
    srcs = ["config_test.cc"],
    extension_names = ["envoy.load_balancing_policies.lrh"],
    rbe_pool = "6gig",
    deps = [
        "//source/extensions/load_balancing_policies/lrh:config",
        "//test/mocks/server:server_factory_context_mocks",
        "//test/mocks/upstream:cluster_info_mocks",
        "//test/mocks/upstream:priority_set_mocks",
        "@envoy_api//envoy/config/core/v3:pkg_cc_proto",
        "@envoy_api//envoy/extensions/load_balancing_policies/lrh/v3:pkg_cc_proto",
    ],
)

envoy_cc_benchmark_binary(
    name = "lrh_lb_benchmark",
    srcs = ["lrh_lb_benchmark.cc"],
    rbe_pool = "6gig",
    deps = [
        "//source/extensions/load_balancing_policies/lrh:lrh_lb_lib",
        "//source/extensions/load_balancing_policies/maglev:maglev_lb_lib",
        "//source/extensions/load_balancing_policies/ring_hash:ring_hash_lb_lib",
        "//test/extensions/load_balancing_policies/common:benchmark_base_tester_lib",
    ],
)

envoy_benchmark_test(
    name = "lrh_lb_benchmark_test",
    timeout = "long",
    benchmark_binary = "lrh_lb_benchmark",
)
