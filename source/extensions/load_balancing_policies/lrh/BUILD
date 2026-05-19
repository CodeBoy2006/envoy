load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_extension",
    "envoy_cc_library",
    "envoy_extension_package",
)

licenses(["notice"])  # Apache 2

envoy_extension_package()

envoy_cc_library(
    name = "lrh_lb_lib",
    srcs = ["lrh_lb.cc"],
    hdrs = ["lrh_lb.h"],
    deps = [
        "//envoy/upstream:load_balancer_interface",
        "//source/common/common:hash_lib",
        "//source/common/common:minimal_logger_lib",
        "//source/extensions/load_balancing_policies/common:thread_aware_lb_lib",
        "@abseil-cpp//absl/container:inlined_vector",
        "@envoy_api//envoy/config/cluster/v3:pkg_cc_proto",
        "@envoy_api//envoy/extensions/load_balancing_policies/lrh/v3:pkg_cc_proto",
    ],
)

envoy_cc_extension(
    name = "config",
    srcs = ["config.cc"],
    hdrs = ["config.h"],
    extra_visibility = [
        "//test:__subpackages__",
    ],
    deps = [
        ":lrh_lb_lib",
        "//source/common/common:minimal_logger_lib",
        "//source/common/upstream:load_balancer_context_base_lib",
        "//source/common/upstream:load_balancer_factory_base_lib",
        "//source/extensions/load_balancing_policies/common:factory_base",
        "@envoy_api//envoy/extensions/load_balancing_policies/lrh/v3:pkg_cc_proto",
    ],
)
