java_library(
    name = "grpc_all",
    exports = [
        "//external:jar/io/grpc/grpc_all"
    ],
    runtime_deps = [
        ":grpc_api",
        ":grpc_auth",
        ":grpc_context",
        ":grpc_core",
        ":grpc_netty",
        ":grpc_okhttp",
        ":grpc_protobuf",
        ":grpc_stub",
        ":grpc_testing"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_api",
    exports = [
        "//external:jar/io/grpc/grpc_api"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/errorprone:error_prone_annotations",
        "//3rdparty/jvm/com/google/guava:guava",
        "//3rdparty/jvm/org/codehaus/mojo:animal_sniffer_annotations",
        ":grpc_context"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_auth",
    exports = [
        "//external:jar/io/grpc/grpc_auth"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/google/auth:google_auth_library_credentials",
        ":grpc_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_context",
    exports = [
        "//external:jar/io/grpc/grpc_context"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_core",
    exports = [
        "//external:jar/io/grpc/grpc_core"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/google/android:annotations",
        "//3rdparty/jvm/com/google/code/gson:gson",
        "//3rdparty/jvm/io/opencensus:opencensus_api",
        "//3rdparty/jvm/io/opencensus:opencensus_contrib_grpc_metrics",
        "//3rdparty/jvm/io/perfmark:perfmark_api",
        ":grpc_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_netty",
    exports = [
        "//external:jar/io/grpc/grpc_netty"
    ],
    runtime_deps = [
        "//3rdparty/jvm/io/netty:netty_codec_http2",
        "//3rdparty/jvm/io/netty:netty_handler_proxy",
        ":grpc_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_okhttp",
    exports = [
        "//external:jar/io/grpc/grpc_okhttp"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/squareup/okhttp:okhttp",
        "//3rdparty/jvm/com/squareup/okio:okio",
        ":grpc_core"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "grpc_protobuf",
    exports = [
        "//external:jar/io/grpc/grpc_protobuf"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/google/api/grpc:proto_google_common_protos",
        "//3rdparty/jvm/com/google/guava:guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        ":grpc_api",
        ":grpc_protobuf_lite"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_protobuf_lite",
    exports = [
        "//external:jar/io/grpc/grpc_protobuf_lite"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/google/guava:guava",
        ":grpc_api"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "grpc_services",
    exports = [
        "//external:jar/io/grpc/grpc_services"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/google/protobuf:protobuf_java_util",
        ":grpc_core",
        ":grpc_protobuf",
        ":grpc_stub"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_stub",
    exports = [
        "//external:jar/io/grpc/grpc_stub"
    ],
    runtime_deps = [
        ":grpc_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_testing",
    exports = [
        "//external:jar/io/grpc/grpc_testing"
    ],
    runtime_deps = [
        "//3rdparty/jvm/junit:junit",
        ":grpc_core",
        ":grpc_stub"
    ],
    visibility = [
        "//visibility:public"
    ]
)


