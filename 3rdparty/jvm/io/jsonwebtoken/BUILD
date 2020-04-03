java_library(
    name = "jjwt_api",
    exports = [
        "//external:jar/io/jsonwebtoken/jjwt_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "jjwt_gson",
    exports = [
        "//external:jar/io/jsonwebtoken/jjwt_gson"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/google/code/gson:gson",
        ":jjwt_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "jjwt_impl",
    exports = [
        "//external:jar/io/jsonwebtoken/jjwt_impl"
    ],
    runtime_deps = [
        ":jjwt_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)


