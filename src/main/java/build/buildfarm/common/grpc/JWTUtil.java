package build.buildfarm.common.grpc;

import io.grpc.Metadata;

public class JWTUtil {
    public static String KEY_ENV_VAR_NAME = "BUILDFARM_JWT_KEY_PATH";
    public static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
    public static final String AUTH_TYPE_PREFIX = "Bearer ";
}
