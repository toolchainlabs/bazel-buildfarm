package build.buildfarm.common.grpc;

import io.grpc.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Authenticate gRPC requests by requiring one of a statically-configured set of tokens to be present in
 * the Authorization header.
 *
 * Good example of how to do server header interception:
 * https://github.com/grpc/grpc-java/tree/master/examples/example-jwt-auth/src/main/java/io/grpc/examples/jwtauth
 */
public class StaticAuthInterceptor implements ServerInterceptor {
    public static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    public static final String AUTH_TYPE_PREFIX = "Bearer ";

    private final Set<String> tokensSet;

    public StaticAuthInterceptor(Collection<String> tokens) {
        this.tokensSet = new HashSet<>();
        this.tokensSet.addAll(tokens);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String authHeader = headers.get(AUTHORIZATION_METADATA_KEY);
        if (authHeader != null) {
            if (authHeader.startsWith(AUTH_TYPE_PREFIX)) {
                String token = authHeader.substring(AUTH_TYPE_PREFIX.length());
                if (tokensSet.contains(token)) {
                    return next.startCall(call, headers);
                }
            }
        }

        call.close(Status.UNAUTHENTICATED, new Metadata());
        return new ServerCall.Listener<ReqT>() {
        };
    }
}
