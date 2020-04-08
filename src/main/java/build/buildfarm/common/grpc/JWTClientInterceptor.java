package build.buildfarm.common.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;

// Adapted from https://github.com/grpc/grpc-java/blob/master/examples/example-jwt-auth/src/main/java/io/grpc/examples/jwtauth/JwtCredential.java

/**
 * A ClientInterceptor to add JWT credentials to outgoing gRPC requests. This is used to authenticate the worker
 * to the Buildfarm server.
 */
public class JWTClientInterceptor implements ClientInterceptor {
    private final byte[] key;

    JWTClientInterceptor(byte[] key) {
        this.key = key;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(method, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
                // Make a JWT compact serialized string.
                Date now = new Date();
                final String jwt =
                        Jwts.builder()
                                .setIssuer("worker")
                                .setSubject("worker")
                                .setExpiration(Date.from(now.toInstant().plusSeconds(600)))
                                .setIssuedAt(now)
                                .signWith(SignatureAlgorithm.HS256, key)
                                .compact();

                headers.put(JWTUtil.AUTHORIZATION_METADATA_KEY, String.format("%s %s", JWTUtil.AUTH_TYPE_PREFIX, jwt));
                super.start(responseListener, headers);
            }
        };
    }


    // Return an instance of this class if authentication is enabled.
    public static ClientInterceptor instance() {
        String keyPath = System.getenv(JWTUtil.KEY_ENV_VAR_NAME);
        if (keyPath == null) {
            return null;
        }

        byte[] key;
        try {
            key = Files.readAllBytes(Path.of(keyPath));
        } catch (IOException ex) {
            throw new RuntimeException(String.format("Unable to read JWT key from %s", keyPath), ex);
        }

        return new JWTClientInterceptor(key);
    }
}
