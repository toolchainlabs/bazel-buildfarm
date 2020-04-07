package build.buildfarm.server;

import java.util.logging.Logger;

import build.buildfarm.common.grpc.JWTUtil;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;

/**
 * Authenticate gRPC requests by requiring one of a statically-configured set of tokens to be present in
 * the Authorization header.
 *
 * Good example of how to do server header interception:
 * https://github.com/grpc/grpc-java/tree/master/examples/example-jwt-auth/src/main/java/io/grpc/examples/jwtauth
 */
public class JWTAuthHeaderInterceptor implements ServerInterceptor {
    private static final Logger logger = Logger.getLogger(JWTAuthHeaderInterceptor.class.getName());

    private final JwtParser parser;

    public JWTAuthHeaderInterceptor(byte[] key) {
        parser = Jwts.parserBuilder().setSigningKey(key).build();
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String authHeader = headers.get(JWTUtil.AUTHORIZATION_METADATA_KEY);
        if (authHeader != null) {
            if (authHeader.startsWith(JWTUtil.AUTH_TYPE_PREFIX)) {
                String token = authHeader.substring(JWTUtil.AUTH_TYPE_PREFIX.length());
                try {
                    Jws<Claims> claims = parser.parseClaimsJws(token);
                    return next.startCall(call, headers);
                } catch (JwtException ex) {
                    logger.warning(String.format("Authentication error: %s", ex.getMessage()));
                    // fall-through to code to return unauthenticated status
                }
            }
        }

        call.close(Status.UNAUTHENTICATED, new Metadata());
        return new ServerCall.Listener<ReqT>() {
        };
    }
}
