// Copyright 2017 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.server;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

import build.bazel.remote.execution.v2.DigestFunction;
import build.buildfarm.v1test.*;
import com.google.longrunning.ListOperationsRequest;
import com.google.longrunning.ListOperationsResponse;
import com.google.longrunning.OperationsGrpc;
import com.google.protobuf.util.Durations;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.crypto.SecretKey;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.UUID;

@RunWith(JUnit4.class)
public class AuthenticatedBuildFarmServerTest {
    private final static String INSTANCE_NAME = "memory";

    private BuildFarmServer server;
    private ManagedChannel inProcessChannel;
    private MemoryInstanceConfig memoryInstanceConfig;

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private String authHeaderValue = "";

    @Before
    public void setUp() throws Exception {
        String uniqueServerName = "in-process server for " + getClass();

        SecretKey key = Keys.secretKeyFor(SignatureAlgorithm.HS256);
        final File keyFile = tempDir.newFile("key.dat");
        Files.write(Path.of(keyFile.getAbsolutePath()), key.getEncoded());

        Date now = new Date();
        authHeaderValue = Jwts.builder()
            .setIssuer("me")
            .setSubject("jerry")
            .setAudience("kramer")
            .setExpiration(Date.from(now.toInstant().plusSeconds(3600)))
            .setIssuedAt(now) // for example, now
            .setId(UUID.randomUUID().toString())
            .signWith(key)
            .compact();

        memoryInstanceConfig = MemoryInstanceConfig.newBuilder()
                .setListOperationsDefaultPageSize(1024)
                .setListOperationsMaxPageSize(16384)
                .setTreeDefaultPageSize(1024)
                .setTreeMaxPageSize(16384)
                .setOperationPollTimeout(Durations.fromSeconds(10))
                .setOperationCompletedDelay(Durations.fromSeconds(10))
                .setCasConfig(ContentAddressableStorageConfig.newBuilder()
                        .setMemory(MemoryCASConfig.newBuilder()
                                .setMaxSizeBytes(640 * 1024)))
                .setActionCacheConfig(ActionCacheConfig.newBuilder()
                        .setDelegateCas(DelegateCASConfig.getDefaultInstance())
                        .build())
                .setDefaultActionTimeout(Durations.fromSeconds(600))
                .setMaximumActionTimeout(Durations.fromSeconds(3600))
                .build();

        BuildFarmServerConfig.Builder configBuilder =
                BuildFarmServerConfig.newBuilder()
                        .setPort(0);

        configBuilder.addInstancesBuilder()
                .setName(INSTANCE_NAME)
                .setDigestFunction(DigestFunction.Value.SHA256)
                .setMemoryInstanceConfig(memoryInstanceConfig);

        server = new BuildFarmServer(
                "test",
                InProcessServerBuilder.forName(uniqueServerName).directExecutor(),
                configBuilder.build()) {
            @Nullable
            @Override
            protected String getAuthKeyFilename() {
                return keyFile.getAbsolutePath();
            }
        };
        server.start();
        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName)
                .directExecutor().build();
    }

    @After
    public void tearDown() throws Exception {
        inProcessChannel.shutdownNow();
        server.stop();
    }

    @Test
    public void listOperations() {
        ListOperationsRequest request = ListOperationsRequest.newBuilder()
                .setName(INSTANCE_NAME + "/operations")
                .setPageSize(1024)
                .build();

        OperationsGrpc.OperationsBlockingStub stub1 =
                OperationsGrpc.newBlockingStub(inProcessChannel);

        try {
            ListOperationsResponse response1 = stub1.listOperations(request);
            fail("should have thrown");
        } catch (Exception ex) {
            assertThat(ex.getMessage()).containsMatch("UNAUTHENTICATED");
        }

        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER), String.format("Bearer %s", authHeaderValue));
        ClientInterceptor interceptor = MetadataUtils.newAttachHeadersInterceptor(headers);
        Channel authenticatedChannel = ClientInterceptors.intercept(inProcessChannel, interceptor);

        OperationsGrpc.OperationsBlockingStub stub2 =
                OperationsGrpc.newBlockingStub(authenticatedChannel);

        ListOperationsResponse response2 = stub2.listOperations(request);

        assertThat(response2.getOperationsList()).isEmpty();
    }
}
