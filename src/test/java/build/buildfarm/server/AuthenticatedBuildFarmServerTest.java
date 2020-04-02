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

import static build.bazel.remote.execution.v2.ExecutionStage.Value.COMPLETED;
import static build.bazel.remote.execution.v2.ExecutionStage.Value.EXECUTING;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionCacheGrpc;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest.Request;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse.Response;
import build.bazel.remote.execution.v2.BatchReadBlobsRequest;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteRequest;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionGrpc;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.GetActionResultRequest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.StaticAuthInterceptor;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.server.BuildFarmServer;
import build.buildfarm.v1test.*;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.SettableFuture;
import com.google.longrunning.CancelOperationRequest;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.ListOperationsRequest;
import com.google.longrunning.ListOperationsResponse;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsGrpc;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.Test;

@RunWith(JUnit4.class)
public class AuthenticatedBuildFarmServerTest {
    private final static String INSTANCE_NAME = "memory";
    private final static String AUTH_TOKEN = "abcdef123456";

    private BuildFarmServer server;
    private ManagedChannel inProcessChannel;
    private MemoryInstanceConfig memoryInstanceConfig;

    @Before
    public void setUp() throws Exception {
        String uniqueServerName = "in-process server for " + getClass();

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
                        .setPort(0)
                        .setAuth(AuthConfig.newBuilder()
                                .setStatic(StaticAuthConfig.newBuilder()
                                        .addTokens(AUTH_TOKEN).build()));
        configBuilder.addInstancesBuilder()
                .setName(INSTANCE_NAME)
                .setDigestFunction(DigestFunction.Value.SHA256)
                .setMemoryInstanceConfig(memoryInstanceConfig);

        server = new BuildFarmServer(
                "test",
                InProcessServerBuilder.forName(uniqueServerName).directExecutor(),
                configBuilder.build());
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
        headers.put(StaticAuthInterceptor.AUTHORIZATION_METADATA_KEY, String.format("%s%s", StaticAuthInterceptor.AUTH_TYPE_PREFIX, AUTH_TOKEN));
        ClientInterceptor interceptor = MetadataUtils.newAttachHeadersInterceptor(headers);
        Channel authenticatedChannel = ClientInterceptors.intercept(inProcessChannel, interceptor);

        OperationsGrpc.OperationsBlockingStub stub2 =
                OperationsGrpc.newBlockingStub(authenticatedChannel);

        ListOperationsResponse response2 = stub2.listOperations(request);

        assertThat(response2.getOperationsList()).isEmpty();
    }
}
