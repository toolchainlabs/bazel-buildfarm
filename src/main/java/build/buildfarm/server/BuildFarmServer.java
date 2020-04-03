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

import static build.buildfarm.common.IOUtils.formatIOError;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.SEVERE;

import build.buildfarm.common.grpc.StaticAuthInterceptor;
import build.buildfarm.common.grpc.TracingMetadataUtils.ServerHeadersInterceptor;
import build.buildfarm.v1test.BuildFarmServerConfig;
import com.google.common.io.ByteStreams;
import com.google.devtools.common.options.OptionsParser;
import com.google.protobuf.TextFormat;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.services.HealthStatusManager;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;

public class BuildFarmServer {
  // We need to keep references to the root and netty loggers to prevent them from being garbage
  // collected, which would cause us to loose their configuration.
  private static final Logger nettyLogger = Logger.getLogger("io.grpc.netty");
  public static final Logger logger =
    Logger.getLogger(BuildFarmServer.class.getName());

  private final ScheduledExecutorService keepaliveScheduler = newSingleThreadScheduledExecutor();
  private final ActionCacheRequestCounter actionCacheRequestCounter;
  private final Instances instances;
  private final HealthStatusManager healthStatusManager;
  private final Server server;
  private boolean stopping = false;

  public BuildFarmServer(String session, BuildFarmServerConfig config)
      throws InterruptedException, ConfigurationException {
    this(session, ServerBuilder.forPort(config.getPort()), config);
  }

  public BuildFarmServer(String session, ServerBuilder<?> serverBuilder, BuildFarmServerConfig config)
      throws InterruptedException, ConfigurationException {
    String defaultInstanceName = config.getDefaultInstanceName();
    instances = new BuildFarmInstances(session, config.getInstancesList(), defaultInstanceName, this::stop);

    healthStatusManager = new HealthStatusManager();
    actionCacheRequestCounter = new ActionCacheRequestCounter(ActionCacheService.logger, Duration.ofSeconds(10));

    Collection<String> tokens = Collections.emptyList();
    if (config.getAuth().hasStatic()) {
      tokens = config.getAuth().getStatic().getTokensList();
    }

    ServerInterceptor headersInterceptor = new ServerHeadersInterceptor();

    serverBuilder
        .addService(healthStatusManager.getHealthService())
        .addService(new ActionCacheService(instances, actionCacheRequestCounter::increment))
        .addService(new CapabilitiesService(instances))
        .addService(new ContentAddressableStorageService(
            instances,
            /* deadlineAfter=*/ 1, TimeUnit.DAYS,
            /* requestLogLevel=*/ FINE))
        .addService(new ByteStreamService(instances, /* writeDeadlineAfter=*/ 1, TimeUnit.DAYS))
        .addService(new ExecutionService(
            instances,
            config.getExecuteKeepaliveAfterSeconds(),
            TimeUnit.SECONDS,
            keepaliveScheduler))
        .addService(new OperationQueueService(instances))
        .addService(new OperationsService(instances));

    if (!tokens.isEmpty()) {
        serverBuilder.intercept(new StaticAuthInterceptor(tokens));
    }

    serverBuilder
        .intercept(TransmitStatusRuntimeExceptionInterceptor.instance())
        .intercept(headersInterceptor);

    server = serverBuilder.build();

    logger.info(String.format("%s initialized", session));
  }

  private static BuildFarmServerConfig toBuildFarmServerConfig(Readable input, BuildFarmServerOptions options) throws IOException {
    BuildFarmServerConfig.Builder builder = BuildFarmServerConfig.newBuilder();
    TextFormat.merge(input, builder);
    if (options.port > 0) {
        builder.setPort(options.port);
    }
    return builder.build();
  }

  public void start() throws IOException {
    actionCacheRequestCounter.start();
    instances.start();
    server.start();
    healthStatusManager.setStatus(HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        logger.info("*** shutting down gRPC server since JVM is shutting down");
        BuildFarmServer.this.stop();
        logger.info("*** server shut down");
      }
    });
  }

  public void stop() {
    synchronized (this) {
      if (stopping) {
        return;
      }
      stopping = true;
    }
    healthStatusManager.setStatus(HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.NOT_SERVING);
    try {
      if (server != null) {
        server.shutdown();
      }
      instances.stop();
      server.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      if (server != null) {
        server.shutdownNow();
      }
    }
    if (!shutdownAndAwaitTermination(keepaliveScheduler, 10, TimeUnit.SECONDS)) {
      logger.warning("could not shut down keepalive scheduler");
    }
    if (!actionCacheRequestCounter.stop()) {
      logger.warning("count not shut down action cache request counter");
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  private static void printUsage(OptionsParser parser) {
    System.out.println("Usage: CONFIG_PATH");
    System.out.println(parser.describeOptions(Collections.<String, String>emptyMap(),
                                              OptionsParser.HelpVerbosity.LONG));
  }

  /**
   * returns success or failure
   */
  static boolean serverMain(String[] args) {
    // Only log severe log messages from Netty. Otherwise it logs warnings that look like this:
    //
    // 170714 08:16:28.552:WT 18 [io.grpc.netty.NettyServerHandler.onStreamError] Stream Error
    // io.netty.handler.codec.http2.Http2Exception$StreamException: Received DATA frame for an
    // unknown stream 11369
    nettyLogger.setLevel(SEVERE);

    OptionsParser parser = OptionsParser.newOptionsParser(BuildFarmServerOptions.class);
    parser.parseAndExitUponError(args);
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      printUsage(parser);
      return false;
    }

    Path configPath = Paths.get(residue.get(0));
    BuildFarmServerOptions options = parser.getOptions(BuildFarmServerOptions.class);

    String session = "buildfarm-server";
    if (!options.publicName.isEmpty()) {
      session += "-" + options.publicName;
    }
    session += "-" + UUID.randomUUID();
    BuildFarmServer server;
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      server = new BuildFarmServer(session, toBuildFarmServerConfig(new InputStreamReader(configInputStream), options));
      configInputStream.close();
      server.start();
      server.blockUntilShutdown();
      server.stop();
      return true;
    } catch (IOException e) {
      System.err.println("error: " + formatIOError(e));
    } catch (ConfigurationException e) {
      System.err.println("error: " + e.getMessage());
    } catch (InterruptedException e) {
      System.err.println("error: interrupted");
    }
    return false;
  }

  public static void main(String[] args) {
    System.exit(serverMain(args) ? 0 : 1);
  }
}
