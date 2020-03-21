// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import static build.buildfarm.common.Actions.checkPreconditionFailure;
import static build.buildfarm.common.Actions.satisfiesRequirements;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.OutputFile;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.Tree;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.cas.ContentAddressableStorage.EntryLimitException;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.Poller;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.common.Write;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.worker.ExecutionPolicies;
import build.buildfarm.worker.RetryingMatchListener;
import build.buildfarm.worker.WorkerContext;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.CASInsertionPolicy;
import build.buildfarm.v1test.ExecutionPolicy;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.PreconditionFailure;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Logger;

class ShardWorkerContext implements WorkerContext {
  private static final Logger logger = Logger.getLogger(ShardWorkerContext.class.getName());

  private final String name;
  private final Platform platform;
  private final Duration operationPollPeriod;
  private final OperationPoller operationPoller;
  private final int inlineContentLimit;
  private final int inputFetchStageWidth;
  private final int executeStageWidth;
  private final ShardBackplane backplane;
  private final ExecFileSystem execFileSystem;
  private final InputStreamFactory inputStreamFactory;
  private final Map<String, ExecutionPolicy> policies;
  private final Instance instance;
  private final long deadlineAfter;
  private final TimeUnit deadlineAfterUnits;
  private final Duration defaultActionTimeout;
  private final Duration maximumActionTimeout;
  private final Map<String, QueueEntry> activeOperations = Maps.newConcurrentMap();

  ShardWorkerContext(
      String name,
      Platform platform,
      Duration operationPollPeriod,
      OperationPoller operationPoller,
      int inlineContentLimit,
      int inputFetchStageWidth,
      int executeStageWidth,
      ShardBackplane backplane,
      ExecFileSystem execFileSystem,
      InputStreamFactory inputStreamFactory,
      Iterable<ExecutionPolicy> policies,
      Instance instance,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      Duration defaultActionTimeout,
      Duration maximumActionTimeout) {
    this.name = name;
    this.operationPollPeriod = operationPollPeriod;
    this.operationPoller = operationPoller;
    this.inlineContentLimit = inlineContentLimit;
    this.inputFetchStageWidth = inputFetchStageWidth;
    this.executeStageWidth = executeStageWidth;
    this.backplane = backplane;
    this.execFileSystem = execFileSystem;
    this.inputStreamFactory = inputStreamFactory;
    this.policies = uniqueIndex(policies, (policy) -> policy.getName());
    this.instance = instance;
    this.deadlineAfter = deadlineAfter;
    this.deadlineAfterUnits = deadlineAfterUnits;
    this.defaultActionTimeout = defaultActionTimeout;
    this.maximumActionTimeout = maximumActionTimeout;

    this.platform =  ExecutionPolicies.adjustPlatformProperties(platform, policies);
    logger.fine(String.format("%s will match against platform %s", this, platform));
  }

  private static Retrier createBackplaneRetrier() {
    return new Retrier(
        Backoff.exponential(
              java.time.Duration.ofMillis(/*options.experimentalRemoteRetryStartDelayMillis=*/ 100),
              java.time.Duration.ofMillis(/*options.experimentalRemoteRetryMaxDelayMillis=*/ 5000),
              /*options.experimentalRemoteRetryMultiplier=*/ 2,
              /*options.experimentalRemoteRetryJitter=*/ 0.1,
              /*options.experimentalRemoteRetryMaxAttempts=*/ 5),
          Retrier.REDIS_IS_RETRIABLE);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Poller createPoller(String name, QueueEntry queueEntry, ExecutionStage.Value stage) {
    Poller poller = new Poller(operationPollPeriod);
    resumePoller(poller, name, queueEntry, stage, () -> {}, Deadline.after(10, DAYS));
    return poller;
  }

  @Override
  public void resumePoller(
      Poller poller,
      String name,
      QueueEntry queueEntry,
      ExecutionStage.Value stage,
      Runnable onFailure,
      Deadline deadline) {
    String operationName = queueEntry.getExecuteEntry().getOperationName();
    poller.resume(
        () -> {
          boolean success = false;
          try {
            success = operationPoller.poll(queueEntry, stage, System.currentTimeMillis() + 30 * 1000);
          } catch (IOException e) {
            logger.log(SEVERE, format("%s: poller: error while polling %s", name, operationName), e);
          }

          logger.info(format("%s: poller: Completed Poll for %s: %s", name, operationName, success ? "OK" : "Failed"));
          if (!success) {
            onFailure.run();
          }
          return success;
        },
        () -> {
          logger.info(format("%s: poller: Deadline expired for %s", name, operationName));
          onFailure.run();
        },
        deadline);
  }

  @Override
  public DigestUtil getDigestUtil() {
    return instance.getDigestUtil();
  }

  private ByteString getBlob(Digest digest) throws IOException, InterruptedException {
    try (InputStream in = inputStreamFactory.newInput(digest, 0)) {
      return ByteString.readFrom(in);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().equals(Status.NOT_FOUND)) {
        return null;
      }
      throw e;
    }
  }

  @Override
  public QueuedOperation getQueuedOperation(QueueEntry queueEntry)
      throws IOException, InterruptedException {
    Digest queuedOperationDigest = queueEntry.getQueuedOperationDigest();
    ByteString queuedOperationBlob = getBlob(queuedOperationDigest);
    if (queuedOperationBlob == null) {
      return null;
    }
    try {
      return QueuedOperation.parseFrom(queuedOperationBlob);
    } catch (InvalidProtocolBufferException e) {
      logger.warning(
          format(
              "invalid queued operation: %s(%s)",
              queueEntry.getExecuteEntry().getOperationName(),
              DigestUtil.toString(queuedOperationDigest)));
      return null;
    }
  }

  private void matchInterruptible(Platform platform, MatchListener listener)
      throws IOException, InterruptedException {
    listener.onWaitStart();
    QueueEntry queueEntry = null;
    try {
      queueEntry = backplane.dispatchOperation();
    } catch (IOException e) {
      Status status = Status.fromThrowable(e);
      if (status.getCode() != Code.UNAVAILABLE) {
        throw e;
      }
      // unavailable backplane will propagate a null queueEntry
    }
    listener.onWaitEnd();
    if (queueEntry == null || satisfiesRequirements(platform, queueEntry.getPlatform())) {
      listener.onEntry(queueEntry);
    } else {
      backplane.rejectOperation(queueEntry);
    }
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  @Override
  public void match(MatchListener listener) throws InterruptedException {
    RetryingMatchListener dedupMatchListener = new RetryingMatchListener() {
      boolean matched = false;

      @Override
      public boolean getMatched() {
        return matched;
      }

      @Override
      public void onWaitStart() {
        listener.onWaitStart();
      }

      @Override
      public void onWaitEnd() {
        listener.onWaitEnd();
      }

      @Override
      public boolean onEntry(QueueEntry queueEntry) throws InterruptedException {
        if (queueEntry == null) {
          matched = true;
          return listener.onEntry(null);
        }
        String operationName = queueEntry.getExecuteEntry().getOperationName();
        if (activeOperations.putIfAbsent(operationName, queueEntry) != null) {
          logger.warning("matched duplicate operation " + operationName);
          return false;
        }
        matched = true;
        boolean success = listener.onEntry(queueEntry);
        if (!success) {
          requeue(operationName);
        }
        return success;
      }

      @Override
      public void onError(Throwable t) {
        Throwables.throwIfUnchecked(t);
        throw new RuntimeException(t);
      }

      @Override
      public void setOnCancelHandler(Runnable onCancelHandler) {
        listener.setOnCancelHandler(onCancelHandler);
      }
    };
    while (!dedupMatchListener.getMatched()) {
      try {
        matchInterruptible(platform, dedupMatchListener);
      } catch (IOException e) {
        throw Status.fromThrowable(e).asRuntimeException();
      }
    }
  }

  private ExecuteOperationMetadata expectExecuteOperationMetadata(Operation operation) {
    Any metadata = operation.getMetadata();
    if (metadata == null) {
      return null;
    }

    if (metadata.is(QueuedOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(QueuedOperationMetadata.class).getExecuteOperationMetadata();
      } catch(InvalidProtocolBufferException e) {
        logger.log(SEVERE, "invalid operation metadata: " + operation.getName(), e);
        return null;
      }
    }

    if (metadata.is(ExecuteOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(ExecuteOperationMetadata.class);
      } catch(InvalidProtocolBufferException e) {
        logger.log(SEVERE, "invalid operation metadata: " + operation.getName(), e);
        return null;
      }
    }

    return null;
  }

  private void requeue(String operationName) {
    QueueEntry queueEntry = activeOperations.remove(operationName);
    try {
      operationPoller.poll(queueEntry, ExecutionStage.Value.QUEUED, 0);
    } catch (IOException e) {
      // ignore, at least dispatcher will pick us up in 30s
      logger.log(SEVERE, "Failure while trying to fast requeue " + operationName, e);
    }
  }

  @Override
  public void requeue(Operation operation) {
    requeue(operation.getName());
  }

  @Override
  public void deactivate(String operationName) {
    activeOperations.remove(operationName);
  }

  @Override
  public void logInfo(String msg) {
    logger.info(msg);
  }

  @Override
  public CASInsertionPolicy getFileCasPolicy() {
    return CASInsertionPolicy.ALWAYS_INSERT;
  }

  @Override
  public CASInsertionPolicy getStdoutCasPolicy() {
    return CASInsertionPolicy.ALWAYS_INSERT;
  }

  @Override
  public CASInsertionPolicy getStderrCasPolicy() {
    return CASInsertionPolicy.ALWAYS_INSERT;
  }

  @Override
  public int getInputFetchStageWidth() {
    return inputFetchStageWidth;
  }

  @Override
  public int getExecuteStageWidth() {
    return executeStageWidth;
  }

  @Override
  public boolean hasDefaultActionTimeout() {
    return defaultActionTimeout.getSeconds() > 0 || defaultActionTimeout.getNanos() > 0;
  }

  @Override
  public boolean hasMaximumActionTimeout() {
    return maximumActionTimeout.getSeconds() > 0 || maximumActionTimeout.getNanos() > 0;
  }

  @Override
  public boolean getStreamStdout() {
    return true;
  }

  @Override
  public boolean getStreamStderr() {
    return true;
  }

  @Override
  public Duration getDefaultActionTimeout() {
    return defaultActionTimeout;
  }

  @Override
  public Duration getMaximumActionTimeout() {
    return maximumActionTimeout;
  }

  private void insertBlob(Digest digest, ByteString content) throws InterruptedException {
    if (digest.getSizeBytes() > 0) {
      Blob blob = new Blob(content, digest);
      execFileSystem.getStorage().put(blob);
    }
  }

  private void insertFile(Digest digest, Path file) throws IOException, InterruptedException {
    AtomicBoolean complete = new AtomicBoolean(false);
    Write write = execFileSystem.getStorage().getWrite(digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
    write.addListener(() -> complete.set(true), directExecutor());
    try (OutputStream out = write.getOutput(deadlineAfter, deadlineAfterUnits, () -> {})) {
      try (InputStream in = Files.newInputStream(file)) {
        com.google.common.base.Preconditions.checkNotNull(in);
        ByteStreams.copy(in, out);
      }
    } catch (IOException e) {
      // complete writes should be ignored
      if (!complete.get()) {
        write.reset(); // we will not attempt retry with current behavior, abandon progress
        if (e.getCause() != null) {
          Throwables.propagateIfInstanceOf(e.getCause(), InterruptedException.class);
        }
        throw e;
      }
    }
  }

  private void updateActionResultStdOutputs(ActionResult.Builder resultBuilder) throws InterruptedException {
    ByteString stdoutRaw = resultBuilder.getStdoutRaw();
    if (stdoutRaw.size() > 0) {
      // reset to allow policy to determine inlining
      resultBuilder.setStdoutRaw(ByteString.EMPTY);
      Digest stdoutDigest = getDigestUtil().compute(stdoutRaw);
      insertBlob(stdoutDigest, stdoutRaw);
      resultBuilder.setStdoutDigest(stdoutDigest);
    }

    ByteString stderrRaw = resultBuilder.getStderrRaw();
    if (stderrRaw.size() > 0) {
      // reset to allow policy to determine inlining
      resultBuilder.setStderrRaw(ByteString.EMPTY);
      Digest stderrDigest = getDigestUtil().compute(stderrRaw);
      insertBlob(stderrDigest, stderrRaw);
      resultBuilder.setStderrDigest(stderrDigest);
    }
  }

  @Override
  public void uploadOutputs(
      Digest actionDigest,
      ActionResult.Builder resultBuilder,
      Path actionRoot,
      Iterable<String> outputFiles,
      Iterable<String> outputDirs)
      throws IOException, InterruptedException, StatusException {
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    for (String outputFile : outputFiles) {
      Path outputPath = actionRoot.resolve(outputFile);
      if (!Files.exists(outputPath)) {
        logInfo("ReportResultStage: " + outputFile + " does not exist...");
        continue;
      }

      if (Files.isDirectory(outputPath)) {
        logInfo("ReportResultStage: " + outputFile + " is a directory");
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(outputFile)
            .setDescription("An output file was a directory");
        continue;
      }

      // will run into issues if we end up blocking on the cache insertion, might
      // want to decrement input references *before* this to ensure that we cannot
      // cause an internal deadlock

      Digest digest;
      try {
        digest = getDigestUtil().compute(outputPath);
      } catch (NoSuchFileException e) {
        continue;
      }

      OutputFile.Builder outputFileBuilder = resultBuilder.addOutputFilesBuilder()
          .setPath(outputFile)
          .setDigest(digest)
          .setIsExecutable(Files.isExecutable(outputPath));
      try {
        insertFile(digest, outputPath);
      } catch (EntryLimitException e) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_MISSING)
            .setSubject("blobs/" + DigestUtil.toString(digest))
            .setDescription("An output could not be uploaded because it exceeded the maximum size of an entry");
      }
    }

    for (String outputDir : outputDirs) {
      Path outputDirPath = actionRoot.resolve(outputDir);
      if (!Files.exists(outputDirPath)) {
        logInfo("ReportResultStage: " + outputDir + " does not exist...");
        continue;
      }

      if (!Files.isDirectory(outputDirPath)) {
        logInfo("ReportResultStage: " + outputDir + " is not a directory...");
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(outputDir)
            .setDescription("An output directory was not a directory");
        continue;
      }

      Tree.Builder treeBuilder = Tree.newBuilder();
      Directory.Builder outputRoot = treeBuilder.getRootBuilder();
      Files.walkFileTree(outputDirPath, new SimpleFileVisitor<Path>() {
        Directory.Builder currentDirectory = null;
        Stack<Directory.Builder> path = new Stack<>();

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Digest digest;
          try {
            digest = getDigestUtil().compute(file);
          } catch (NoSuchFileException e) {
            logger.log(SEVERE, format("error visiting file %s under output dir %s", outputDirPath.relativize(file), outputDirPath.toAbsolutePath()), e);
            return FileVisitResult.CONTINUE;
          }

          // should we cast to PosixFilePermissions and do gymnastics there for executable?

          // TODO symlink per revision proposal
          FileNode.Builder fileNodeBuilder = currentDirectory.addFilesBuilder()
              .setName(file.getFileName().toString())
              .setDigest(digest)
              .setIsExecutable(Files.isExecutable(file));
          try {
            insertFile(digest, file);
          } catch (InterruptedException e) {
            throw new IOException(e);
          } catch (EntryLimitException e) {
            preconditionFailure.addViolationsBuilder()
                .setType(VIOLATION_TYPE_MISSING)
                .setSubject("blobs/" + DigestUtil.toString(digest))
                .setDescription("An output could not be uploaded because it exceeded the maximum size of an entry");
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
          path.push(currentDirectory);
          if (dir.equals(outputDirPath)) {
            currentDirectory = outputRoot;
          } else {
            currentDirectory = treeBuilder.addChildrenBuilder();
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Directory.Builder parentDirectory = path.pop();
          if (parentDirectory != null) {
            parentDirectory.addDirectoriesBuilder()
                .setName(dir.getFileName().toString())
                .setDigest(getDigestUtil().compute(currentDirectory.build()));
          }
          currentDirectory = parentDirectory;
          return FileVisitResult.CONTINUE;
        }
      });
      Tree tree = treeBuilder.build();
      ByteString treeBlob = tree.toByteString();
      Digest treeDigest = getDigestUtil().compute(treeBlob);
      insertBlob(treeDigest, treeBlob);
      resultBuilder.addOutputDirectoriesBuilder()
          .setPath(outputDir)
          .setTreeDigest(treeDigest);
    }
    checkPreconditionFailure(actionDigest, preconditionFailure.build());

    /* put together our outputs and update the result */
    updateActionResultStdOutputs(resultBuilder);
  }

  private void logComplete(String operationName) {
    logger.info("CompletedOperation: " + operationName);
  }

  @Override
  public ExecutionPolicy getExecutionPolicy(String name) {
    return policies.get(name);
  }

  @Override
  public boolean putOperation(Operation operation, Action action) throws IOException, InterruptedException {
    boolean success = createBackplaneRetrier().execute(() -> instance.putOperation(operation));
    if (success && operation.getDone()) {
      logComplete(operation.getName());
    }
    return success;
  }

  private Map<Digest, Directory> createDirectoriesIndex(
      Iterable<Directory> directories) {
    Set<Digest> directoryDigests = Sets.newHashSet();
    ImmutableMap.Builder<Digest, Directory> directoriesIndex = new ImmutableMap.Builder<>();
    for (Directory directory : directories) {
      // double compute here...
      Digest directoryDigest = getDigestUtil().compute(directory);
      if (!directoryDigests.add(directoryDigest)) {
        continue;
      }
      directoriesIndex.put(directoryDigest, directory);
    }

    return directoriesIndex.build();
  }

  @Override
  public Path createExecDir(String operationName, Tree tree, Action action, Command command) throws IOException, InterruptedException {
    return execFileSystem.createExecDir(
        operationName,
        getDigestUtil().createDirectoriesIndex(tree),
        action,
        command);
  }

  // might want to split for removeDirectory and decrement references to avoid removing for streamed output
  @Override
  public void destroyExecDir(Path execDir) throws IOException, InterruptedException {
    execFileSystem.destroyExecDir(execDir);
  }

  @Override
  public void blacklistAction(String actionId)
      throws IOException, InterruptedException {
    createBackplaneRetrier().execute(() -> {
      backplane.blacklistAction(actionId);
      return null;
    });
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult)
      throws IOException, InterruptedException {
    createBackplaneRetrier().execute(() -> {
      instance.putActionResult(actionKey, actionResult);
      return null;
    });
  }

  @Override
  public Write getOperationStreamWrite(String name) {
    throw new UnsupportedOperationException();
  }
}
