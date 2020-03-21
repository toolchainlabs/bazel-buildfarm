// Copyright 2020 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.v1test.ExecutionPolicy;

public final class ExecutionPolicies {
  private ExecutionPolicies() {
    // Utility
  }

  /**
   * Adds execution policy platform psuedo-properties advertising the platform supports any of its
   * configured execution policies.
   *
   * @param platform The platform to adjust with execution-policy pseudo-properties.
   * @param policies The execution policies to add to the platform's properties.
   * @return An adjusted platform containing the execution-policy pseudo-properties.
   */
  public static Platform adjustPlatformProperties(Platform platform, Iterable<ExecutionPolicy> policies) {
    Platform.Builder platformWithPoliciesBuilder = platform.toBuilder();
    for (ExecutionPolicy policy : policies) {
      platformWithPoliciesBuilder.addPropertiesBuilder()
          .setName("execution-policy")
          .setValue(policy.getName());
    }
    return platformWithPoliciesBuilder.build();
  }
}
