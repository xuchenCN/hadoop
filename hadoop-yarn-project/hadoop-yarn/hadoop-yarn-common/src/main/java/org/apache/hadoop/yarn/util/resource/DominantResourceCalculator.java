/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.yarn.util.resource;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Arrays;

/**
 * A {@link ResourceCalculator} which uses the concept of  
 * <em>dominant resource</em> to compare multi-dimensional resources.
 *
 * Essentially the idea is that the in a multi-resource environment, 
 * the resource allocation should be determined by the dominant share 
 * of an entity (user or queue), which is the maximum share that the 
 * entity has been allocated of any resource. 
 * 
 * In a nutshell, it seeks to maximize the minimum dominant share across 
 * all entities. 
 * 
 * For example, if user A runs CPU-heavy tasks and user B runs
 * memory-heavy tasks, it attempts to equalize CPU share of user A 
 * with Memory-share of user B. 
 * 
 * In the single resource case, it reduces to max-min fairness for that resource.
 * 
 * See the Dominant Resource Fairness paper for more details:
 * www.cs.berkeley.edu/~matei/papers/2011/nsdi_drf.pdf
 */
@Private
@Unstable
public class DominantResourceCalculator extends ResourceCalculator {
  
  @Override
  public int compare(Resource clusterResource, Resource lhs, Resource rhs) {
    
    if (lhs.equals(rhs)) {
      return 0;
    }

    for (int i = 1; i <= 3; i ++) {
      float l = getResourceAsValue(clusterResource, lhs, i);
      float r = getResourceAsValue(clusterResource, rhs, i);

      if (l < r) {
        return -1;
      } else if (l > r) {
        return 1;
      }
    }
    return 0;
  }

  /**
   * Get the resource value according to the rank. Lower the rank, more
   * dominant the resource. Rank = 1 means the most dominant resource value.
   */
  protected float getResourceAsValue(
      Resource clusterResource, Resource resource, int rank) {
    if (rank < 1 || rank > 3) {
      throw new IllegalArgumentException("The rank " + rank
          + " is not in range [1,3].");
    }
    float[] values = new float[] {
        (float) resource.getMemory() / clusterResource.getMemory(),
        (float) resource.getVirtualCores() / clusterResource.getVirtualCores(),
        (float) resource.getVirtualDisks() / clusterResource.getVirtualDisks()};
    Arrays.sort(values);
    return values[values.length - rank];
  }
  
  @Override
  public int computeAvailableContainers(Resource available, Resource required) {
    int min = Integer.MAX_VALUE;
    if (required.getMemory() != 0) {
      min = Math.min(min,
          available.getMemory() / required.getMemory());
    }
    if (required.getVirtualCores() != 0) {
      min = Math.min(min,
          available.getVirtualCores() / required.getVirtualCores());
    }
    if (required.getVirtualDisks() != 0) {
      min = Math.min(min,
          available.getVirtualDisks() / required.getVirtualDisks());
    }
    return min;
  }

  @Override
  public float divide(Resource clusterResource, 
      Resource numerator, Resource denominator) {
    return getResourceAsValue(clusterResource, numerator, 1) /
        getResourceAsValue(clusterResource, denominator, 1);
  }
  
  @Override
  public boolean isInvalidDivisor(Resource r) {
    if (r.getMemory() == 0.0f || r.getVirtualCores() == 0.0f) {
      return true;
    }
    return false;
  }

  @Override
  public float ratio(Resource a, Resource b) {
    float max = 0.0f;
    if (b.getMemory() != 0) {
      max = Math.max(max,
          (float) a.getMemory() / b.getMemory());
    }
    if (b.getVirtualCores() != 0) {
      max = Math.max(max,
          (float) a.getVirtualCores() / b.getVirtualCores());
    }
    if (b.getVirtualDisks() != 0) {
      max = Math.max(max,
          (float) a.getVirtualDisks() / b.getVirtualDisks());
      }
    return max;
  }

  @Override
  public Resource divideAndCeil(Resource numerator, int denominator) {
    return Resources.createResource(
        divideAndCeil(numerator.getMemory(), denominator),
        divideAndCeil(numerator.getVirtualCores(), denominator),
        divideAndCeil(numerator.getVirtualDisks(), denominator)
        );
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource,
                            Resource maximumResource, Resource stepFactor) {
    int normalizedMemory = Math.min(
      roundUp(
        Math.max(r.getMemory(), minimumResource.getMemory()),
        stepFactor.getMemory()),
      maximumResource.getMemory());
    int normalizedCores = Math.min(
      roundUp(
        Math.max(r.getVirtualCores(), minimumResource.getVirtualCores()),
        stepFactor.getVirtualCores()),
      maximumResource.getVirtualCores());
    int normalizedVdisks = Math.min(
      roundUp(
        Math.max(r.getVirtualDisks(), minimumResource.getVirtualDisks()),
        stepFactor.getVirtualDisks()),
      maximumResource.getVirtualDisks());

    return Resources.createResource(normalizedMemory,
        normalizedCores, normalizedVdisks);
  }

  @Override
  public Resource roundUp(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundUp(r.getMemory(), stepFactor.getMemory()), 
        roundUp(r.getVirtualCores(), stepFactor.getVirtualCores()),
        roundUp(r.getVirtualDisks(), stepFactor.getVirtualDisks())
        );
  }

  @Override
  public Resource roundDown(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundDown(r.getMemory(), stepFactor.getMemory()),
        roundDown(r.getVirtualCores(), stepFactor.getVirtualCores()),
        roundDown(r.getVirtualDisks(), stepFactor.getVirtualDisks())
        );
  }

  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundUp(
            (int)Math.ceil(r.getMemory() * by), stepFactor.getMemory()),
        roundUp(
            (int)Math.ceil(r.getVirtualCores() * by), 
            stepFactor.getVirtualCores()),
        roundDown(
            (int)Math.ceil(r.getVirtualDisks() * by),
            stepFactor.getVirtualDisks())
        );
  }

  @Override
  public Resource multiplyAndNormalizeDown(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundDown(
            (int)(r.getMemory() * by), 
            stepFactor.getMemory()
            ),
        roundDown(
            (int)(r.getVirtualCores() * by), 
            stepFactor.getVirtualCores()
            ),
        roundDown(
            (int)(r.getVirtualDisks() * by),
            stepFactor.getVirtualDisks()
            )
        );
  }

}
