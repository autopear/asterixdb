/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.hyracks.storage.common.IIndexAccessParameters;

public class RandomMergePolicy implements ILSMMergePolicy {
    public enum Distribution {
        Binomial,
        Latest,
        Uniform,
        Zipf
    }

    private float mergeProbability;
    private int minComponents;
    private int maxComponents;
    private Distribution dist;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = index.getDiskComponents();

        if (!areComponentsMergable(immutableComponents)) {
            return;
        }

        if (fullMergeIsRequested) {
            IIndexAccessParameters iap =
                    new IndexAccessParameters(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            ILSMIndexAccessor accessor = index.createAccessor(iap);
            accessor.scheduleFullMerge();
        } else {
            List<ILSMDiskComponent> mergableComponents = getMergableComponents(immutableComponents);
            if (mergableComponents != null && mergableComponents.size() > 1) {
                if (!areComponentsMergable(mergableComponents)) {
                    throw new IllegalStateException();
                }
                ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                accessor.scheduleMerge(mergableComponents);
            }
        }
    }

    @Override
    public void configure(Map<String, String> properties) {
        mergeProbability = Float.parseFloat(properties.get(RandomMergePolicyFactory.MERGE_PROBABILITY));
        mergeProbability = Math.round(mergeProbability * 1000.0f) / 1000.0f;
        if (mergeProbability <= 0.0f)
            mergeProbability = 0.0f;
        if (mergeProbability >= 1.0f)
            mergeProbability = 1.0f;
        minComponents = Integer.parseInt(properties.get(RandomMergePolicyFactory.MIN_COMPONENTS));
        maxComponents = Integer.parseInt(properties.get(RandomMergePolicyFactory.MAX_COMPONENTS));
        String distStr = properties.get(RandomMergePolicyFactory.DISTRIBUTION).toLowerCase();
        if (distStr.compareTo("binomial") == 0)
            dist = Distribution.Binomial;
        else if (distStr.compareTo("zipf") == 0)
            dist = Distribution.Zipf;
        else if (distStr.compareTo("latest") == 0)
            dist = Distribution.Latest;
        else
            dist = Distribution.Uniform;
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = index.getDiskComponents();
        return isMergeOngoing(immutableComponents);
    }

    /**
     * checks whether all given components are mergable or not
     *
     * @param immutableComponents
     * @return true if all components are mergable, false otherwise.
     */
    private boolean areComponentsMergable(List<ILSMDiskComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
        return true;
    }

    /**
     * This method returns whether there is an ongoing merge operation or not by checking
     * each component state of given components.
     *
     * @return true if there is an ongoing merge operation, false otherwise.
     */
    private boolean isMergeOngoing(List<ILSMDiskComponent> immutableComponents) {
        int size = immutableComponents.size();
        for (int i = 0; i < size; i++) {
            if (immutableComponents.get(i).getState() == ComponentState.READABLE_MERGING) {
                return true;
            }
        }
        return false;
    }

    protected Boolean shouldMerge() {
        if (mergeProbability >= 1.0f)
            return true;
        if (mergeProbability <= 0.0f)
            return false;
        int intProb = (int) (mergeProbability * 1000);
        int r = new Random(System.nanoTime()).nextInt(1000) + 1;
        return r <= intProb;
    }

    private int generateRandomStart(int bound) {
        switch (dist) {
            case Zipf:
                return bound - new ZipfDistribution(bound + 1, 0.99).sample() + 1;
            case Latest:
                return new ZipfDistribution(bound + 1, 0.99).sample() - 1;
            case Binomial:
                return new BinomialDistribution(bound, 0.5).sample();
            case Uniform:
            default: {
                return new UniformIntegerDistribution(0, bound).sample();
            }
        }
    }

    private List<ILSMDiskComponent> getMergableComponents(List<ILSMDiskComponent> immutableComponents) {
        // No merge
        if (!shouldMerge() || (maxComponents > 1 && minComponents > maxComponents)
                || (minComponents == 0 && maxComponents == 0))
            return null;

        // Full merge
        if (minComponents == 1 && maxComponents == 1)
            return immutableComponents;

        int s = immutableComponents.size();

        // No merge
        if (s < 2 || minComponents > s)
            return null;

        int min = minComponents > 1 ? minComponents : 2;
        int max = (maxComponents < 2 || maxComponents >= s) ? s : maxComponents;

        // No merge
        if (min > max)
            return null;

        int r = (min == max) ? min : (new UniformIntegerDistribution(min, max).sample());

        int start = (s == r) ? 0 : generateRandomStart(s - r);

        List<ILSMDiskComponent> mergableComponents = new ArrayList<>();
        for (int i = 0; i < r; i++) {
            mergableComponents.add(immutableComponents.get(start + i));
        }
        return mergableComponents;
    }
}
