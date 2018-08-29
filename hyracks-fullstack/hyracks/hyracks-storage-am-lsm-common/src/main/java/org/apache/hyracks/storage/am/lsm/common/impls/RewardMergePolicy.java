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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.*;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;

public class RewardMergePolicy implements ILSMMergePolicy {

    double discount;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException {
        return;
    }

    @Override
    public void configure(Map<String, String> properties) {
        discount = Double.parseDouble(properties.get(RewardMergePolicyFactory.DISCOUNT));
        if (discount > 1.0)
            discount = 1.0;
        if (discount < 0.0)
            discount = 0.001;
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = index.getDiskComponents();
        List<ILSMDiskComponent> mergableComponents = getMergableComponents(index);

        if (mergableComponents == null || mergableComponents.size() < 2)
            return false;

        boolean isMergeOngoing = isMergeOngoing(immutableComponents);
        if (isMergeOngoing) {
            return true;
        } else {
            if (!areComponentsMergable(mergableComponents)) {
                throw new IllegalStateException();
            }
            ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            accessor.scheduleMerge(mergableComponents);
            return true;
        }
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

    private List<ILSMDiskComponent> randomMerge(List<ILSMDiskComponent> immutableComponents) {
        if (new UniformIntegerDistribution(1, 1000).sample() > 333)
            return null;

        int s = immutableComponents.size();

        // No merge
        if (s < 2)
            return null;

        int max = s > 10 ? 10 : s;

        int r = (max == 2) ? 2 : (new UniformIntegerDistribution(2, max).sample());

        int start = (s == r) ? 0 : new UniformIntegerDistribution(0, s - r).sample();

        List<ILSMDiskComponent> mergableComponents = new ArrayList<>();
        for (int i = 0; i < r; i++) {
            mergableComponents.add(immutableComponents.get(start + i));
        }
        return mergableComponents;
    }

    private List<ILSMDiskComponent> getMergableComponents(ILSMIndex index) {
        if (index.getHarness() == null)
            return null;
        ILSMOperationHistory history = index.getHarness().getOperationHistory();
        if (history == null)
            return null;
        List<ILSMDiskComponent> diskComponents = index.getDiskComponents();

        double mergeSlope = history.getMergeSlope();
        if (Double.isNaN(mergeSlope))
            return randomMerge(diskComponents);

        double normalSearchSlope = history.getNormalSearchSlope();
        if (Double.isNaN(normalSearchSlope))
            return randomMerge(diskComponents);
        double mergeSearchSlope = history.getMergeSearchSlope();
        if (Double.isNaN(mergeSearchSlope))
            return randomMerge(diskComponents);

        int start = 0;
        int end = 0;
        double bestReward = 0.0;

        int total = diskComponents.size();
        for (int i = 0; i < total - 1; i++) {
            // i: first component
            for (int j = i + 1; j < total; j++) {
                // j: last component
                long mergeSize = 0;
                for (int k = i; k <= j; k++)
                    mergeSize += (long) Math.ceil((double) diskComponents.get(k).getComponentSize() / 1048576.0);
                double mergeTime = Math.ceil(mergeSlope * mergeSize);

                double reward =
                        Math.pow(discount, mergeTime) * (normalSearchSlope * (j - i)
                                + discount * (mergeSearchSlope - normalSearchSlope) * total)
                        - (mergeSearchSlope - normalSearchSlope) * total;

                if (reward > bestReward) {
                    start = i;
                    end = j;
                    bestReward = reward;
                }
            }
        }

        if (bestReward > 0.0) {
            List<ILSMDiskComponent> components = new ArrayList<>();
            for (int i = start; i <= end; i++)
                components.add(diskComponents.get(i));
            return components;
        }

        return null;
    }
}
