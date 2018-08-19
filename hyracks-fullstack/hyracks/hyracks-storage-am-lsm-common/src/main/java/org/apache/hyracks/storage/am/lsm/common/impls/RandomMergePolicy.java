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
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class RandomMergePolicy implements ILSMMergePolicy {
    private int minComponents;
    private int maxComponents;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException {
        return;
    }

    @Override
    public void configure(Map<String, String> properties) {
        minComponents = Integer.parseInt(properties.get(RandomMergePolicyFactory.MIN_COMPONENTS));
        maxComponents = Integer.parseInt(properties.get(RandomMergePolicyFactory.MAX_COMPONENTS));
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = index.getDiskComponents();
        List<ILSMDiskComponent> mergableComponents = getMergableComponents(immutableComponents);

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

    protected List<ILSMDiskComponent> getMergableComponents(List<ILSMDiskComponent> immutableComponents) {
        // No merge
        if ((maxComponents > 1 && minComponents > maxComponents) || (minComponents == 0 && maxComponents == 0))
            return null;

        // Full merge
        if (minComponents == 1 && maxComponents == 1)
            return immutableComponents;

        int s = immutableComponents.size();

        // No merge
        if (s < 2 || minComponents > s)
            return null;

        int min = minComponents > 0 ? minComponents : 1;
        int max = (maxComponents < 2 || maxComponents >= s) ? s : maxComponents;

        // No merge
        if (min > max)
            return null;

        int r = (min == max) ? min : new Random(System.nanoTime()).nextInt(max - min + 1) + min;

        // No merge
        if (r < 2)
            return null;

        int start = (s == r) ? 0 : new Random(System.nanoTime()).nextInt(s - r + 1);

        List<ILSMDiskComponent> mergableComponents = new ArrayList<>();
        for (int i = 0; i < r; i++) {
            mergableComponents.add(immutableComponents.get(start + i));
        }
        return mergableComponents;
    }
}
