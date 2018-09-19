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
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.common.IIndexAccessParameters;

public class FixedMergePolicy implements ILSMMergePolicy {
    private int numComponents;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested, boolean isMergeOperation)
            throws HyracksDataException {
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
            List<ILSMDiskComponent> componentsToBeMerged = getMergableComponents(immutableComponents);
            if (componentsToBeMerged != null) {
                ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                accessor.scheduleMerge(componentsToBeMerged);
            }
        }
    }

    @Override
    public void configure(Map<String, String> properties) {
        numComponents = Integer.parseInt(properties.get(FixedMergePolicyFactory.NUM_COMPONENTS));
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

    private List<ILSMDiskComponent> getMergableComponents(List<ILSMDiskComponent> immutableComponents) {
        int l = immutableComponents.size();
        if (l < 2 || l <= numComponents)
            return null;

        int numToMerge = l - numComponents + 1;

        long total = 0;
        int start = 0;

        for (int i = 0; i < numToMerge; i++)
            total += immutableComponents.get(i).getComponentSize();

        for (int i = 1; i <= l - numToMerge; i++) {
            long sum = 0;
            for (int j = 0; j < numToMerge; j++)
                sum += immutableComponents.get(i + j).getComponentSize();
            if (sum < total) {
                total = sum;
                start = i;
            }
        }

        List<ILSMDiskComponent> componentsToBeMerged = new ArrayList<>();
        for (int i = 0; i < numToMerge; i++)
            componentsToBeMerged.add(immutableComponents.get(start + i));
        return componentsToBeMerged;
    }
}
