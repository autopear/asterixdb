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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class GoogleDefaultMergePolicy implements ILSMMergePolicy {
    private int numComponents;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        if (!areComponentsReadableWritableState(immutableComponents)) {
            return;
        }
        if (fullMergeIsRequested) {
            ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            accessor.scheduleFullMerge();
        } else
            scheduleMerge(index);
    }

    private boolean scheduleMerge(final ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        Collections.reverse(immutableComponents);
        int size = immutableComponents.size();
        if (size <= numComponents) {
            return false;
        }
        long sum = getTotalSize(immutableComponents);
        int endIndex = size - 2;
        int mergedIndex = endIndex;

        for (int i = 0; i < endIndex; i++) {
            if (immutableComponents.get(i).getComponentSize() <= sum - immutableComponents.get(i).getComponentSize()) {
                mergedIndex = i;
                break;
            }
            sum = sum - immutableComponents.get(i).getComponentSize();
        }
        List<ILSMDiskComponent> mergableComponents = new ArrayList<ILSMDiskComponent>();
        for (int i = mergedIndex; i < immutableComponents.size(); i++) {
            mergableComponents.add(immutableComponents.get(i));
        }
        Collections.reverse(mergableComponents);
        ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        accessor.scheduleMerge(mergableComponents);
        return true;
    }

    private boolean areComponentsReadableWritableState(List<ILSMDiskComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
        return true;
    }

    private boolean isMergeOngoing(List<ILSMDiskComponent> immutableComponents) {
        int size = immutableComponents.size();
        for (int i = 0; i < size; i++) {
            if (immutableComponents.get(i).getState() == ComponentState.READABLE_MERGING) {
                return true;
            }
        }
        return false;
    }

    private long getTotalSize(List<ILSMDiskComponent> immutableComponents) {
        long sum = 0;
        for (int i = 0; i < immutableComponents.size(); i++) {
            sum = sum + immutableComponents.get(i).getComponentSize();
        }
        return sum;
    }

    @Override
    public void configure(Map<String, String> properties) {
        numComponents = Integer.parseInt(properties.get(GoogleDefaultMergePolicyFactory.NUM_COMPONENTS));
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = index.getDiskComponents();
        int size = immutableComponents.size();
        if (size <= numComponents) {
            return false;
        }
        boolean isMergeOngoing = isMergeOngoing(immutableComponents);
        if (isMergeOngoing) {
            return true;
        }
        if (!areComponentsReadableWritableState(immutableComponents)) {
            throw new IllegalStateException();
        }
        boolean isMergeTriggered = scheduleMerge(index);
        if (!isMergeTriggered) {
            throw new IllegalStateException();
        }
        return true;
    }
}