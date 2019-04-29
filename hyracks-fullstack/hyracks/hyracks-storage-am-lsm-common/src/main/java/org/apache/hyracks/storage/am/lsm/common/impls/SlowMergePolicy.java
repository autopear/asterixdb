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
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class SlowMergePolicy implements ILSMMergePolicy {
    private int minComponents;
    private int minDelay;
    private int maxDelay;
    private long interval = 0;
    private long lastMerge = 0;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested, boolean wasMerge)
            throws HyracksDataException {
        if (wasMerge) {
            lastMerge = System.nanoTime();
            interval = ThreadLocalRandom.current().nextInt(minDelay, maxDelay + 1) * 1000000;
        } else {
            if (interval == 0 || System.nanoTime() - lastMerge >= interval) {
                List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
                if (!areComponentsReadableWritableState(immutableComponents)) {
                    return;
                }
                if (fullMergeIsRequested) {
                    ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                    accessor.scheduleFullMerge();
                    return;
                }
                scheduleMerge(index);
            }
        }
    }

    private boolean scheduleMerge(final ILSMIndex index) throws HyracksDataException {
        Optional<Long> latestSeq = ((AbstractLSMIndex) index).getLatestDiskComponentSequence();
        if (!latestSeq.isPresent()) {
            return false;
        }
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        List<ILSMDiskComponent> mergableComponents = getMergableComponents(immutableComponents);
        if (mergableComponents != null && mergableComponents.size() > 1) {
            if (!areComponentsMergable(mergableComponents)) {
                throw new IllegalStateException();
            }
            ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            accessor.scheduleMerge(mergableComponents);
            return true;
        }
        return false;
    }

    private List<ILSMDiskComponent> getMergableComponents(List<ILSMDiskComponent> immutableComponents) {
        int l = immutableComponents.size();
        if (l < 2 || l < minComponents)
            return null;
        int numToMerge = ThreadLocalRandom.current().nextInt(minComponents, l + 1);
        List<ILSMDiskComponent> componentsToBeMerged = new ArrayList<>();
        for (int i = 0; i < numToMerge; i++)
            componentsToBeMerged.add(immutableComponents.get(i));
        return componentsToBeMerged;
    }

    private boolean areComponentsMergable(List<ILSMDiskComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
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

    @Override
    public void configure(Map<String, String> properties) {
        minComponents = Integer.parseInt(properties.get(SlowMergePolicyFactory.MIN_COMPONENTS));
        minDelay = Integer.parseInt(properties.get(SlowMergePolicyFactory.MIN_DELAY));
        maxDelay = Integer.parseInt(properties.get(SlowMergePolicyFactory.MAX_DELAY));
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        // TODO: for now, we simply block the ingestion when there is an ongoing merge
        List<ILSMDiskComponent> immutableComponents = index.getDiskComponents();
        return isMergeOngoing(immutableComponents);
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
}
