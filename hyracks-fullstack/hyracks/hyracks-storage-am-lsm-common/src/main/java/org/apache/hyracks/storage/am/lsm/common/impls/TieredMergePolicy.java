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
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public class TieredMergePolicy extends StackMergePolicy {
    private int numComponents;
    private double highBucket;

    @Override
    public void diskComponentAdded(ILSMIndex index, List<ILSMDiskComponent> newComponents, boolean fullMergeIsRequested,
            boolean wasMerge) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        if (!areComponentsReadableWritableState(immutableComponents)) {
            return;
        }
        List<ILSMDiskComponent> mergableComponents = getMergableComponents(index.getDiskComponents());
        if (mergableComponents.isEmpty()) {
            return;
        }
        ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        accessor.scheduleMerge(mergableComponents);
    }

    @Override
    public List<ILSMDiskComponent> getMergableComponents(List<ILSMDiskComponent> components) {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(components);
        if (components == null || immutableComponents.isEmpty()) {
            return Collections.emptyList();
        }
        long memTableSize = (long) Math.ceil(immutableComponents.get(0).getLsmIndex().memTableSize * highBucket);
        List<List<ILSMDiskComponent>> tiers = new ArrayList<>();
        for (ILSMDiskComponent c : immutableComponents) {
            int t = getTier(c);
            int tierSize = tiers.size();
            if (t > tierSize) {
                if (t - tierSize > 1) {
                    for (int i = tierSize; i < t - 1; i++) {
                        tiers.add(Collections.emptyList());
                    }
                }
                List<ILSMDiskComponent> tierComponents = new ArrayList<>();
                tierComponents.add(c);
                tiers.add(tierComponents);
            } else {
                tiers.get(tiers.size() - 1).add(c);
            }
        }
        for (int t = tiers.size() - 1; t >= 0; t--) {
            List<ILSMDiskComponent> tierComponents = tiers.get(t);
            long nextTierSize = Math.round(memTableSize * Math.pow(numComponents, t + 1));
            if (tierComponents.size() >= numComponents) {
                for (int l = tierComponents.size(); l >= numComponents; l--) {
                    long total = getComponentsSize(tierComponents, 0, l - 1);
                    if (total <= nextTierSize) {
                        return tierComponents.subList(0, l);
                    }
                }
            }
        }
        return Collections.emptyList();
    }

    private long getComponentsSize(List<ILSMDiskComponent> components, int start, int end) {
        long s = 0L;
        for (int i = start; i <= end; i++) {
            s += components.get(i).getComponentSize();
        }
        return s;
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
        this.properties = StringUtils.join(properties).replaceAll("\n", " ");
        while (this.properties.contains("  ")) {
            this.properties = this.properties.replaceAll("  ", " ");
        }
        numComponents = Integer.parseInt(properties.get(TieredMergePolicyFactory.NUM_COMPONENTS));
        highBucket = Double.parseDouble(properties.get(TieredMergePolicyFactory.HIGH_BUCKET));
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

    private int getTier(ILSMDiskComponent component) {
        long memTableSize = (long) Math.ceil(component.getLsmIndex().memTableSize * highBucket);
        long numMemTables = (long) (Math.ceil((double) component.getComponentSize() / memTableSize));
        return (int) Math.ceil((Math.log(numMemTables) / Math.log(numComponents))) + 1;
    }
}
