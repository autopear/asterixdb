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

public class FixedMergePolicy extends StackMergePolicy {
    private int numComponents;

    @Override
    public void diskComponentAdded(ILSMIndex index, List<ILSMDiskComponent> newComponents, boolean fullMergeIsRequested,
            boolean wasMerge) throws HyracksDataException {
        if (!wasMerge) {
            List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
            if (!areComponentsReadableWritableState(immutableComponents)) {
                return;
            }
            if (fullMergeIsRequested) {
                ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                accessor.scheduleFullMerge();
                return;
            }
            List<ILSMDiskComponent> mergableComponents = getMergableComponents(index.getDiskComponents());
            if (mergableComponents.isEmpty()) {
                return;
            }
            ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            accessor.scheduleMerge(mergableComponents);
        }
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
        numComponents = Integer.parseInt(properties.get(FixedMergePolicyFactory.NUM_COMPONENTS));
    }

    @Override
    public List<ILSMDiskComponent> getMergableComponents(List<ILSMDiskComponent> components) {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(components);
        int l = immutableComponents.size();
        if (l < 2 || l <= numComponents) {
            return Collections.emptyList();
        }

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
