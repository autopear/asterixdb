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

public class MinLatencyMergePolicy implements ILSMMergePolicy {
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
        int depth = 0;
        long numFlushes = index.getHarness().getNumberOfFlushes();
        while ((tree(depth) < numFlushes))
            depth++;
        int mergedIndex = binomial_index(depth, numComponents - 1, (int) (numFlushes - tree(depth - 1) - 1));
        if (mergedIndex == size - 1)
            return false;
        List<ILSMDiskComponent> mergableComponents = new ArrayList<>();
        for (int i = mergedIndex; i < immutableComponents.size(); i++)
            mergableComponents.add(immutableComponents.get(i));
        Collections.reverse(mergableComponents);
        ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        accessor.scheduleMerge(mergableComponents);
        return true;
    }

    private int binomial_index(int d, int h, int t) {
        if (t == 0) {
            return 0;
        } else if (t < binomial_choose(d + h - 1, h)) {
            return binomial_index(d - 1, h, t);
        }
        return binomial_index(d, h - 1, t - binomial_choose(d + h - 1, h)) + 1;
    }

    private int tree(int d) {
        if (d < 0) {
            return 0;
        }
        return binomial_choose(d + numComponents, d) - 1;
    }

    private int binomial_choose(int n, int k) {
        //TODO: This is recomputed every time. Might be better to save values
        if (k < 0 || k > n) {
            return 0;
        }
        if (k == 0 || k == n) {
            return 1;
        } else {
            int bin[][] = new int[n + 1][n + 1];

            for (int r = 0; r <= n; r++) {
                for (int c = 0; c <= r && c <= k; c++) {
                    if (c == 0 || c == r) {
                        bin[r][c] = 1;
                    } else {
                        bin[r][c] = bin[r - 1][c - 1] + bin[r - 1][c];
                    }
                }
            }
            return bin[n][k];
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

    private boolean isMergeOngoing(List<ILSMDiskComponent> immutableComponents) {
        int size = immutableComponents.size();
        for (int i = 0; i < size; i++) {
            if (immutableComponents.get(i).getState() == ComponentState.READABLE_MERGING) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void configure(Map<String, String> properties) {
        numComponents = Integer.parseInt(properties.get(MinLatencyMergePolicyFactory.NUM_COMPONENTS));
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = index.getDiskComponents();
        boolean isMergeOngoing = isMergeOngoing(immutableComponents);
        if (isMergeOngoing) {
            return true;
        }
        return false;
    }
}
