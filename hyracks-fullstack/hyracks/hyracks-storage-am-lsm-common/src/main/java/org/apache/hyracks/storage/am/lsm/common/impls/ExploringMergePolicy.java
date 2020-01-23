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

public class ExploringMergePolicy extends StackMergePolicy {
    private double ratio;
    private int min;
    private int max;

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

    @Override
    public List<ILSMDiskComponent> getMergableComponents(List<ILSMDiskComponent> components) {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(components);
        Collections.reverse(immutableComponents);
        int length = immutableComponents.size();
        if (length <= min - 1) {
            return Collections.emptyList();
        }
        boolean mightBeStuck = (length > max) ? true : false;
        List<ILSMDiskComponent> bestSelection = new ArrayList<>(0);
        List<ILSMDiskComponent> smallest = new ArrayList<>(0);
        List<ILSMDiskComponent> mergableComponents = new ArrayList<>();
        long bestSize = 0;
        long smallestSize = Long.MAX_VALUE;
        int bestStart = -1;
        int bestEnd = -1;
        int smallestStart = -1;
        int smallestEnd = -1;
        boolean merging = false;

        for (int start = 0; start < length; start++) {
            for (int currentEnd = start + min - 1; currentEnd < length; currentEnd++) {
                List<ILSMDiskComponent> potentialMatchFiles = immutableComponents.subList(start, currentEnd + 1);
                if (potentialMatchFiles.size() < min) {
                    continue;
                }
                long size = getTotalSize(potentialMatchFiles);
                if (mightBeStuck && size < smallestSize) {
                    smallest = potentialMatchFiles;
                    smallestSize = size;
                    smallestStart = start;
                    smallestEnd = currentEnd;
                }
                if (!fileInRatio(potentialMatchFiles)) {
                    continue;
                }
                if (isBetterSelection(bestSelection, bestSize, potentialMatchFiles, size, mightBeStuck)) {
                    bestSelection = potentialMatchFiles;
                    bestSize = size;
                    bestStart = start;
                    bestEnd = currentEnd;
                }
            }
        }
        if (bestSelection.size() == 0 && mightBeStuck && smallestStart != -1 && smallestEnd != -1) {
            merging = true;
            mergableComponents = new ArrayList<>(smallest);
        } else if (bestStart != -1 && bestEnd != -1) {
            merging = true;
            mergableComponents = new ArrayList<>(bestSelection);

        }
        if (merging) {
            Collections.reverse(mergableComponents);
            return mergableComponents;
        }
        return Collections.emptyList();
    }

    public boolean isBetterSelection(List<ILSMDiskComponent> bestSelection, long bestSize,
            List<ILSMDiskComponent> selection, long size, boolean mightBeStuck) {
        if (mightBeStuck && bestSize > 0 && size > 0) {
            double thresholdQuantity = ((double) bestSelection.size() / bestSize);
            return thresholdQuantity < ((double) selection.size() / size);
        }
        return selection.size() > bestSelection.size() || (selection.size() == bestSelection.size() && size < bestSize);
    }

    public boolean fileInRatio(final List<ILSMDiskComponent> files) {
        if (files.size() < 2) {
            return true;
        }
        long totalFileSize = getTotalSize(files);
        for (ILSMDiskComponent file : files) {
            long singleFileSize = file.getComponentSize();
            long sumAllOtherFileSizes = totalFileSize - singleFileSize;
            if (singleFileSize > sumAllOtherFileSizes * ratio) {
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

    private long getTotalSize(List<ILSMDiskComponent> files) {
        long sum = 0;
        for (int i = 0; i < files.size(); i++) {
            sum = sum + files.get(i).getComponentSize();
        }
        return sum;
    }

    @Override
    public void configure(Map<String, String> properties) {
        this.properties = StringUtils.join(properties).replaceAll("\n", " ");
        while (this.properties.contains("  ")) {
            this.properties = this.properties.replaceAll("  ", " ");
        }
        ratio = Double.parseDouble(properties.get(ExploringMergePolicyFactory.LAMBDA));
        min = Integer.parseInt(properties.get(ExploringMergePolicyFactory.MIN_COMPONENTS));
        max = Integer.parseInt(properties.get(ExploringMergePolicyFactory.MAX_COMPONENTS));
    }
}