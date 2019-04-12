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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class SizeTieredMergePolicy implements ILSMMergePolicy {
    private double bucket_low;
    private double bucket_high;
    private int threshold_min;
    private int threshold_max;
    private long min_sstable_size;

    //    private int getLastComponentToMerge() {
    //        stack.add(0, new Long(1));
    //        boolean canMerge = true;
    //        int lastIdx = stack.size() - 1;
    //        int maxLastIdx = -1;
    //        while (canMerge) {
    //            long victim = 0;
    //            int checked = 0;
    //            boolean merged = false;
    //            for (int i = 0; i < stack.size(); i++) {
    //                long c = stack.get(i).longValue();
    //                if (c == victim)
    //                    checked++;
    //                else {
    //                    victim = c;
    //                    checked = 1;
    //                }
    //                if (checked == threshold) {
    //                    int start = i - threshold + 1;
    //                    int end = i;
    //                    int currentLastIdx = stack.size() - 1 - end;
    //                    if (maxLastIdx == -1 || currentLastIdx < maxLastIdx)
    //                        maxLastIdx = currentLastIdx;
    //                    long sum = 0;
    //                    for (int j = start; j <= end; j++) {
    //                        sum += stack.get(start).longValue();
    //                        stack.remove(start);
    //                    }
    //                    stack.add(start, new Long(sum));
    //                    merged = true;
    //                    break;
    //                }
    //            }
    //            if (!merged)
    //                canMerge = false;
    //        }
    //        if (maxLastIdx > -1)
    //            return lastIdx - maxLastIdx;
    //        else
    //            return -1;
    //    }

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested, boolean wasMerge)
            throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        if (!areComponentsReadableWritableState(immutableComponents)) {
            return;
        }
        scheduleMerge(index);
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
        int length = immutableComponents.size();
        List<ILSMDiskComponent> mergableComponents = new ArrayList<>();
        for (int start = 0; start <= length - threshold_min; start++) {
            int max_end = start + threshold_max;
            if (max_end > length)
                max_end = length;
            for (int end = max_end - 1; end >= start + threshold_min - 1; end--) {
                boolean all_small = true;
                double total = 0;
                mergableComponents.clear();
                for (int i = start; i <= end; i++) {
                    ILSMDiskComponent c = immutableComponents.get(i);
                    mergableComponents.add(c);
                    long size = c.getComponentSize();
                    total += size;
                    if (size >= min_sstable_size)
                        all_small = false;
                }
                if (all_small)
                    return mergableComponents;
                double avg_size = total / (end - start + 1);
                boolean is_bucket = true;
                for (ILSMDiskComponent c : mergableComponents) {
                    double size = (double) c.getComponentSize();
                    if (size < avg_size * bucket_low || size > avg_size * bucket_high) {
                        is_bucket = false;
                        break;
                    }
                }
                if (is_bucket)
                    return mergableComponents;
            }
        }
        return null;
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
        bucket_low = Double.parseDouble(properties.get(SizeTieredMergePolicyFactory.BUCKET_LOW));
        bucket_high = Double.parseDouble(properties.get(SizeTieredMergePolicyFactory.BUCKET_HIGH));
        threshold_min = Integer.parseInt(properties.get(SizeTieredMergePolicyFactory.THRESHOLD_MIN));
        threshold_max = Integer.parseInt(properties.get(SizeTieredMergePolicyFactory.THRESHOLD_MAX));
        min_sstable_size = Long.parseLong(properties.get(SizeTieredMergePolicyFactory.MIN_SSTABLE_SIZE));
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
