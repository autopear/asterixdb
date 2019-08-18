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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILevelMergePolicyHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILevelMergePolicyHelper.Distribution;

public class LevelMergePolicy implements ILSMMergePolicy {

    protected ILevelMergePolicyHelper helper;
    protected String pickStrategy;
    protected long level0Components;
    protected long level1Components;
    protected boolean absoluteOverlap;
    protected String properties;

    public static final Map<String, Distribution> dist = new HashMap<String, Distribution>() {
        {
            put(LevelMergePolicyFactory.RAND_UNIFORM, Distribution.Uniform);
            put(LevelMergePolicyFactory.RAND_BINOMIAL, Distribution.Binomial);
            put(LevelMergePolicyFactory.RAND_OLDEST, Distribution.Oldest);
            put(LevelMergePolicyFactory.RAND_LATEST, Distribution.Latest);
        }
    };

    public long getLevel0Components() {
        return level0Components;
    }

    public long getLevel1Components() {
        return level1Components;
    }

    public void setHelper(ILevelMergePolicyHelper helper) {
        this.helper = helper;
    }

    @Override
    public void diskComponentAdded(ILSMIndex index, List<ILSMDiskComponent> newComponents, boolean fullMergeIsRequested,
            boolean wasMerge) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        if (!areComponentsReadableWritableState(immutableComponents)) {
            return;
        }
        List<ILSMDiskComponent> componentsToMerge = getMergableComponents(immutableComponents);
        if (!componentsToMerge.isEmpty()) {
            ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            accessor.scheduleMerge(componentsToMerge);
        }
        /*
        if (newComponents.isEmpty()) {
            return;
        }
        long level = newComponents.get(0).getLevel();
        List<ILSMDiskComponent> components = helper.getComponents(index.getDiskComponents(), level);
        if (level == 0) {
            if (components.size() > level0Components) {
                ILSMDiskComponent picked = helper.getOldestComponent(index.getDiskComponents(), 0);
                List<ILSMDiskComponent> mergableComponents =
                        new ArrayList<>(helper.getOverlappingComponents(picked, index.getDiskComponents()));
                mergableComponents.add(0, picked);
                ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                accessor.scheduleMerge(mergableComponents);
            }
        } else {
            if (components.size() > Math.pow(level1Components, level)) {
                ILSMDiskComponent picked = null;
                if (pickStrategy.compareTo(LevelMergePolicyFactory.NEWEST) == 0) {
                    picked = helper.getNewestComponent(components, level);
                } else if (dist.containsKey(pickStrategy)) {
                    picked = helper.getRandomComponent(components, level, dist.get(pickStrategy));
                } else if (pickStrategy.compareTo(LevelMergePolicyFactory.MIN_OVERLAP) == 0) {
                    ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                    accessor.scheduleMerge(helper.getMinimumOverlappingComponents(components, level));
                } else if (pickStrategy.compareTo(LevelMergePolicyFactory.MAX_OVERLAP) == 0) {
                    ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                    accessor.scheduleMerge(helper.getMaximumOverlappingComponents(components, level));
                } else {
                    picked = helper.getOldestComponent(index.getDiskComponents(), level);
                }
                List<ILSMDiskComponent> mergableComponents =
                        new ArrayList<>(helper.getOverlappingComponents(picked, index.getDiskComponents()));
                mergableComponents.add(0, picked);
                ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                accessor.scheduleMerge(mergableComponents);
            }
        }*/
    }

    @Override
    public List<ILSMDiskComponent> getMergableComponents(List<ILSMDiskComponent> immutableComponents) {
        List<Long> levels = new ArrayList<>();
        for (ILSMDiskComponent component : immutableComponents) {
            long level = component.getLevel();
            if (!levels.contains(level)) {
                levels.add(level);
            }
        }
        levels.sort(Collections.reverseOrder());
        ILSMDiskComponent picked = null;
        for (long level : levels) {
            List<ILSMDiskComponent> components = helper.getComponents(immutableComponents, level);
            if (level == 0) {
                if (components.size() > level0Components) {
                    picked = helper.getOldestComponent(components, 0);
                    break;
                }
            } else {
                if (components.size() > Math.pow(level1Components, level)) {
                    if (pickStrategy.compareTo(LevelMergePolicyFactory.NEWEST) == 0) {
                        picked = helper.getNewestComponent(components, level);
                    } else if (pickStrategy.compareTo(LevelMergePolicyFactory.BEST) == 0) {
                        return helper.getBestComponents(components, level, absoluteOverlap);
                    } else if (dist.containsKey(pickStrategy)) {
                        picked = helper.getRandomComponent(components, level, dist.get(pickStrategy));
                    } else if (pickStrategy.compareTo(LevelMergePolicyFactory.MIN_OVERLAP) == 0) {
                        return helper.getMinimumOverlappingComponents(immutableComponents, level, absoluteOverlap);
                    } else if (pickStrategy.compareTo(LevelMergePolicyFactory.MAX_OVERLAP) == 0) {
                        return helper.getMaximumOverlappingComponents(immutableComponents, level, absoluteOverlap);
                    } else {
                        picked = helper.getOldestComponent(immutableComponents, level);
                    }
                    break;
                }
            }
        }
        if (picked != null) {
            List<ILSMDiskComponent> mergableComponents =
                    new ArrayList<>(helper.getOverlappingComponents(picked, immutableComponents, absoluteOverlap));
            mergableComponents.add(0, picked);
            return mergableComponents;
        }
        return Collections.emptyList();
    }

    @Override
    public void configure(Map<String, String> properties) {
        this.properties = StringUtils.join(properties).replaceAll("\n", " ");
        while (this.properties.contains("  ")) {
            this.properties = this.properties.replaceAll("  ", " ");
        }
        pickStrategy = properties.get(LevelMergePolicyFactory.PICK).toLowerCase();
        level0Components = Long.parseLong(properties.get(LevelMergePolicyFactory.NUM_COMPONENTS_0));
        level1Components = Long.parseLong(properties.get(LevelMergePolicyFactory.NUM_COMPONENTS_1));
        absoluteOverlap = properties.getOrDefault(LevelMergePolicyFactory.OVERLAP_MODE, "relative")
                .compareTo(LevelMergePolicyFactory.MODE_ABSOLUTE) == 0;
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        return isMergeOngoing(index.getDiskComponents());
    }

    private boolean isMergeOngoing(List<ILSMDiskComponent> immutableComponents) {
        int size = immutableComponents.size();
        for (int i = 0; i < size; i++) {
            if (immutableComponents.get(i).getState() == ILSMComponent.ComponentState.READABLE_MERGING) {
                return true;
            }
        }
        return false;
    }

    private boolean areComponentsReadableWritableState(List<ILSMDiskComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ILSMComponent.ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
        return true;
    }

    public String getProperties() {
        return properties;
    }
}
