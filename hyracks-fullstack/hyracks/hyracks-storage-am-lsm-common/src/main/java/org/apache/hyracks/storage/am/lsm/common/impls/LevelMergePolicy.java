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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILevelMergePolicyHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILevelMergePolicyHelper.Distribution;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LevelMergePolicy implements ILSMMergePolicy {
    private static final Logger LOGGER = LogManager.getLogger();

    protected ILevelMergePolicyHelper helper;
    protected String pickStrategy;
    protected long level0Components;
    protected long level1Components;

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
        List<ILSMDiskComponent> componentsToMerge = getMergableComponents(index.getDiskComponents());
        if (!componentsToMerge.isEmpty()) {
            ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            accessor.scheduleMerge(componentsToMerge);
        }
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
                    } else if (dist.containsKey(pickStrategy)) {
                        picked = helper.getRandomComponent(components, level, dist.get(pickStrategy));
                    } else if (pickStrategy.compareTo(LevelMergePolicyFactory.MIN_OVERLAP) == 0) {
                        return helper.getMinimumOverlappingComponents(components, level);
                    } else if (pickStrategy.compareTo(LevelMergePolicyFactory.MAX_OVERLAP) == 0) {
                        return helper.getMaximumOverlappingComponents(components, level);
                    } else {
                        picked = helper.getOldestComponent(immutableComponents, level);
                    }
                    break;
                }
            }
        }
        if (picked != null) {
            List<ILSMDiskComponent> mergableComponents =
                    new ArrayList<>(helper.getOverlappingComponents(picked, immutableComponents));
            mergableComponents.add(0, picked);
            return mergableComponents;
        }
        return Collections.emptyList();
    }

    @Override
    public void configure(Map<String, String> properties) {
        pickStrategy = "oldest";
        level0Components = 2;
        level1Components = 4;
        //        pickStrategy = properties.getOrDefault(LevelMergePolicyFactory.PICK, "oldest").toLowerCase();
        //        level0Components = Long.getLong(properties.getOrDefault(LevelMergePolicyFactory.NUM_COMPONENTS_0, "10"));
        //        level1Components = Long.getLong(properties.getOrDefault(LevelMergePolicyFactory.NUM_COMPONENTS_1, "10"));
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
}
