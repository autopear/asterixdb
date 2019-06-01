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
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LevelMergePolicy implements ILSMMergePolicy {
    private static final Logger LOGGER = LogManager.getLogger();

    protected String pickStrategy;
    protected long level0Components;
    protected long level1Components;

    public long getLevel0Components() {
        return level0Components;
    }

    public long getLevel1Components() {
        return level1Components;
    }

    @Override
    public void diskComponentAdded(ILSMIndex index, List<ILSMDiskComponent> newComponents, boolean fullMergeIsRequested,
            boolean wasMerge) throws HyracksDataException {
        List<ILSMDiskComponent> componentsToMerge = getMergableComponents(index.getDiskComponents());
        if (!componentsToMerge.isEmpty()) {
            LOGGER.info("[LevelMerge]\t" + +Thread.currentThread().getId() + "\tToMerge: "
                    + getComponents(componentsToMerge) + "\tAll: " + getComponents(index.getDiskComponents()));
            ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            accessor.scheduleMerge(componentsToMerge);
        }
    }

    @Override
    public List<ILSMDiskComponent> getMergableComponents(List<ILSMDiskComponent> immutableComponents) {
        Map<Long, ArrayList<ILSMDiskComponent>> levels = new HashMap<>();
        for (ILSMDiskComponent component : immutableComponents) {
            long level = component.getLevel();
            ArrayList<ILSMDiskComponent> levelComponents = levels.getOrDefault(level, new ArrayList<>());
            levelComponents.add(component);
            levels.put(level, levelComponents);
        }
        List<Long> allLevels = new ArrayList<>(levels.keySet());
        Collections.sort(allLevels, Collections.reverseOrder());
        for (Long level : allLevels) {
            ArrayList<ILSMDiskComponent> levelComponents = levels.get(level);
            LOGGER.info("[check] " + level + ": " + getComponents(levelComponents));
            if (level == 0) {
                if (levelComponents.size() >= level0Components) {
                    Map<Long, ILSMDiskComponent> level0Map = new HashMap<>();
                    for (ILSMDiskComponent component : levelComponents) {
                        level0Map.put(component.getLevelSequence(), component);
                    }
                    List<Long> sids = new ArrayList<>(level0Map.keySet());
                    Collections.sort(sids);
                    List<ILSMDiskComponent> componentsToMerge = new ArrayList<>();
                    for (int i = 0; i < level0Components; i++) {
                        componentsToMerge.add(0, level0Map.get(sids.get(i)));
                    }
                    return componentsToMerge;
                }
            } else {
                if (levelComponents.size() > Math.pow(level1Components, level)) {
                    ILSMDiskComponent pickedComponent;
                    if (pickStrategy.compareTo("random") == 0) {
                        pickedComponent = pickRandomComponent(levelComponents);
                    } else {
                        pickedComponent = pickOldestComponent(levelComponents);
                    }
                    LOGGER.info("[picked]\t" + getComponentBaseName(pickedComponent));
                    List<ILSMDiskComponent> componentsToMerge =
                            findOverlappedComponents(pickedComponent, levels.getOrDefault(level + 1, null));
                    return componentsToMerge;
                }
            }
        }
        return Collections.emptyList();
    }

    private static String getComponentBaseName(ILSMDiskComponent c) {
        return c.getLevel() + "_" + c.getLevelSequence();
    }

    private static String getComponents(List<ILSMDiskComponent> components) {
        String ret = getComponentBaseName(components.get(0));
        for (int i = 1; i < components.size(); i++) {
            ret += ";" + getComponentBaseName(components.get(i));
        }
        return ret;
    }

    private ILSMDiskComponent pickOldestComponent(List<ILSMDiskComponent> components) {
        ILSMDiskComponent oldest = null;
        long sid = -1L;
        for (ILSMDiskComponent component : components) {
            long levelSequence = component.getLevelSequence();
            if (sid == -1L) {
                sid = levelSequence;
                oldest = component;
            } else {
                if (levelSequence < sid) {
                    sid = levelSequence;
                    oldest = component;
                }
            }
        }
        return oldest;
    }

    private ILSMDiskComponent pickRandomComponent(List<ILSMDiskComponent> components) {
        int r = ThreadLocalRandom.current().nextInt(0, components.size());
        return components.get(r);
    }

    private List<ILSMDiskComponent> findOverlappedComponents(ILSMDiskComponent component,
            List<ILSMDiskComponent> components) {
        if (components == null) {
            return Collections.singletonList(component);
        }
        Map<Long, ILSMDiskComponent> levelMap = new HashMap<>();
        for (ILSMDiskComponent c : components) {
            levelMap.put(c.getLevelSequence(), c);
        }
        List<Long> sids = new ArrayList<>(levelMap.keySet());
        Collections.sort(sids, Collections.reverseOrder());
        ArrayList<ILSMDiskComponent> componentsToMerge = new ArrayList<>();
        componentsToMerge.add(component);
        if (component.getLSMComponentFilter() == null) {
            for (int i = 0; i < sids.size(); i++) {
                ILSMDiskComponent c = levelMap.get(sids.get(i));
                componentsToMerge.add(1, c);
            }
        } else {
            MultiComparator filterCmp =
                    MultiComparator.create(component.getLSMComponentFilter().getFilterCmpFactories());
            for (int i = 0; i < sids.size(); i++) {
                ILSMDiskComponent c = levelMap.get(sids.get(i));
                if (isOverlapped(filterCmp, component, c)) {
                    componentsToMerge.add(1, c);
                }
            }
        }
        LOGGER.info(
                "[LevelMerge]\tOverlapped: " + getComponents(componentsToMerge) + "\tAll " + getComponents(components));
        return componentsToMerge;
    }

    private boolean isOverlapped(MultiComparator filterCmp, ILSMDiskComponent c1, ILSMDiskComponent c2) {
        ITupleReference minTuple1 = c1.getLSMComponentFilter().getMinTuple();
        ITupleReference maxTuple1 = c1.getLSMComponentFilter().getMaxTuple();
        ITupleReference minTuple2 = c2.getLSMComponentFilter().getMinTuple();
        ITupleReference maxTuple2 = c2.getLSMComponentFilter().getMaxTuple();
        try {
            if (filterCmp.compare(minTuple1, maxTuple1) > 0 || filterCmp.compare(minTuple2, maxTuple2) > 0) {
                return true;
            }
            if (filterCmp.compare(minTuple1, maxTuple2) > 0 || filterCmp.compare(minTuple2, maxTuple1) > 0) {
                return false;
            }
        } catch (HyracksDataException ex) {
            return true;
        }
        return true;
    }

    @Override
    public void configure(Map<String, String> properties) {
        pickStrategy = "oldest";
        level0Components = 2;
        level1Components = 4;
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
