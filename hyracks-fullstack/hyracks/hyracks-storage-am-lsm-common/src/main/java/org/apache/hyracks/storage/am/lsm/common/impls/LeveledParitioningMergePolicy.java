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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by mohiuddin on 4/5/17.
 */
public class LeveledParitioningMergePolicy implements ILSMMergePolicy {

    private int maxLevel;
    private int maxLevel0ComponentCount;
    private int maxLevel1ComponentCount;
    private long maxComponentSize;
    private IComponentOrderPolicy orderPolicy;
    private IComponentPartitionPolicy partitionPolicy;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException {

        List<List<ILSMDiskComponent>> immutableComponentsInLevels = new ArrayList<>(index.getDiskComponentsInLevels());

        for(int i = 0 ; i < maxLevel-1 ; i++)
        {
            List<ILSMDiskComponent> immutableComponents = immutableComponentsInLevels.get(i);
            double maxComponentCountInALevel = 0;
            if(i==0)
                maxComponentCountInALevel = maxLevel0ComponentCount;
            else if(i==1)
                maxComponentCountInALevel = maxLevel1ComponentCount;
            else
                maxComponentCountInALevel = Math.pow(maxLevel1ComponentCount, i);

            int componentIndexToMerge = 0;
            if(immutableComponents.size() >= maxComponentCountInALevel) {
                componentIndexToMerge = orderPolicy.pickComponentToMerge(immutableComponents);

                List<ILSMDiskComponent> immutableComponentsInNextLevel = immutableComponentsInLevels.get(i+1);
                List<ILSMDiskComponent> overlappingComponents  = findOverlappingComponents(immutableComponents.get(componentIndexToMerge), immutableComponentsInNextLevel);
//                if(overlappingComponents.size()==0) {
//                    immutableComponents.get(componentIndexToMerge).setLevel(i + 1);
//
//                }
//                else
                List<ILSMDiskComponent> newComponentsAfterMerge = partitionPolicy.mergeByPartition(overlappingComponents);
            }
//            for (ILSMComponent c : immutableComponents) {
//                try {
//                    List<Double> mbr = ((AbstractLSMDiskComponent)c).GetMBR();
//                    if(mbr==null)
//                        return;
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//            }

        }
        //Pair<Integer, Integer> mergeableIndexes = getMergableComponentsIndex(immutableComponents);

    }

    public List<ILSMDiskComponent> findOverlappingComponents(ILSMDiskComponent mergingComponent , List<ILSMDiskComponent> immutableComponents)
    {
        List<ILSMDiskComponent> overlappingComponents = new ArrayList<>();

        return overlappingComponents;
    }
    @Override
    public void configure(Map<String, String> properties) {
        // Do nothing
        maxLevel = Integer.parseInt(properties.get("max-level"));
        maxLevel0ComponentCount = Integer.parseInt(properties.get("max-level0-components-count"));
        maxLevel1ComponentCount = Integer.parseInt(properties.get("max-level1-components-count"));
        maxComponentSize = Long.parseLong(properties.get("max-component-size"));
        String order = properties.get("components-order-policy");
        if (order.equals("zorder"))
            orderPolicy = new ZOrderPolicy();
        String partition = properties.get("components-partition-policy");
        if (partition.equals("STR"))
            partitionPolicy = new STRPartitionPolicy();

    }

    /**
     * checks whether all given components are of READABLE_UNWRITABLE state
     *
     * @param immutableComponents
     * @return true if all components are of READABLE_UNWRITABLE state, false otherwise.
     */
    private boolean areComponentsReadableWritableState(List<ILSMDiskComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ILSMComponent.ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) {
        return false;
    }

    public String getName() {
        return "leveled-partitioning";
    }

    public int getMaxLevel() {
        return maxLevel;
    }
}
