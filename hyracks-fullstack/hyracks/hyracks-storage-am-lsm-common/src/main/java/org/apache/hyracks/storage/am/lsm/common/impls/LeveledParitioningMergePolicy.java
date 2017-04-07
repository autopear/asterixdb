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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.common.api.*;

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
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested)
            throws HyracksDataException, IndexException {
        // Do nothing

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
