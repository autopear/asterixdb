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
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILevelMergePolicyHelper;


public abstract class AbstractLevelMergePolicyHelper implements ILevelMergePolicyHelper {
    protected final AbstractLSMIndex index;
    protected final long tableSize;

    public AbstractLevelMergePolicyHelper(AbstractLSMIndex index, long tableSize) {
        this.index = index;
        this.tableSize = tableSize;
    }

    public ILSMDiskComponent getOldestComponent(List<ILSMDiskComponent> components, long level) {
        long levelSequence = -1;
        ILSMDiskComponent oldest = null;
        for (ILSMDiskComponent component : components) {
            if (component.getLevel() == level) {
                if (oldest == null) {
                    levelSequence = component.getLevelSequence();
                    oldest = component;
                } else {
                    long t = component.getLevelSequence();
                    if (t < levelSequence) {
                        levelSequence = t;
                        oldest = component;
                    }
                }
            }
        }
        return oldest;
    }

    public ILSMDiskComponent getRandomComponent(List<ILSMDiskComponent> components, long level, Distribution distribution) {
        if (level == 0) {
            return getOldestComponent(components, 0);
        }
        List<ILSMDiskComponent> levelComponents = new ArrayList<>();
        for (ILSMDiskComponent component : components) {
            if (component.getLevel() == level) {
                levelComponents.add(component);
            }
        }
        if (levelComponents.isEmpty()) {
            return null;
        }
        int r = ThreadLocalRandom.current().nextInt(0, levelComponents.size());
        return levelComponents.get(r);
    }
}
