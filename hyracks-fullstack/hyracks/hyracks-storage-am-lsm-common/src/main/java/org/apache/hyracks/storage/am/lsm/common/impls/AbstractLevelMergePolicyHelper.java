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

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILevelMergePolicyHelper;

public abstract class AbstractLevelMergePolicyHelper implements ILevelMergePolicyHelper {
    protected final AbstractLSMIndex index;

    public AbstractLevelMergePolicyHelper(AbstractLSMIndex index) {
        this.index = index;
    }

    public List<ILSMDiskComponent> getComponents(List<ILSMDiskComponent> components, long level) {
        List<ILSMDiskComponent> ret = new ArrayList<>();
        for (int i = 0; i < components.size(); i++) {
            ILSMDiskComponent c = components.get(i);
            if (c.getLevel() == level) {
                ret.add(c);
            }
        }
        return ret;
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
                    if (t <= levelSequence) {
                        levelSequence = t;
                        oldest = component;
                    }
                }
            }
        }
        return oldest;
    }

    public ILSMDiskComponent getNewestComponent(List<ILSMDiskComponent> components, long level) {
        long levelSequence = -1;
        ILSMDiskComponent newest = null;
        for (ILSMDiskComponent component : components) {
            if (component.getLevel() == level) {
                if (newest == null) {
                    levelSequence = component.getLevelSequence();
                    newest = component;
                } else {
                    long t = component.getLevelSequence();
                    if (t > levelSequence) {
                        levelSequence = t;
                        newest = component;
                    }
                }
            }
        }
        return newest;
    }

    public List<ILSMDiskComponent> getMinimumOverlappingComponents(List<ILSMDiskComponent> components, long level) {
        List<ILSMDiskComponent> mergableComponents = new ArrayList<>();
        long overlapped = components.size();
        List<ILSMDiskComponent> nextLevelComponents = new ArrayList<>();
        for (ILSMDiskComponent c : components) {
            if (c.getLevel() == level) {
                if (nextLevelComponents.isEmpty()) {
                    for (ILSMDiskComponent c1 : components) {
                        if (c1.getLevel() == level + 1) {
                            nextLevelComponents.add(c1);
                        }
                    }
                    if (nextLevelComponents.isEmpty()) {
                        return Collections.singletonList(getOldestComponent(components, level));
                    }
                    overlapped = nextLevelComponents.size();
                }
                List<ILSMDiskComponent> overlapComponents = getOverlappingComponents(c, nextLevelComponents);
                if (overlapComponents.size() <= overlapped) {
                    overlapped = overlapComponents.size();
                    mergableComponents.clear();
                    mergableComponents.add(c);
                    mergableComponents.addAll(overlapComponents);
                }
            }
        }
        return mergableComponents;
    }

    public List<ILSMDiskComponent> getMaximumOverlappingComponents(List<ILSMDiskComponent> components, long level) {
        List<ILSMDiskComponent> mergableComponents = new ArrayList<>();
        long overlapped = 0;
        List<ILSMDiskComponent> nextLevelComponents = new ArrayList<>();
        for (ILSMDiskComponent c : components) {
            if (c.getLevel() == level) {
                if (nextLevelComponents.isEmpty()) {
                    for (ILSMDiskComponent c1 : components) {
                        if (c1.getLevel() == level + 1) {
                            nextLevelComponents.add(c1);
                        }
                    }
                    if (nextLevelComponents.isEmpty()) {
                        return Collections.singletonList(getOldestComponent(components, level));
                    }
                }
                List<ILSMDiskComponent> overlapComponents = getOverlappingComponents(c, nextLevelComponents);
                if (overlapComponents.size() >= overlapped) {
                    overlapped = overlapComponents.size();
                    mergableComponents.clear();
                    mergableComponents.add(c);
                    mergableComponents.addAll(overlapComponents);
                }
            }
        }
        return mergableComponents;
    }

    public ILSMDiskComponent getBestComponent(List<ILSMDiskComponent> components, long level) {
        if (level == 0) {
            return getOldestComponent(components, level);
        }
        List<ILSMDiskComponent> srcComponents = new ArrayList<>();
        List<ILSMDiskComponent> dstComponents = new ArrayList<>();
        for (ILSMDiskComponent component : components) {
            if (component.getLevel() == level) {
                srcComponents.add(component);
            }
            if (component.getLevel() == level + 1) {
                dstComponents.add(component);
            }
        }
        if (srcComponents.size() == 1) {
            return srcComponents.get(0);
        }
        if (dstComponents.isEmpty()) {
            return getOldestComponent(components, level);
        }
        ILSMDiskComponent best = null;
        int cnt = -1;
        for (ILSMDiskComponent component : srcComponents) {
            int t = getOverlappingComponents(component, dstComponents).size();
            if (best == null || t > cnt) {
                best = component;
                cnt = t;
            }
        }
        return best == null ? getOldestComponent(srcComponents, level) : best;
    }

    public ILSMDiskComponent getRandomComponent(List<ILSMDiskComponent> components, long level,
            Distribution distribution) {
        List<ILSMDiskComponent> levelComponents = getComponents(components, level);
        int total = levelComponents.size();
        if (total == 0) {
            return null;
        }
        if (total == 1) {
            return levelComponents.get(0);
        }
        int r;
        switch (distribution) {
            case Binomial:
                r = new BinomialDistribution(total, 0.5).sample();
                break;
            case Latest:
                r = new ZipfDistribution(total, 0.9).sample();
            case Oldest:
                r = total - 1 - new ZipfDistribution(total, 0.9).sample();
                break;
            case Uniform:
            default: {
                r = new UniformIntegerDistribution(0, total).sample();
            }
        }
        return levelComponents.get(r);
    }
}
