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
            if (c.getMinId() == level) {
                ret.add(c);
            }
        }
        return ret;
    }

    public ILSMDiskComponent getOldestComponent(List<ILSMDiskComponent> components, long level) {
        long levelSequence = -1;
        ILSMDiskComponent oldest = null;
        for (ILSMDiskComponent component : components) {
            if (component.getMinId() == level) {
                if (oldest == null) {
                    levelSequence = component.getMaxId();
                    oldest = component;
                } else {
                    long t = component.getMaxId();
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
            if (component.getMinId() == level) {
                if (newest == null) {
                    levelSequence = component.getMaxId();
                    newest = component;
                } else {
                    long t = component.getMaxId();
                    if (t > levelSequence) {
                        levelSequence = t;
                        newest = component;
                    }
                }
            }
        }
        return newest;
    }

    public List<ILSMDiskComponent> getMinimumOverlappingComponents(List<ILSMDiskComponent> components, long level,
            boolean absolute) {
        List<ILSMDiskComponent> thisLevelComponents = getComponents(components, level);
        List<ILSMDiskComponent> nextLevelComponents = getComponents(components, level + 1);
        if (nextLevelComponents.isEmpty()) {
            return Collections.singletonList(getOldestComponent(thisLevelComponents, level));
        }

        /*thisLevelComponents.sort(new Comparator<ILSMDiskComponent>() {
            @Override
            public int compare(ILSMDiskComponent c1, ILSMDiskComponent c2) {
                return Long.compare(c1.getLevelSequence(), c2.getLevelSequence());
            }
        });*/

        long overlapped = components.size();
        List<ILSMDiskComponent> toMerge = new ArrayList<>();
        for (ILSMDiskComponent picked : thisLevelComponents) {
            List<ILSMDiskComponent> overlappedComponents =
                    getOverlappingComponents(picked, nextLevelComponents, absolute);
            if (overlappedComponents.isEmpty()) {
                return Collections.singletonList(picked);
            }
            if (overlappedComponents.size() < overlapped) {
                overlapped = overlappedComponents.size();
                toMerge.clear();
                toMerge.add(picked);
                toMerge.addAll(overlappedComponents);
            }
        }
        return toMerge;
    }

    public List<ILSMDiskComponent> getMaximumOverlappingComponents(List<ILSMDiskComponent> components, long level,
            boolean absolute) {
        List<ILSMDiskComponent> thisLevelComponents = getComponents(components, level);
        List<ILSMDiskComponent> nextLevelComponents = getComponents(components, level + 1);
        if (nextLevelComponents.isEmpty()) {
            return Collections.singletonList(getOldestComponent(thisLevelComponents, level));
        }

        /*thisLevelComponents.sort(new Comparator<ILSMDiskComponent>() {
            @Override
            public int compare(ILSMDiskComponent c1, ILSMDiskComponent c2) {
                return Long.compare(c1.getLevelSequence(), c2.getLevelSequence());
            }
        });*/

        long overlapped = -1L;
        List<ILSMDiskComponent> toMerge = new ArrayList<>();
        for (ILSMDiskComponent picked : thisLevelComponents) {
            List<ILSMDiskComponent> overlappedComponents =
                    getOverlappingComponents(picked, nextLevelComponents, absolute);
            if (overlappedComponents.size() > overlapped) {
                overlapped = overlappedComponents.size();
                toMerge.clear();
                toMerge.add(picked);
                toMerge.addAll(overlappedComponents);
            }
        }
        return toMerge;
    }

    public List<ILSMDiskComponent> getBestComponents(List<ILSMDiskComponent> components, long level, boolean absolute) {
        if (level == 0) {
            ILSMDiskComponent picked = getOldestComponent(components, level);
            List<ILSMDiskComponent> toMerge = new ArrayList<>(getOverlappingComponents(picked, components, absolute));
            toMerge.add(0, picked);
            return toMerge;
        }
        List<ILSMDiskComponent> srcComponents = new ArrayList<>();
        List<ILSMDiskComponent> dstComponents = new ArrayList<>();
        for (ILSMDiskComponent component : components) {
            if (component.getMinId() == level) {
                srcComponents.add(component);
            }
            if (component.getMinId() == level + 1) {
                dstComponents.add(component);
            }
        }
        if (srcComponents.size() == 1) {
            List<ILSMDiskComponent> toMerge =
                    new ArrayList<>(getOverlappingComponents(srcComponents.get(0), components, absolute));
            toMerge.add(0, srcComponents.get(0));
            return toMerge;
        }
        if (dstComponents.isEmpty()) {
            ILSMDiskComponent picked = getOldestComponent(components, level);
            List<ILSMDiskComponent> toMerge = new ArrayList<>(getOverlappingComponents(picked, components, absolute));
            toMerge.add(0, picked);
            return toMerge;
        }
        ILSMDiskComponent best = null;
        int cnt = -1;
        for (ILSMDiskComponent component : srcComponents) {
            int t = getOverlappingComponents(component, dstComponents, absolute).size();
            if (best == null || t > cnt) {
                best = component;
                cnt = t;
            }
        }
        if (best == null) {
            best = getOldestComponent(srcComponents, level);
        }
        List<ILSMDiskComponent> toMerge = new ArrayList<>(getOverlappingComponents(best, components, absolute));
        toMerge.add(0, best);
        return toMerge;
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
