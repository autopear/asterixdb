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
package org.apache.hyracks.storage.am.lsm.rtree.impls;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;

public abstract class CurvePartitioner {

    protected Map<ILSMDiskComponent, Long> curveValues;
    protected List<ILSMDiskComponent> components;

    public CurvePartitioner(List<ILSMDiskComponent> components) throws HyracksDataException {
        this.curveValues = new HashMap<>();
        this.components = components;
        initialize();
        compute();
    }

    public CurvePartitioner(List<ILSMDiskComponent> components, long level) throws HyracksDataException {
        this.curveValues = new HashMap<>();
        this.components = new ArrayList<>();
        for (ILSMDiskComponent c : components) {
            if (c.getLevel() == level) {
                this.components.add(c);
            }
        }
        initialize();
        compute();
    }

    protected abstract void initialize() throws HyracksDataException;

    protected abstract void compute() throws HyracksDataException;

    public long getValue(ILSMDiskComponent component) {
        return curveValues.getOrDefault(component, -1L);
    }

    public static double[] getCenter(ILSMDiskComponent component) throws HyracksDataException {
        double[] minMBR = AbstractLSMRTree.bytesToDoubles(component.getMinKey());
        double[] maxMBR = AbstractLSMRTree.bytesToDoubles(component.getMaxKey());
        double[] center = new double[minMBR.length];
        for (int i = 0; i < center.length; i++) {
            center[i] = (minMBR[i] + maxMBR[i]) / 2;
        }
        return center;
    }
}
