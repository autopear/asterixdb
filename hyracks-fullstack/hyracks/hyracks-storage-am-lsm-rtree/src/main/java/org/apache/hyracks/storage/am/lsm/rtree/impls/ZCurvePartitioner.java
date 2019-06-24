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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;

public class ZCurvePartitioner extends CurvePartitioner {

    public ZCurvePartitioner(List<ILSMDiskComponent> components) throws HyracksDataException {
        super(components);
    }

    public ZCurvePartitioner(List<ILSMDiskComponent> components, long level) throws HyracksDataException {
        super(components, level);
    }

    @Override
    protected void compute() throws HyracksDataException {
        Map<String, ArrayList<ILSMDiskComponent>> centersMap = new HashMap<>();
        Map<String, double[]> centers = new HashMap<>();
        int dim = -1;
        for (ILSMDiskComponent c : components) {
            double[] center = getCenter(c);
            dim = center.length;
            String key = doubleArrayToKey(center);
            ArrayList<ILSMDiskComponent> t = centersMap.getOrDefault(key, new ArrayList<>());
            t.add(c);
            centersMap.put(key, t);
            centers.put(key, center);
        }

        if (centersMap.isEmpty()) {
            return;
        }

        double stepSize = Double.MAX_VALUE / 2;
        double[] bounds = new double[dim];

        while (true) {
            Map<String, Integer> quadrants = new HashMap<>();
            int changed = dim - 1;
            for (int i = dim - 1; i >= 0; i--) {
                for (String key : centers.keySet()) {
                    double[] center = centers.get(key);
                    int quadrant = quadrants.getOrDefault(key, 0);
                    if (center[i] >= bounds[i]) {
                        quadrant ^= (1 << (dim - i - 1));
                        quadrants.put(key, quadrant);
                    }
                    if (changed == i) {
                        if (center[i] >= bounds[i]) {
                            bounds[i] += stepSize;
                        } else {
                            bounds[i] -= stepSize;
                        }
                        changed--;
                    }
                }
            }

            Set<Integer> uniques = new HashSet<>();
            for (String key : quadrants.keySet()) {
                int quadrant = quadrants.get(key);
                for (ILSMDiskComponent c : centersMap.get(key)) {
                    curveValues.put(c, quadrant);
                }
                uniques.add(quadrant);
            }

            stepSize /= 2;

            if (uniques.size() == centers.size() || stepSize <= 2 * DoublePointable.getEpsilon()) {
                return;
            }
        }
    }

    public String doubleArrayToKey(double[] doubles) {
        if (doubles == null) {
            return "null";
        }
        String s = Double.toString(doubles[0]);
        for (int i = 1; i < doubles.length; i++) {
            s += "+" + Double.toString(doubles[i]);
        }
        return s;
    }
}
