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

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;

public class ZCurvePartitioner extends CurvePartitioner {

    private int dim;
    private double[] bounds;
    private double worldWidth;
    private double worldHeight;

    public ZCurvePartitioner(List<ILSMDiskComponent> components) throws HyracksDataException {
        super(components);
    }

    public ZCurvePartitioner(List<ILSMDiskComponent> components, long level) throws HyracksDataException {
        super(components, level);
    }

    @Override
    protected void initialize() throws HyracksDataException {
        dim = getCenter(components.get(0)).length;
        bounds = null;
        for (ILSMDiskComponent c : components) {
            double[] minMBR = AbstractLSMRTree.bytesToDoubles(c.getMinKey());
            double[] maxMBR = AbstractLSMRTree.bytesToDoubles(c.getMaxKey());
            if (bounds == null) {
                bounds = new double[4];
                bounds[0] = minMBR[0];
                bounds[1] = minMBR[1];
                bounds[2] = maxMBR[0];
                bounds[3] = maxMBR[1];
            } else {
                for (int i = 0; i < 2; i++) {
                    bounds[i] = Math.min(bounds[i], minMBR[i]);
                    bounds[2 + i] = Math.max(bounds[2 + i], maxMBR[i]);
                }
            }
        }
        worldWidth = bounds[2] - bounds[0];
        worldHeight = bounds[3] - bounds[1];
    }

    private int[] getCenterKey(double[] center) {
        int x = (int) ((center[0] - bounds[0]) * Integer.MAX_VALUE / worldWidth);
        int y = (int) ((center[1] - bounds[1]) * Integer.MAX_VALUE / worldHeight);
        return new int[] { x, y };
    }

    @Override
    protected void compute() throws HyracksDataException {
        for (ILSMDiskComponent c : components) {
            int[] center = getCenterKey(getCenter(c));

            long morton = 0;
            for (long pos = 0; pos < Integer.SIZE; pos++) {
                long mask = 1L << pos;
                morton |= ((long) center[0] & mask) << (pos + 1);
                morton |= ((long) center[1] & mask) << pos;
            }
            curveValues.put(c, morton);
        }
    }
}
