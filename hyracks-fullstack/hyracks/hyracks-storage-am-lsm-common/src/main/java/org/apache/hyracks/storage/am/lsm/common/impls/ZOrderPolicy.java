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

import org.apache.hyracks.storage.am.lsm.common.api.IComponentOrderPolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;


import java.util.List;

/**
 * Created by mohiuddin on 4/13/17.
 */
public class ZOrderPolicy implements IComponentOrderPolicy {

    protected static final int Resolution = Integer.MAX_VALUE;
    private long currentCursorOnZorder;

    public ZOrderPolicy()
    {
        currentCursorOnZorder = 0;
    }
    @Override
    public int pickComponentToMerge(List<ILSMDiskComponent> immutableDiskComponents, Rectangle mbrOfThisLevel) throws Exception {
        long minZvalueGreaterThanCurrentCursor= Long.MAX_VALUE;
        long minZValue = Long.MAX_VALUE ;
        int minZValueIndex = 0 ;
        int pickedComponentIndex = -1;
        int i =0 ;
        for (ILSMDiskComponent c : immutableDiskComponents) {

            List<Double> mbr = ((AbstractLSMDiskComponent) c).GetMBR();
            if (mbr == null || mbr.size() != 4)
                continue;

            Rectangle rMbr;
            rMbr = new Rectangle(mbr.get(0), mbr.get(1), mbr.get(2), mbr.get(3));
            Point centerPoint = rMbr.getCenterPoint();
            long zValueOfCenterPoint = computeZ(mbrOfThisLevel, centerPoint.x, centerPoint.y);
            if(zValueOfCenterPoint > currentCursorOnZorder && minZvalueGreaterThanCurrentCursor > zValueOfCenterPoint) {
                minZvalueGreaterThanCurrentCursor = zValueOfCenterPoint;
                pickedComponentIndex = i;

            }
            if(minZValue>zValueOfCenterPoint) {
                minZValue = zValueOfCenterPoint;
                minZValueIndex = i;
            }

            i++;
        }

        if(pickedComponentIndex >=0)
        {
            currentCursorOnZorder = minZvalueGreaterThanCurrentCursor;
            return pickedComponentIndex;
        }
        else //No ZValue Found Greater than currentCursor, then rotate to the min Zvalue
        {
            currentCursorOnZorder = minZValue;
            return minZValueIndex;
        }
    }

    public static long computeZ(Rectangle mbr, double x, double y) {
        int ix = (int) ((x - mbr.x1) * Resolution / mbr.getWidth());
        int iy = (int) ((y - mbr.y1) * Resolution / mbr.getHeight());
        return computeZOrder(ix, iy);
    }

    public static long computeZOrder(long x, long y) {
        long morton = 0;

        for (long bitPosition = 0; bitPosition < 32; bitPosition++) {
            long mask = 1L << bitPosition;
            morton |= (x & mask) << (bitPosition + 1);
            morton |= (y & mask) << bitPosition;
        }
        return morton;
    }
}
