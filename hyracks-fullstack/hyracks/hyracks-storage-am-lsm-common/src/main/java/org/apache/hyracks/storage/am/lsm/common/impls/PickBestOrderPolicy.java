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
 * Created by mohiuddin on 7/15/18.
 */

public class PickBestOrderPolicy implements IComponentOrderPolicy {
    public PickBestOrderPolicy() {

    }

    @Override
    public int pickComponentToMerge(List<ILSMDiskComponent> immutableDiskComponents,
            List<ILSMDiskComponent> immutableDiskComponentsForNextLevel, Rectangle mbrOfThisLevel) throws Exception {

        int maxCount = Integer.MIN_VALUE;
        int maxIndex = 0, index = 0;
        for (ILSMDiskComponent c : immutableDiskComponents) {

            Rectangle rMbr = ((AbstractLSMDiskComponent) c).getRangeOrMBR();
            if (rMbr == null || rMbr.isEmpty())
                continue;

            int count = 0;
            for (ILSMDiskComponent nextLC : immutableDiskComponentsForNextLevel) {

                Rectangle nextLC_Mbr = ((AbstractLSMDiskComponent) nextLC).getRangeOrMBR();
                if (nextLC_Mbr == null || nextLC_Mbr.isEmpty())
                    continue;

                if (rMbr.isIntersected(nextLC_Mbr))
                    count++;

            }

            if (count > maxCount) {
                maxCount = count;
                maxIndex = index;
            }

            index++;

        }
        return maxIndex;
        //return 0;
    }

}
