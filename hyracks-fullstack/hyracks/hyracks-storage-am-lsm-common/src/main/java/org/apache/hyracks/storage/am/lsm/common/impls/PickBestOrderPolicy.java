package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.storage.am.lsm.common.api.IComponentOrderPolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;

import java.util.List;

/**
 * Created by mohiuddin on 7/15/18.
 */

public class PickBestOrderPolicy  implements IComponentOrderPolicy {
    public PickBestOrderPolicy()
    {

    }


    @Override
    public int pickComponentToMerge(List<ILSMDiskComponent> immutableDiskComponents,
            List<ILSMDiskComponent> immutableDiskComponentsForNextLevel, Rectangle mbrOfThisLevel)
            throws Exception {

        int maxCount = Integer.MIN_VALUE;
        int maxIndex = 0, index =0 ;
        for (ILSMDiskComponent c : immutableDiskComponents) {

            Rectangle rMbr = ((AbstractLSMDiskComponent) c).getRangeOrMBR();
            if (rMbr == null || rMbr.isEmpty())
                continue;

            int count = 0;
            for (ILSMDiskComponent nextLC : immutableDiskComponentsForNextLevel) {

                Rectangle nextLC_Mbr = ((AbstractLSMDiskComponent) nextLC).getRangeOrMBR();
                if (nextLC_Mbr == null || nextLC_Mbr.isEmpty())
                    continue;

                if(rMbr.isIntersected(nextLC_Mbr))
                    count++;

            }

            if(count>maxCount)
            {
                maxCount = count;
                maxIndex = index;
            }

            index++;

        }
        return maxIndex;
        //return 0;
    }

}
