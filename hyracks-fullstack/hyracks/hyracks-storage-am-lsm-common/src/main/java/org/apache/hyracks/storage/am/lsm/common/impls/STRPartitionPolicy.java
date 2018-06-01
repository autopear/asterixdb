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

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentPartitionPolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;

import java.util.*;

/**
 * Created by mohiuddin on 4/13/17.
 */


public class STRPartitionPolicy implements IComponentPartitionPolicy {


    @Override public List<List<ITupleReference>> mergeByPartition(HashMap<Point, ITupleReference> mergingTuples,
            int numberOfPartitions) {
        List<List<ITupleReference>> listOfnewTuplesOfPartitions = new ArrayList<>();
        for(int i =0; i <numberOfPartitions; i++) {
            List<ITupleReference> tuples = new ArrayList<>();
            listOfnewTuplesOfPartitions.add(tuples);
        }
        createSTRPartitionsFromPoints(mergingTuples.keySet(), numberOfPartitions, mergingTuples, listOfnewTuplesOfPartitions);

        return listOfnewTuplesOfPartitions;
    }

    @Override
    public List<ILSMDiskComponent> findOverlappingComponents(ILSMDiskComponent mergingComponent,
            List<ILSMDiskComponent> immutableComponents)
    {
        List<Double> mergingComponentMbr;
        List<ILSMDiskComponent> overlappingComponents = new ArrayList<>();
        try {
            mergingComponentMbr = ((AbstractLSMDiskComponent)mergingComponent).GetMBR();
            Rectangle mergingComponentMbrRectangle;

            if(mergingComponentMbr==null || mergingComponentMbr.size() != 4)
                return null;

            mergingComponentMbrRectangle = new Rectangle(mergingComponentMbr.get(0),mergingComponentMbr.get(1),mergingComponentMbr.get(2),mergingComponentMbr.get(3));

            for (ILSMDiskComponent c : immutableComponents) {

                List<Double> mbr = ((AbstractLSMDiskComponent)c).GetMBR();
                if(mbr==null || mbr.size() != 4)
                    continue;

                Rectangle rMbr;
                rMbr = new Rectangle(mbr.get(0),mbr.get(1),mbr.get(2),mbr.get(3));

                if(mergingComponentMbrRectangle.isIntersected(rMbr))
                    overlappingComponents.add(c);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }


        return overlappingComponents;
    }
    @Override
    public List<ILSMDiskComponent> findOverlappingComponents(ILSMDiskComponent mergingComponent,
            List<ILSMDiskComponent> immutableComponents, Rectangle newMBR)
    {
        List<Double> mergingComponentMbr;
        List<ILSMDiskComponent> overlappingComponents = new ArrayList<>();
        try {
            mergingComponentMbr = ((AbstractLSMDiskComponent)mergingComponent).GetMBR();
            Rectangle mergingComponentMbrRectangle;

            if(mergingComponentMbr==null || mergingComponentMbr.size() != 4)
                return null;

            mergingComponentMbrRectangle = new Rectangle(mergingComponentMbr.get(0),mergingComponentMbr.get(1),mergingComponentMbr.get(2),mergingComponentMbr.get(3));
            newMBR.adjustMBR(mergingComponentMbrRectangle);
            for (ILSMDiskComponent c : immutableComponents) {

                List<Double> mbr = ((AbstractLSMDiskComponent)c).GetMBR();
                if(mbr==null || mbr.size() != 4)
                    continue;

                Rectangle rMbr;
                rMbr = new Rectangle(mbr.get(0),mbr.get(1),mbr.get(2),mbr.get(3));

                if(mergingComponentMbrRectangle.isIntersected(rMbr)) {
                    overlappingComponents.add(c);
                    newMBR.adjustMBR(rMbr);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


        return overlappingComponents;
    }
    @Override public Rectangle computeMBROfALevel(List<ILSMDiskComponent> immutableComponents) {
        Rectangle mbr = new Rectangle();
        for (ILSMDiskComponent c : immutableComponents) {

            List<Double> mbrdoubles = null;
            try {
                mbrdoubles = ((AbstractLSMDiskComponent)c).GetMBR();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if(mbrdoubles==null || mbrdoubles.size() != 4)
                continue;

            Rectangle rMbr;
            rMbr = new Rectangle(mbrdoubles.get(0),mbrdoubles.get(1),mbrdoubles.get(2),mbrdoubles.get(3));

            mbr.adjustMBR(rMbr);

        }

        return mbr;
    }

    @Override public void adjustMBROfALevel(Rectangle mbr, Rectangle newMBR) {
        mbr.adjustMBR(newMBR);
    }

    public void createSTRPartitionsFromPoints(Collection<Point> points, int numOfPartitions, HashMap<Point, ITupleReference> mergingTuples, List<List<ITupleReference>> listOfnewTuplesOfPartitions) {
        // Apply the STR algorithm in two rounds
        // 1- First round, sort points by X and split into the given columns

        ArrayList xSortedPoints = new ArrayList(points);
        Collections.sort(xSortedPoints, new Comparator<Point>() {
            @Override
            public int compare(Point a, Point b) {
                return a.x < b.x? -1 : (a.x > b.x? 1 : 0);
            }});

        int nodeCapacity = (int)Math.ceil(points.size()/numOfPartitions);

        List[] verticalSlices = verticalSlices(xSortedPoints,
                (int) Math.ceil(Math.sqrt(numOfPartitions)));

        List parentBoundables = new ArrayList();
        int index = 0;
        for (int i = 0; i < verticalSlices.length && index< listOfnewTuplesOfPartitions.size(); i++) {
            index = createPartitionsFromAVerticalSlice(verticalSlices[i], nodeCapacity, index, mergingTuples, listOfnewTuplesOfPartitions);
        }
    }
    protected int createPartitionsFromAVerticalSlice(List verticalSilcePoints, int nodeCapacity,
            int index, HashMap<Point, ITupleReference> mergingTuples, List<List<ITupleReference>> listOfnewTuplesOfPartitions) {
        //ArrayList parentBoundables = new ArrayList();
        List<ITupleReference> partitions = listOfnewTuplesOfPartitions.get(index);
        //parentBoundables.add(createNode(newLevel));
        ArrayList ySortedPoints = new ArrayList(verticalSilcePoints);
        Collections.sort(ySortedPoints, new Comparator<Point>() {
            @Override
            public int compare(Point a, Point b) {
                return a.y < b.y? -1 : (a.y > b.y? 1 : 0);
            }
        });

        for (Iterator i = ySortedPoints.iterator(); i.hasNext(); ) {
            Point currentPoint = (Point) i.next();
            ITupleReference tuple = mergingTuples.get(currentPoint);
            listOfnewTuplesOfPartitions.get(index).add(tuple);

            if (listOfnewTuplesOfPartitions.get(index).size() >= nodeCapacity && index<listOfnewTuplesOfPartitions.size()-1) {
                index++;
            }
        }
        return index;
    }

    protected List[] verticalSlices(List points, int sliceCount) {
        int sliceCapacity = (int) Math.ceil(points.size() / (double) sliceCount);
        List[] slices = new List[sliceCount];
        Iterator i = points.iterator();
        for (int j = 0; j < sliceCount; j++) {
            slices[j] = new ArrayList();
            int pointAddedToSlice = 0;
            while (i.hasNext() && pointAddedToSlice < sliceCapacity) {
                Point childPoint = (Point) i.next();
                slices[j].add(childPoint);
                pointAddedToSlice++;
            }
        }
        return slices;
    }
}
