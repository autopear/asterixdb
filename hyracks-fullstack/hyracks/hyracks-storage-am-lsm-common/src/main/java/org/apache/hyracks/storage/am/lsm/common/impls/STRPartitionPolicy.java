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

    /**MBR of the input file*/
    private final Rectangle mbr = new Rectangle();
    /**Number of rows and columns*/
    private int columns, rows;
    /**Locations of vertical strips*/
    private double[] xSplits;
    /**Locations of horizontal strips for each vertical strip*/
    private double[] ySplits;


    @Override public List<List<ITupleReference>> mergeByPartition(HashMap<Point, ITupleReference> mergingTuples,
            int numberOfPartitions) {
        List<List<ITupleReference>> listOfnewTuplesOfPartitions = new ArrayList<>();
        for(int i =0; i <numberOfPartitions; i++) {
            List<ITupleReference> tuples = new ArrayList<>();
            listOfnewTuplesOfPartitions.add(tuples);
        }

        //createFromPoints();

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

    public void createFromPoints(Rectangle mbr, Point[] points, int capacity) {
        // Apply the STR algorithm in two rounds
        // 1- First round, sort points by X and split into the given columns
        Arrays.sort(points, new Comparator<Point>() {
            @Override
            public int compare(Point a, Point b) {
                return a.x < b.x? -1 : (a.x > b.x? 1 : 0);
            }});
        // Calculate partitioning numbers based on a grid
        int numSplits = (int) Math.ceil((double)points.length / capacity);
        //GridInfo gridInfo = new GridInfo(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
        this.calculateCellDimensions(numSplits, mbr);
        //this.columns = gridInfo.columns;
        //this.rows = gridInfo.rows;
        this.xSplits = new double[columns];
        this.ySplits = new double[rows * columns];
        int prev_quantile = 0;
        this.mbr.set(mbr);
        for (int column = 0; column < columns; column++) {
            int col_quantile = (column + 1) * points.length / columns;
            // Determine the x split for this column. Last column has a special handling
            this.xSplits[column] = col_quantile == points.length ? mbr.x2 : points[col_quantile-1].x;
            // 2- Partition this column vertically in the same way
            Arrays.sort(points, prev_quantile, col_quantile, new Comparator<Point>() {
                @Override
                public int compare(Point a, Point b) {
                    return a.y < b.y? -1 : (a.y > b.y? 1 : 0);
                }
            });
            // Compute y-splits for this column
            for (int row = 0; row < rows; row++) {
                int row_quantile = (prev_quantile * (rows - (row+1)) +
                        col_quantile * (row+1)) / rows;
                // Determine y split for this row. Last row has a special handling
                this.ySplits[column * rows + row] = row_quantile == col_quantile ? mbr.y2 : points[row_quantile].y;
            }

            prev_quantile = col_quantile;
        }
    }

    public void calculateCellDimensions(int numCells, Rectangle mbr) {
        int gridCols = 1;
        int gridRows = 1;
        while (gridRows * gridCols < numCells) {
            // (  cellWidth          >    cellHeight        )
            if ((mbr.x2 - mbr.x1) / gridCols > (mbr.y2 - mbr.y1) / gridRows) {
                gridCols++;
            } else {
                gridRows++;
            }
        }
        columns = gridCols;
        rows = gridRows;
    }
}
