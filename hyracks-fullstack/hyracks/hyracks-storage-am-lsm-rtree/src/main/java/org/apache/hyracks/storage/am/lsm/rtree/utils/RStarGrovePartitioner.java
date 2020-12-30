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

package org.apache.hyracks.storage.am.lsm.rtree.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

public class RStarGrovePartitioner {
    public enum MinimizationFunction {
        PERIMETER,
        AREA
    }

    static void computeMargins(double[][] coords, int start, int end, double[][] marginsLeft, double[][] marginsRight) {
        final int numDimensions = coords.length;
        assert numDimensions == marginsLeft.length && numDimensions == marginsRight.length;
        final int numPoints = end - start;
        for (int d = 0; d < numDimensions; d++) {
            double minLeft = Double.POSITIVE_INFINITY, minRight = Double.POSITIVE_INFINITY;
            double maxLeft = Double.NEGATIVE_INFINITY, maxRight = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < numPoints; i++) {
                minLeft = Math.min(minLeft, coords[d][start + i]);
                maxLeft = Math.max(maxLeft, coords[d][start + i]);
                minRight = Math.min(minRight, coords[d][end - 1 - i]);
                maxRight = Math.max(maxRight, coords[d][end - 1 - i]);
                marginsLeft[d][i] = maxLeft - minLeft;
                marginsRight[d][numPoints - 1 - i] = maxRight - minRight;
            }
        }
    }

    static class SplitTask {
        /**
         * The range of points to partition
         */
        int start, end;
        /**
         * The coordinate of the minimum corner of the MBB that covers these points
         */
        double[] min;
        /**
         * The coordinate of the maximum corner of the MBB that covers these points
         */
        double[] max;
        /**
         * Where the separation happens in the range [start, end)
         */
        int separator;
        /**
         * The coordinate along where the separation happened
         */
        double splitCoord;
        /**
         * The axis along where the separation happened. 0 for X and 1 for Y
         */
        int axis;

        SplitTask(int s, int e, int numDimensions) {
            this.start = s;
            this.end = e;
            this.min = new double[numDimensions];
            this.max = new double[numDimensions];
        }
    }

    protected static boolean isValid(long size, long partitionSize) {
        int lowerBound = (int) Math.ceil((double) size / partitionSize);
        int upperBound = (int) Math.floor((double) size / partitionSize);
        return lowerBound <= upperBound;
    }

    public static EnvelopeNDLite[] partitionPoints(final double[][] coords, final int partitionSize,
            final double fractionMinSplitSize, final MinimizationFunction f) {
        final int numDimensions = coords.length;
        final int numPoints = coords[0].length;
        class SorterDim implements IndexedSortable {
            int sortAxis;

            @Override
            public int compare(int i, int j) {
                return (int) Math.signum(coords[sortAxis][i] - coords[sortAxis][j]);
            }

            @Override
            public void swap(int i, int j) {
                for (int d = 0; d < numDimensions; d++) {
                    double t = coords[d][i];
                    coords[d][i] = coords[d][j];
                    coords[d][j] = t;
                }
            }
        }
        Stack<SplitTask> rangesToSplit = new Stack<SplitTask>();
        SplitTask firstRange = new SplitTask(0, numPoints, numDimensions);
        // The MBR of the first range covers the entire space (-Infinity, Infinity)
        for (int d = 0; d < numDimensions; d++) {
            firstRange.min[d] = Double.NEGATIVE_INFINITY;
            firstRange.max[d] = Double.POSITIVE_INFINITY;
        }
        rangesToSplit.push(firstRange);
        List<EnvelopeNDLite> finalizedSplits = new ArrayList<>();

        // Create the temporary arrays once to avoid the overhead of recreating them in each loop iteration

        // Store all the values at each split point so that we can choose the minimum at the end
        double[][] marginsLeft = new double[numDimensions][numPoints];
        double[][] marginsRight = new double[numDimensions][numPoints];

        while (!rangesToSplit.isEmpty()) {
            SplitTask range = rangesToSplit.pop();

            if (range.end - range.start <= partitionSize) {
                // No further splitting needed. Create a final partition
                EnvelopeNDLite partitionMBR = new EnvelopeNDLite(numDimensions);
                partitionMBR.setEmpty();
                double[] point = new double[numDimensions];
                for (int i = range.start; i < range.end; i++) {
                    for (int d = 0; d < numDimensions; d++) {
                        point[d] = coords[d][i];
                    }
                    partitionMBR.merge(point);
                }
                // Mark the range as a leaf partition by setting the separator to
                // a negative number x. The partition ID is -x-1
                range.separator = -finalizedSplits.size() - 1;
                finalizedSplits.add(partitionMBR);
                continue;
            }

            // ChooseSplitAxis
            // Sort the entries by each dimension and compute S, the sum of all margin-values of the different distributions
            final int minSplitSize = Math.max(partitionSize, (int) ((range.end - range.start) * fractionMinSplitSize));
            final int numPossibleSplits = (range.end - range.start) - 2 * minSplitSize + 1;

            QuickSort quickSort = new QuickSort();
            SorterDim sorter = new SorterDim();

            // Choose the axis with the minimum sum of margin
            int chosenAxis = 0;
            double minMargin = Double.POSITIVE_INFINITY;

            // Keep the sumMargins for all sort dimensions
            for (sorter.sortAxis = 0; sorter.sortAxis < numDimensions; sorter.sortAxis++) {
                // Sort then compute sum margin.
                quickSort.sort(sorter, range.start, range.end);

                computeMargins(coords, range.start, range.end, marginsLeft, marginsRight);

                double margin = 0.0;
                for (int d = 0; d < numDimensions; d++)
                    for (int k = 1; k <= numPossibleSplits; k++)
                        margin += marginsLeft[d][minSplitSize + k - 1 - 1] + marginsRight[d][minSplitSize + k - 1 - 1];

                if (margin < minMargin) {
                    minMargin = margin;
                    chosenAxis = sorter.sortAxis;
                }
            }

            // Repeat the sorting along the chosen axis if needed
            if (sorter.sortAxis != chosenAxis) {
                sorter.sortAxis = chosenAxis;
                quickSort.sort(sorter, range.start, range.end);
                // Recompute the margins based on the new sorting order
                computeMargins(coords, range.start, range.end, marginsLeft, marginsRight);
            }

            // Along the chosen axis, choose the distribution with the minimum area (or perimeter)
            // Note: Since we partition points, the overlap is always zero and we ought to choose based on the total volume

            int chosenK = -1;
            double minValue = Double.POSITIVE_INFINITY;
            for (int k = 1; k <= numPossibleSplits; k++) {
                // Skip if k is invalid (either side induce an invalid size)
                int size1 = minSplitSize + k - 1;
                if (!isValid(size1, partitionSize)) {
                    continue;
                }

                int size2 = range.end - range.start - size1;
                if (!isValid(size2, partitionSize)) {
                    continue;
                }

                double splitValue = 0.0;
                switch (f) {
                    case AREA:
                        double vol1 = 1.0, vol2 = 1.0;
                        for (int d = 0; d < numDimensions; d++) {
                            vol1 *= marginsLeft[d][minSplitSize + k - 1 - 1];
                            vol2 *= marginsRight[d][minSplitSize + k - 1 - 1];
                        }
                        splitValue = vol1 + vol2;
                        break;
                    case PERIMETER:
                        for (int d = 0; d < numDimensions; d++) {
                            splitValue += marginsLeft[d][minSplitSize + k - 1 - 1];
                            splitValue += marginsRight[d][minSplitSize + k - 1 - 1];
                        }
                        break;
                    default:
                        throw new RuntimeException("Unsupported function " + f);
                }
                if (splitValue < minValue) {
                    chosenK = k;
                    minValue = splitValue;
                }
            }

            // Split at the chosenK
            range.separator = range.start + minSplitSize - 1 + chosenK;

            // Create two sub-ranges
            // Sub-range 1 covers the range [rangeStart, separator)
            // Sub-range 2 covers the range [separator, rangeEnd)
            SplitTask range1 = new SplitTask(range.start, range.separator, numDimensions);
            SplitTask range2 = new SplitTask(range.separator, range.end, numDimensions);
            // Set the MBB of both to split along the chosen value
            for (int d = 0; d < numDimensions; d++) {
                range1.min[d] = range2.min[d] = range.min[d];
                range1.max[d] = range2.max[d] = range.max[d];
            }
            range.axis = chosenAxis;
            range.splitCoord = range1.max[chosenAxis] = range2.min[chosenAxis] = coords[chosenAxis][range.separator];
            rangesToSplit.add(range1);
            rangesToSplit.add(range2);
        }

        EnvelopeNDLite[] ret = new EnvelopeNDLite[finalizedSplits.size()];
        return finalizedSplits.toArray(ret);
    }
}
