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

package org.apache.hyracks.storage.am.lsm.common.api;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class ILSMOperationHistory {
    public List<Long> flushHist;
    public long lastFlush;
    public Semaphore flushSem;

    // Pair<Pair<k, Size>, Time>
    public List<Pair<Pair<Integer, Long>, Double>> mergeHist;
    public long mergeThreads;
    public Semaphore mergeSem;

    // Pair<Pair<N, Size>, Average Latency>
    public List<Pair<Pair<Integer, Double>, Double>> normalSearchHist;
    public int normalSearchCurrentN;
    public double normalSearchCurrentSize;
    public long normalSearchTotal;
    public long normalSearchCount;
    public List<Pair<Pair<Integer, Double>, Double>> mergeSearchHist;
    public int mergeSearchCurrentN;
    public double mergeSearchCurrentSize;
    public long mergeSearchTotal;
    public long mergeSearchCount;
    public Semaphore searchSem;

    private long mergeAccessCounter;
    private long mergeUpdateCounter;
    private double mergeSlope;

    private long normalSearchAccessCounter;
    private long normalSearchUpdateCounter;
    private double normalSearchSlopeN;
    private double normalSearchSlopeS;

    private long mergeSearchAccessCounter;
    private long mergeSearchUpdateCounter;
    private double mergeSearchSlopeN;
    private double mergeSearchSlopeS;

    private long startTime;
    private long numSearches;

    public ILSMOperationHistory() {
        this.flushHist = new ArrayList<>();
        this.lastFlush = 0;
        this.flushSem = new Semaphore(1);

        this.mergeHist = new ArrayList<>();
        this.mergeThreads = 0;
        this.mergeSem = new Semaphore(1);

        this.normalSearchHist = new ArrayList<>();
        this.normalSearchCurrentN = 0;
        this.normalSearchCurrentSize = 0;
        this.normalSearchTotal = 0;
        this.normalSearchCount = 0;
        this.mergeSearchHist = new ArrayList<>();
        this.mergeSearchCurrentN = 0;
        this.mergeSearchCurrentSize = 0;
        this.mergeSearchTotal = 0;
        this.mergeSearchCount = 0;
        this.searchSem = new Semaphore(1);

        this.mergeAccessCounter = 0;
        this.mergeUpdateCounter = 0;
        this.mergeSlope = Double.NaN;

        this.normalSearchAccessCounter = 0;
        this.normalSearchUpdateCounter = 0;
        this.normalSearchSlopeN = Double.NaN;
        this.normalSearchSlopeS = Double.NaN;

        this.mergeSearchAccessCounter = 0;
        this.mergeSearchUpdateCounter = 0;
        this.mergeSearchSlopeN = Double.NaN;
        this.mergeSearchSlopeS = Double.NaN;

        this.startTime = System.nanoTime();
        this.numSearches = 0;
    }

    public void recordFlush(long flushEnd) {
        try {
            flushSem.acquire();
            try {
                if (lastFlush == 0) {
                    lastFlush = flushEnd;
                } else if (flushEnd > lastFlush) {
                    if (flushHist.size() == 10) {
                        flushHist.remove(0);
                    }
                    flushHist.add(new Long(flushEnd - lastFlush));
                    lastFlush = flushEnd;
                } else {
                }
            } finally {
                flushSem.release();
            }
        } catch (InterruptedException ie) {
            // ...
        }
    }

    public Boolean isMerging() {
        return mergeThreads > 0;
    }

    public long startMerge() {
        try {
            mergeSem.acquire();
            try {
                mergeThreads++;
            } finally {
                mergeSem.release();
            }
        } catch (InterruptedException ie) {
            // ...
        }

        return mergeThreads;
    }

    public long finishMerge(int k, long mergedSize, long duration) {
        double d = (double) duration / 1000000.0;
        try {
            mergeSem.acquire();
            try {
                if (mergeHist.size() == 100) {
                    mergeHist.remove(0);
                }
                mergeHist.add(new Pair<>(new Pair<>(new Integer(k), new Long(mergedSize)), new Double(d)));
                if (mergeUpdateCounter == Long.MAX_VALUE)
                    mergeUpdateCounter = 0;
                else
                    mergeUpdateCounter++;
                mergeThreads--;
            } finally {
                mergeSem.release();
            }
        } catch (InterruptedException ie) {
            // ...
        }

        return mergeThreads;
    }

    private long bytesToMegaBytes(long bytes) {
        return (long) Math.ceil((double) bytes / 1048576.0);
    }

    private static Double computeAverageLatency(long count, long totalTime) {
        if (count < 1)
            return new Double(Double.POSITIVE_INFINITY);
        else
            return new Double(((double) totalTime / count) / 1000000.0); // In milliseconds
    }

    private void updateMergeSearchHistory() {
        if (mergeSearchHist.isEmpty()) {
            mergeSearchHist
                    .add(new Pair<>(new Pair<>(new Integer(mergeSearchCurrentN), new Double(mergeSearchCurrentSize)),
                            computeAverageLatency(mergeSearchCount, mergeSearchTotal)));
            return;
        }

        int idx = -1;
        for (int i = 0; i < mergeSearchHist.size(); i++) {
            Pair<Integer, Double> components = mergeSearchHist.get(i).getKey();
            if (components.getKey().intValue() == mergeSearchCurrentN
                    && components.getValue().doubleValue() == mergeSearchCurrentSize) {
                idx = i;
                break;
            }
        }

        if (idx > -1) {
            mergeSearchHist.remove(idx);
        }

        mergeSearchHist.add(new Pair<>(new Pair<>(new Integer(mergeSearchCurrentN), new Double(mergeSearchCurrentSize)),
                computeAverageLatency(mergeSearchCount, mergeSearchTotal)));

        if (mergeSearchHist.size() == 101) {
            mergeSearchHist.remove(0);
        }
    }

    private void updateNormalSearchHistory() {
        if (normalSearchHist.isEmpty()) {
            normalSearchHist
                    .add(new Pair<>(new Pair<>(new Integer(normalSearchCurrentN), new Double(normalSearchCurrentSize)),
                            computeAverageLatency(normalSearchCount, normalSearchTotal)));
            return;
        }

        int idx = -1;
        for (int i = 0; i < normalSearchHist.size(); i++) {
            Pair<Integer, Double> components = normalSearchHist.get(i).getKey();
            if (components.getKey().intValue() == normalSearchCurrentN
                    && components.getValue().doubleValue() == normalSearchCurrentSize) {
                idx = i;
                break;
            }
        }

        if (idx > -1) {
            normalSearchHist.remove(idx);
        }

        normalSearchHist
                .add(new Pair<>(new Pair<>(new Integer(normalSearchCurrentN), new Double(normalSearchCurrentSize)),
                        computeAverageLatency(normalSearchCount, normalSearchTotal)));

        if (normalSearchHist.size() == 101) {
            normalSearchHist.remove(0);
        }
    }

    public void recordPointSearch(int n, double totalSize, long duration, Boolean duringMerge, Boolean inMemory) {
        try {
            searchSem.acquire();
            try {
                if (!inMemory) {
                    if (duringMerge) {
                        if (mergeSearchCurrentN == 0) {
                            mergeSearchCurrentN = n;
                            mergeSearchCurrentSize = totalSize;
                            mergeSearchCount += 1;
                            mergeSearchTotal += duration;
                        } else if (mergeSearchCurrentN == n && normalSearchCurrentSize == totalSize) {
                            mergeSearchCount += 1;
                            mergeSearchTotal += duration;
                        } else {
                            updateMergeSearchHistory();
                            mergeSearchCurrentN = n;
                            mergeSearchCurrentSize = totalSize;
                            mergeSearchCount = 1;
                            mergeSearchTotal = duration;
                        }

                        if (mergeSearchUpdateCounter == Long.MAX_VALUE)
                            mergeSearchUpdateCounter = 0;
                        else
                            mergeSearchUpdateCounter++;
                    } else {
                        if (normalSearchCurrentN == 0) {
                            normalSearchCurrentN = n;
                            normalSearchCurrentSize = totalSize;
                            normalSearchCount += 1;
                            normalSearchTotal += duration;
                        } else if (normalSearchCurrentN == n && normalSearchCurrentSize == totalSize) {
                            normalSearchCount += 1;
                            normalSearchTotal += duration;
                        } else {
                            updateNormalSearchHistory();
                            normalSearchCurrentN = n;
                            normalSearchCurrentSize = totalSize;
                            normalSearchCount = 1;
                            normalSearchTotal = duration;
                        }

                        if (normalSearchUpdateCounter == Long.MAX_VALUE)
                            normalSearchUpdateCounter = 0;
                        else
                            normalSearchUpdateCounter++;
                    }
                }

                numSearches++;
            } finally {
                searchSem.release();
            }
        } catch (InterruptedException ie) {
            // ...
        }
    }

    private static double getTotalSize(List<Long> sizes) {
        double total = 0;
        for (Long l : sizes) {
            total += Math.ceil((double) (l.longValue()) / 1048576.0); // Size in MB
        }
        return total;
    }

    public double getMergeSlope() {
        double slope = Double.NaN;
        try {
            mergeSem.acquire();
            try {
                if (mergeAccessCounter != mergeUpdateCounter || Double.isNaN(mergeSlope)) {
                    int n = mergeHist.size();
                    if (n >= 2) {
                        double[] xs = new double[n];
                        double[] ys = new double[n];
                        double sumx = 0.0, sumy = 0.0;
                        for (int i = 0; i < n; i++) {
                            Pair<Pair<Integer, Long>, Double> p = mergeHist.get(i);
                            long mergedSize = p.getKey().getValue().longValue();
                            double duration = p.getValue().doubleValue();
                            xs[i] = mergedSize;
                            ys[i] = duration;
                            sumx += mergedSize;
                            sumy += duration;
                        }

                        double xbar = sumx / n;
                        double ybar = sumy / n;

                        double xxbar = 0.0, xybar = 0.0;
                        for (int i = 0; i < n; i++) {
                            double x = xs[i];
                            double y = ys[i];
                            xxbar += ((x - xbar) * (x - xbar));
                            xybar += ((x - xbar) * (y - ybar));
                        }
                        slope = xybar / xxbar;
                    }
                    mergeSlope = slope;
                    mergeAccessCounter = mergeUpdateCounter;
                } else {
                    if (mergeHist.size() >= 2) {
                        slope = mergeSlope;
                    }
                }
            } finally {
                mergeSem.release();
            }
        } catch (InterruptedException ie) {
            // ...
        }

        return slope;
    }

    public double getNormalSearchSlope() {
        double slope = Double.NaN;
        try {
            searchSem.acquire();
            try {
                if (normalSearchAccessCounter != normalSearchUpdateCounter || Double.isNaN(normalSearchSlopeN)) {
                    int n = normalSearchHist.size();
                    if (n >= 2) {
                        double[] xs = new double[n];
                        double[] ys = new double[n];
                        double sumx = 0.0, sumy = 0.0;
                        for (int i = 0; i < n; i++) {
                            Pair<Pair<Integer, Double>, Double> p = normalSearchHist.get(i);
                            int x = p.getKey().getKey().intValue();
                            double y = p.getValue().doubleValue();
                            xs[i] = x;
                            ys[i] = y;
                            sumx += x;
                            sumy += y;
                        }

                        double xbar = sumx / n;
                        double ybar = sumy / n;

                        double xxbar = 0.0, xybar = 0.0;
                        for (int i = 0; i < n; i++) {
                            double x = xs[i];
                            double y = ys[i];
                            xxbar += ((x - xbar) * (x - xbar));
                            xybar += ((x - xbar) * (y - ybar));
                        }
                        slope = xybar / xxbar;
                    }
                    normalSearchSlopeN = slope;
                    normalSearchAccessCounter = normalSearchUpdateCounter;
                } else {
                    if (normalSearchHist.size() >= 2) {
                        slope = normalSearchSlopeN;
                    }
                }
            } finally {
                searchSem.release();
            }
        } catch (InterruptedException ie) {
            // ...
        }

        return slope;
    }

    public double[] getNormalSearchSlopes() {
        double slopeN = Double.NaN;
        double slopeS = Double.NaN;
        try {
            searchSem.acquire();
            try {
                if (normalSearchAccessCounter != normalSearchUpdateCounter || Double.isNaN(normalSearchSlopeN)) {
                    int n = normalSearchHist.size();
                    if (n >= 2) {
                        double sumx12 = 0.0, sumx22 = 0.0, sumx1y = 0.0, sumx2y = 0.0, sumx1x2 = 0.0;
                        for (int i = 0; i < n; i++) {
                            Pair<Pair<Integer, Double>, Double> p = normalSearchHist.get(i);
                            Pair<Integer, Double> components = p.getKey();
                            int x1 = components.getKey().intValue();
                            double x2 = components.getValue().doubleValue();
                            double y = p.getValue().doubleValue();

                            sumx12 += (x1 * x1);
                            sumx22 += (x2 * x2);
                            sumx1y += (x1 * y);
                            sumx2y += (x2 * y);
                            sumx1x2 += (x1 * x2);
                        }

                        if (normalSearchCurrentN > 0) {
                            double y = computeAverageLatency(normalSearchCount, normalSearchTotal);

                            sumx12 += (normalSearchCurrentN * normalSearchCurrentN);
                            sumx22 += (normalSearchCurrentSize * normalSearchCurrentSize);
                            sumx1y += (normalSearchCurrentN * y);
                            sumx2y += (normalSearchCurrentSize * y);
                            sumx1x2 += (normalSearchCurrentN * normalSearchCurrentSize);
                        }

                        slopeN = (sumx22 * sumx1y - sumx1x2 * sumx2y) / (sumx12 * sumx22 - sumx1x2 * sumx1x2);
                        slopeS = (sumx12 * sumx2y - sumx1x2 * sumx1y) / (sumx12 * sumx22 - sumx1x2 * sumx1x2);
                    }
                    normalSearchSlopeN = slopeN;
                    normalSearchSlopeS = slopeS;
                    normalSearchAccessCounter = normalSearchUpdateCounter;
                } else {
                    if (mergeHist.size() >= 2) {
                        slopeN = normalSearchSlopeN;
                        slopeS = normalSearchSlopeS;
                    }
                }
            } finally {
                searchSem.release();
            }
        } catch (InterruptedException ie) {
            // ...
        }

        double[] ret = { slopeN, slopeS };
        return ret;
    }

    public double getMergeSearchSlope() {
        double slope = Double.NaN;
        try {
            searchSem.acquire();
            try {
                if (mergeSearchAccessCounter != mergeSearchUpdateCounter || Double.isNaN(mergeSearchSlopeN)) {
                    int n = mergeSearchHist.size();
                    if (n >= 2) {
                        double[] xs = new double[n];
                        double[] ys = new double[n];
                        double sumx = 0.0, sumy = 0.0;
                        for (int i = 0; i < n; i++) {
                            Pair<Pair<Integer, Double>, Double> p = mergeSearchHist.get(i);
                            int x = p.getKey().getKey().intValue();
                            double y = p.getValue().doubleValue();
                            xs[i] = x;
                            ys[i] = y;
                            sumx += x;
                            sumy += y;
                        }

                        double xbar = sumx / n;
                        double ybar = sumy / n;

                        double xxbar = 0.0, xybar = 0.0;
                        for (int i = 0; i < n; i++) {
                            double x = xs[i];
                            double y = ys[i];
                            xxbar += ((x - xbar) * (x - xbar));
                            xybar += ((x - xbar) * (y - ybar));
                        }
                        slope = xybar / xxbar;
                    }
                    mergeSearchSlopeN = slope;
                    mergeSearchAccessCounter = mergeSearchUpdateCounter;
                } else {
                    if (mergeSearchHist.size() >= 2) {
                        slope = mergeSearchSlopeN;
                    }
                }
            } finally {
                searchSem.release();
            }
        } catch (InterruptedException ie) {
            // ...
        }

        return slope;
    }

    public double[] getMergeSearchSlopes() {
        double slopeN = Double.NaN;
        double slopeS = Double.NaN;
        try {
            searchSem.acquire();
            try {
                if (mergeSearchAccessCounter != mergeSearchUpdateCounter || Double.isNaN(mergeSearchSlopeN)) {
                    int n = mergeSearchHist.size();
                    if (n >= 2) {
                        double sumx12 = 0.0, sumx22 = 0.0, sumx1y = 0.0, sumx2y = 0.0, sumx1x2 = 0.0;
                        for (int i = 0; i < n; i++) {
                            Pair<Pair<Integer, Double>, Double> p = mergeSearchHist.get(i);
                            Pair<Integer, Double> components = p.getKey();
                            int x1 = components.getKey().intValue();
                            double x2 = components.getValue().doubleValue();
                            double y = p.getValue().doubleValue();

                            sumx12 += (x1 * x1);
                            sumx22 += (x2 * x2);
                            sumx1y += (x1 * y);
                            sumx2y += (x2 * y);
                            sumx1x2 += (x1 * x2);
                        }

                        if (mergeSearchCurrentN > 0) {
                            double y = computeAverageLatency(mergeSearchCount, mergeSearchTotal);

                            sumx12 += (mergeSearchCurrentN * mergeSearchCurrentN);
                            sumx22 += (mergeSearchCurrentSize * mergeSearchCurrentSize);
                            sumx1y += (mergeSearchCurrentN * y);
                            sumx2y += (mergeSearchCurrentSize * y);
                            sumx1x2 += (mergeSearchCurrentN * mergeSearchCurrentSize);
                        }

                        slopeN = (sumx22 * sumx1y - sumx1x2 * sumx2y) / (sumx12 * sumx22 - sumx1x2 * sumx1x2);
                        slopeS = (sumx12 * sumx2y - sumx1x2 * sumx1y) / (sumx12 * sumx22 - sumx1x2 * sumx1x2);
                    }
                    mergeSearchSlopeN = slopeN;
                    mergeSearchSlopeS = slopeS;
                    mergeSearchAccessCounter = mergeSearchUpdateCounter;
                } else {
                    if (mergeHist.size() >= 2) {
                        slopeN = mergeSearchSlopeN;
                        slopeS = mergeSearchSlopeS;
                    }
                }
            } finally {
                searchSem.release();
            }
        } catch (InterruptedException ie) {
            // ...
        }

        double[] ret = { slopeN, slopeS };
        return ret;
    }

    public double getSearchRate() {
        return (double) numSearches * 1000000.0 / (System.nanoTime() - startTime);
    }

    public String mergeHistoryToString() {
        String ret = "";
        for (int i = 0; i < mergeHist.size(); i++) {
            Pair<Pair<Integer, Long>, Double> p = mergeHist.get(i);
            Pair<Integer, Long> components = p.getKey();
            if (i == 0) {
                ret = "(" + components.getKey() + "," + components.getValue() + ")=" + p.getValue();
            } else {
                ret += (";(" + components.getKey() + "," + components.getValue() + ")=" + p.getValue());
            }
        }
        return "[" + ret + "]";
    }

    private static String searchHistToString(List<Pair<Pair<Integer, Double>, Double>> hist, int n, double size,
            long total, long count) {
        String ret = "";
        for (int i = 0; i < hist.size(); i++) {
            Pair<Pair<Integer, Double>, Double> p = hist.get(i);
            Pair<Integer, Double> components = p.getKey();
            if (i == 0) {
                ret = "(" + components.getKey() + "," + components.getValue() + ")=" + p.getValue();
            } else {
                ret += (";(" + components.getKey() + "," + components.getValue() + ")=" + p.getValue());
            }
        }

        if (n > 0) {
            if (ret.isEmpty()) {
                ret = "(" + n + "," + size + ")=" + computeAverageLatency(count, total);
            } else {
                ret += (";(" + n + "," + size + ")=" + computeAverageLatency(count, total));
            }
        }

        return "[" + ret + "]";
    }

    public String normalSearchHistoryToString() {
        return searchHistToString(normalSearchHist, normalSearchCurrentN, normalSearchCurrentSize, normalSearchTotal,
                normalSearchCount);
    }

    public String mergeSearchHistoryToString() {
        return searchHistToString(mergeSearchHist, mergeSearchCurrentN, mergeSearchCurrentSize, mergeSearchTotal,
                mergeSearchCount);
    }
}
