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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLevelMergePolicyHelper;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.lsm.rtree.tuples.LSMRTreeTupleReferenceForPointMBR;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;

public class LSMRTreeLevelMergePolicyHelper extends AbstractLevelMergePolicyHelper {
    protected final AbstractLSMRTree lsmRTree;
    private Map<Long, Long> lastCurveValue;
    private final String partition;

    public LSMRTreeLevelMergePolicyHelper(AbstractLSMIndex index, String partition) {
        super(index);
        lsmRTree = (AbstractLSMRTree) index;
        this.partition = partition;
        lastCurveValue = new HashMap<>();
    }

    public static double[] clone(double[] src) {
        if (src == null) {
            return null;
        }
        double[] dst = new double[src.length];
        System.arraycopy(src, 0, dst, 0, src.length);
        return dst;
    }

    public static double[] clone(double[] src, int start, int len) {
        if (src == null || start < 0 || start >= src.length || len < 1 || start + len > src.length) {
            return null;
        }
        double[] dst = new double[len];
        System.arraycopy(src, start, dst, 0, len);
        return dst;
    }

    public static boolean isOverlapping(double[] min1, double[] max1, double[] min2, double[] max2) {
        if (min1 == null || max1 == null || min2 == null || max2 == null) {
            return true;
        }
        for (int i = 0; i < min1.length; i++) {
            if (Double.compare(min1[i], max2[i]) > 0 || Double.compare(min2[i], max1[i]) > 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean isOverlapping(double[] mbr1, double[] mbr2) {
        if (mbr1 == null || mbr2 == null) {
            return true;
        }
        int dim = mbr1.length / 2;
        for (int i = 0; i < dim; i++) {
            if (Double.compare(mbr1[i], mbr2[dim + i]) > 0 || Double.compare(mbr2[i], mbr1[dim + i]) > 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean isOverlapping(double[] min, double[] max, double[] mbr) {
        if (min == null || max == null || mbr == null) {
            return true;
        }
        int dim = min.length;
        for (int i = 0; i < dim; i++) {
            if (Double.compare(min[i], mbr[dim + i]) > 0 || Double.compare(mbr[i], max[i]) > 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<ILSMDiskComponent> getBestComponents(List<ILSMDiskComponent> components, long level, boolean absolute) {
        ILSMDiskComponent picked = pickNextZCurveComponent(components, level);
        List<ILSMDiskComponent> toMerge = new ArrayList<>(getOverlappingComponents(picked, components, absolute));
        toMerge.add(0, picked);
        return toMerge;
    }

    @Override
    public List<ILSMDiskComponent> getOverlappingComponents(ILSMDiskComponent component,
            List<ILSMDiskComponent> components, boolean absolute) {
        long levelTo = component.getMinId() + 1;
        List<ILSMDiskComponent> nextLevelComponents = getComponents(components, levelTo);
        if (nextLevelComponents.isEmpty()) {
            return Collections.emptyList();
        }
        double[] minMBR = null;
        double[] maxMBR = null;

        try {
            minMBR = LSMRTree.bytesToDoubles(component.getMinKey());
            maxMBR = LSMRTree.bytesToDoubles(component.getMaxKey());
        } catch (HyracksDataException ex) {
            return nextLevelComponents;
        }

        int dim = minMBR.length;
        List<ILSMDiskComponent> overlapped = new ArrayList<>();
        if (absolute) {
            while (true) {
                boolean added = false;
                for (ILSMDiskComponent c : nextLevelComponents) {
                    if (!overlapped.contains(c)) {
                        try {
                            double[] cMinMBR = LSMRTree.bytesToDoubles(c.getMinKey());
                            double[] cMaxMBR = LSMRTree.bytesToDoubles(c.getMaxKey());
                            if (isOverlapping(minMBR, maxMBR, cMinMBR, cMaxMBR)) {
                                overlapped.add(c);
                                for (int i = 0; i < dim; i++) {
                                    if (Double.compare(cMinMBR[i], minMBR[i]) < 0) {
                                        minMBR[i] = cMinMBR[i];
                                    }
                                    if (Double.compare(cMaxMBR[i], maxMBR[i]) > 0) {
                                        maxMBR[i] = cMaxMBR[i];
                                    }
                                }
                                added = true;
                            }
                        } catch (HyracksDataException ex) {
                            overlapped.add(c);
                        }
                    }
                }
                if (!added) {
                    break;
                }
            }
            /*overlapped.sort(new Comparator<ILSMDiskComponent>() {
                @Override
                public int compare(ILSMDiskComponent c1, ILSMDiskComponent c2) {
                    return Long.compare(c2.getLevelSequence(), c1.getLevelSequence());
                }
            });*/
        } else {
            for (ILSMDiskComponent c : nextLevelComponents) {
                try {
                    double[] cMinMBR = LSMRTree.bytesToDoubles(c.getMinKey());
                    double[] cMaxMBR = LSMRTree.bytesToDoubles(c.getMaxKey());
                    if (isOverlapping(minMBR, maxMBR, cMinMBR, cMaxMBR)) {
                        overlapped.add(c);
                    }
                } catch (HyracksDataException ex) {
                    overlapped.add(c);
                }
            }
        }
        return overlapped;
    }

    private ILSMDiskComponent pickNextZCurveComponent(List<ILSMDiskComponent> components, long level) {
        try {
            List<ILSMDiskComponent> selectedComponents = getComponents(components, level);
            ZCurvePartitioner curve = new ZCurvePartitioner(selectedComponents);
            if (lastCurveValue.containsKey(level)) {
                long lastZ = lastCurveValue.get(level);
                long minZ = -1L;
                long nextZ = -1L;
                ILSMDiskComponent minPicked = null;
                ILSMDiskComponent nextPicked = null;
                for (ILSMDiskComponent c : selectedComponents) {
                    long z = curve.getValue(c);
                    if (minPicked == null) {
                        minPicked = c;
                        minZ = z;
                    } else {
                        if (z > minZ) {
                            minZ = z;
                            minPicked = c;
                        }
                        if (z == minZ) {
                            if (c.getMaxId() < minPicked.getMaxId()) {
                                minPicked = c;
                            }
                        }
                    }
                    if (z > lastZ) {
                        if (nextPicked == null) {
                            nextPicked = c;
                            nextZ = z;
                        } else {
                            if (z < nextZ) {
                                nextZ = z;
                                nextPicked = c;
                            }
                            if (z == nextZ) {
                                if (c.getMaxId() < nextPicked.getMaxId()) {
                                    nextPicked = c;
                                }
                            }
                        }
                    }
                }
                if (nextPicked == null) {
                    lastCurveValue.put(level, minZ);
                    return minPicked;
                } else {
                    lastCurveValue.put(level, nextZ);
                    return nextPicked;
                }
            } else {
                long minZ = -1L;
                ILSMDiskComponent picked = null;
                for (ILSMDiskComponent c : selectedComponents) {
                    long z = curve.getValue(c);
                    if (picked == null) {
                        picked = c;
                        minZ = z;
                    } else {
                        if (z > minZ) {
                            minZ = z;
                            picked = c;
                        }
                        if (z == minZ) {
                            if (c.getMaxId() < picked.getMaxId()) {
                                picked = c;
                            }
                        }
                    }
                }
                lastCurveValue.put(level, minZ);
                return picked;
            }
        } catch (HyracksDataException ex) {
            return getOldestComponent(components, level);
        }
    }

    private List<ILSMDiskComponent> doDefaultMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMRTreeMergeOperation mergeOp = (LSMRTreeMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        ILSMIndexOperationContext opCtx = ((LSMIndexSearchCursor) cursor).getOpCtx();
        lsmRTree.search(opCtx, cursor, rtreeSearchPred);

        List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();
        long levelTo = ((ILSMDiskComponent) mergedComponents.get(0)).getMinId() + 1;
        if (mergedComponents.size() == 1) {
            LSMComponentFileReferences refs = lsmRTree.getNextMergeFileReferencesAtLevel(levelTo);
            ILSMDiskComponent newComponent =
                    lsmRTree.createDiskComponent(refs.getInsertIndexFileReference(), null, null, true);
            IPageWriteCallback pageWriteCallback = lsmRTree.getPageWriteCallbackFactory().createPageWriteCallback();
            ILSMDiskComponentBulkLoader componentBulkLoader =
                    newComponent.createBulkLoader(operation, 1.0f, false, 0L, false, false, false, pageWriteCallback);
            double[] minMBR = null;
            double[] maxMBR = null;
            long totalTuples = 0L;
            try {
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference frameTuple = cursor.getTuple();
                    componentBulkLoader.add(frameTuple);
                    totalTuples++;
                    double[] mbr = lsmRTree.getMBRFromTuple(frameTuple);
                    int dim = mbr.length / 2;
                    if (minMBR == null) {
                        minMBR = clone(mbr, 0, dim);
                    } else {
                        for (int i = 0; i < dim; i++) {
                            if (Double.compare(mbr[i], minMBR[i]) < 0) {
                                minMBR[i] = mbr[i];
                            }
                        }
                    }
                    if (maxMBR == null) {
                        maxMBR = clone(mbr, dim, dim);
                    } else {
                        for (int i = 0; i < dim; i++) {
                            if (Double.compare(mbr[dim + i], maxMBR[i]) > 0) {
                                maxMBR[i] = mbr[dim + i];
                            }
                        }
                    }
                }
            } finally {
                cursor.close();
            }
            newComponent.setMinKey(AbstractLSMRTree.doublesToBytes(minMBR));
            newComponent.setMaxKey(AbstractLSMRTree.doublesToBytes(maxMBR));
            newComponent.setTupleCount(totalTuples);
            if (newComponent.getLSMComponentFilter() != null) {
                ITupleReference minTuple = mergedComponents.get(0).getLSMComponentFilter().getMinTuple();
                ITupleReference maxTuple = mergedComponents.get(0).getLSMComponentFilter().getMaxTuple();
                List<ITupleReference> filterTuples = Arrays.asList(minTuple, maxTuple);
                lsmRTree.getFilterManager().updateFilter(newComponent.getLSMComponentFilter(), filterTuples,
                        NoOpOperationCallback.INSTANCE);
                lsmRTree.getFilterManager().writeFilter(newComponent.getLSMComponentFilter(),
                        newComponent.getMetadataHolder());
            }
            componentBulkLoader.end();
            mergeOp.setTarget(refs.getInsertIndexFileReference());
            mergeOp.setBloomFilterTarget(refs.getBloomFilterFileReference());
            return Collections.singletonList(newComponent);
        } else {
            List<ILSMDiskComponent> newComponents = new ArrayList<>();
            List<ILSMDiskComponentBulkLoader> componentBulkLoaders = new ArrayList<>();
            List<ITupleReference> minTuples = new ArrayList<>();
            List<ITupleReference> maxTuples = new ArrayList<>();
            List<FileReference> mergeFileTargets = new ArrayList<>();
            List<FileReference> mergeBloomFilterTargets = new ArrayList<>();
            double[] minMBR = null;
            double[] maxMBR = null;
            long totalTuples = 0L;
            ITupleReference minTuple = null;
            ITupleReference maxTuple = null;
            MultiComparator filterCmp = null;
            long numTuplesInPartition = lsmRTree.getMaxNumTuplesPerComponent();
            try {
                ILSMDiskComponent newComponent = null;
                ILSMDiskComponentBulkLoader componentBulkLoader = null;
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference frameTuple = cursor.getTuple();
                    double[] mbr = lsmRTree.getMBRFromTuple(frameTuple);
                    int dim = mbr.length / 2;
                    if (minMBR == null) {
                        minMBR = clone(mbr, 0, dim);
                    } else {
                        for (int i = 0; i < dim; i++) {
                            if (Double.compare(mbr[i], minMBR[i]) < 0) {
                                minMBR[i] = mbr[i];
                            }
                        }
                    }
                    if (maxMBR == null) {
                        maxMBR = clone(mbr, dim, dim);
                    } else {
                        for (int i = 0; i < dim; i++) {
                            if (Double.compare(mbr[dim + i], maxMBR[i]) > 0) {
                                maxMBR[i] = mbr[dim + i];
                            }
                        }
                    }
                    if (newComponent == null) {
                        LSMComponentFileReferences refs = lsmRTree.getNextMergeFileReferencesAtLevel(levelTo);
                        mergeFileTargets.add(refs.getInsertIndexFileReference());
                        mergeBloomFilterTargets.add(refs.getBloomFilterFileReference());
                        newComponent = lsmRTree.createDiskComponent(refs.getInsertIndexFileReference(), null,
                                refs.getBloomFilterFileReference(), true);
                        IPageWriteCallback pageWriteCallback =
                                lsmRTree.getPageWriteCallbackFactory().createPageWriteCallback();
                        componentBulkLoader = newComponent.createBulkLoader(operation, 1.0f, false, 0L, false, false,
                                false, pageWriteCallback);
                        newComponents.add(newComponent);
                        componentBulkLoaders.add(componentBulkLoader);
                        filterCmp = newComponent.getLSMComponentFilter() == null ? null
                                : MultiComparator.create(newComponent.getLSMComponentFilter().getFilterCmpFactories());
                        minTuple = null;
                        maxTuple = null;
                        totalTuples = 0L;
                    }
                    if (minTuple == null) {
                        minTuple = frameTuple;
                    } else {
                        if (filterCmp != null && filterCmp.compare(frameTuple, minTuple) < 0) {
                            minTuple = frameTuple;
                        }
                    }
                    if (maxTuple == null) {
                        maxTuple = frameTuple;
                    } else {
                        if (filterCmp != null && filterCmp.compare(frameTuple, maxTuple) > 0) {
                            maxTuple = frameTuple;
                        }
                    }
                    componentBulkLoader.add(frameTuple);
                    totalTuples++;
                    if (totalTuples++ >= numTuplesInPartition) {
                        newComponent.setMinKey(AbstractLSMRTree.doublesToBytes(minMBR));
                        newComponent.setMaxKey(AbstractLSMRTree.doublesToBytes(maxMBR));
                        newComponent.setTupleCount(totalTuples);
                        minTuples.add(minTuple);
                        maxTuples.add(maxTuple);
                        newComponent = null;
                        componentBulkLoader = null;
                        minTuple = null;
                        maxTuple = null;
                        filterCmp = null;
                        minMBR = null;
                        maxMBR = null;
                    }
                }
                if (newComponent != null) {
                    newComponent.setMinKey(AbstractLSMRTree.doublesToBytes(minMBR));
                    newComponent.setMaxKey(AbstractLSMRTree.doublesToBytes(maxMBR));
                    newComponent.setTupleCount(totalTuples);
                    minTuples.add(minTuple);
                    maxTuples.add(maxTuple);
                }
                mergeOp.setTargets(mergeFileTargets);
                mergeOp.setBloomFilterTargets(mergeBloomFilterTargets);
            } finally {
                cursor.close();
            }
            for (int i = 0; i < newComponents.size(); i++) {
                ILSMDiskComponent newComponent = newComponents.get(i);
                if (newComponent.getLSMComponentFilter() != null) {
                    List<ITupleReference> filterTuples = Arrays.asList(minTuples.get(i), maxTuples.get(i));
                    lsmRTree.getFilterManager().updateFilter(newComponent.getLSMComponentFilter(), filterTuples,
                            NoOpOperationCallback.INSTANCE);
                    lsmRTree.getFilterManager().writeFilter(newComponent.getLSMComponentFilter(),
                            newComponent.getMetadataHolder());
                }
            }
            for (ILSMDiskComponentBulkLoader componentBulkLoader : componentBulkLoaders) {
                componentBulkLoader.end();
            }
            return newComponents;
        }
    }

    private List<ILSMDiskComponent> doSTROrderMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMRTreeMergeOperation mergeOp = (LSMRTreeMergeOperation) operation;
        LSMRTreeWithAntiMatterTuplesSearchCursor cursor =
                (LSMRTreeWithAntiMatterTuplesSearchCursor) (mergeOp.getCursor());
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        ILSMIndexOperationContext opCtx = cursor.getOpCtx();
        lsmRTree.search(opCtx, cursor, rtreeSearchPred);

        List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();
        long levelTo = ((ILSMDiskComponent) mergedComponents.get(0)).getMinId() + 1;
        if (mergedComponents.size() == 1) {
            LSMComponentFileReferences refs = lsmRTree.getNextMergeFileReferencesAtLevel(levelTo);
            ILSMDiskComponent newComponent =
                    lsmRTree.createDiskComponent(refs.getInsertIndexFileReference(), null, null, true);
            IPageWriteCallback pageWriteCallback = lsmRTree.getPageWriteCallbackFactory().createPageWriteCallback();
            ILSMDiskComponentBulkLoader componentBulkLoader =
                    newComponent.createBulkLoader(operation, 1.0f, false, 0L, false, false, false, pageWriteCallback);
            double[] minMBR = null;
            double[] maxMBR = null;
            long totalTuples = 0L;
            try {
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference frameTuple = cursor.getTuple();
                    componentBulkLoader.add(frameTuple);
                    totalTuples++;
                    double[] mbr = lsmRTree.getMBRFromTuple(frameTuple);
                    int dim = mbr.length / 2;
                    if (minMBR == null) {
                        minMBR = clone(mbr, 0, dim);
                    } else {
                        for (int i = 0; i < dim; i++) {
                            if (Double.compare(mbr[i], minMBR[i]) < 0) {
                                minMBR[i] = mbr[i];
                            }
                        }
                    }
                    if (maxMBR == null) {
                        maxMBR = clone(mbr, dim, dim);
                    } else {
                        for (int i = 0; i < dim; i++) {
                            if (Double.compare(mbr[dim + i], maxMBR[i]) > 0) {
                                maxMBR[i] = mbr[dim + i];
                            }
                        }
                    }
                }
            } finally {
                cursor.close();
            }
            newComponent.setMinKey(AbstractLSMRTree.doublesToBytes(minMBR));
            newComponent.setMaxKey(AbstractLSMRTree.doublesToBytes(maxMBR));
            newComponent.setTupleCount(totalTuples);
            if (newComponent.getLSMComponentFilter() != null) {
                ITupleReference minTuple = mergedComponents.get(0).getLSMComponentFilter().getMinTuple();
                ITupleReference maxTuple = mergedComponents.get(0).getLSMComponentFilter().getMaxTuple();
                List<ITupleReference> filterTuples = Arrays.asList(minTuple, maxTuple);
                lsmRTree.getFilterManager().updateFilter(newComponent.getLSMComponentFilter(), filterTuples,
                        NoOpOperationCallback.INSTANCE);
                lsmRTree.getFilterManager().writeFilter(newComponent.getLSMComponentFilter(),
                        newComponent.getMetadataHolder());
            }
            componentBulkLoader.end();
            mergeOp.setTarget(refs.getInsertIndexFileReference());
            mergeOp.setBloomFilterTarget(refs.getBloomFilterFileReference());
            return Collections.singletonList(newComponent);
        } else {
            List<ILSMDiskComponent> newComponents = new ArrayList<>();
            List<ILSMDiskComponentBulkLoader> componentBulkLoaders = new ArrayList<>();
            List<FileReference> mergeFileTargets = new ArrayList<>();
            List<FileReference> mergeBloomFilterTargets = new ArrayList<>();
            List<TupleWithMBR> allTuples = new ArrayList<>();
            long numTuplesInPartition = lsmRTree.getMaxNumTuplesPerComponent();
            try {
                while (cursor.hasNext()) {
                    cursor.next();
                    LSMRTreeTupleReferenceForPointMBR srcTuple = (LSMRTreeTupleReferenceForPointMBR) cursor.getTuple();
                    double[] srcMBR = lsmRTree.getMBRFromTuple(srcTuple);
                    LSMRTreeTupleReferenceForPointMBR dstTuple0 = srcTuple.getCopy(0);
                    LSMRTreeTupleReferenceForPointMBR dstTuple1 = srcTuple.getCopy(1);
                    LSMRTreeTupleReferenceForPointMBR dstTuple2 = srcTuple.getCopy(2);
                    double[] dstMBR0 = lsmRTree.getMBRFromTuple(dstTuple0);
                    double[] dstMBR1 = lsmRTree.getMBRFromTuple(dstTuple1);
                    double[] dstMBR2 = lsmRTree.getMBRFromTuple(dstTuple2);
                    allTuples.add(new TupleWithMBR(dstTuple0, dstMBR0));
                    for (int i = 0; i < srcMBR.length; i++) {
                        if (srcMBR[i] != dstMBR0[i]) {
                            lsmRTree.writeLog("[ERR-COPY]\tsrc=" + mbr2str(srcMBR) + ", dst0=" + mbr2str(dstMBR0));
                            break;
                        }
                    }
                    for (int i = 0; i < srcMBR.length; i++) {
                        if (srcMBR[i] != dstMBR1[i]) {
                            lsmRTree.writeLog("[ERR-COPY]\tsrc=" + mbr2str(srcMBR) + ", dst1=" + mbr2str(dstMBR1));
                            break;
                        }
                    }
                    for (int i = 0; i < srcMBR.length; i++) {
                        if (srcMBR[i] != dstMBR2[i]) {
                            lsmRTree.writeLog("[ERR-COPY]\tsrc=" + mbr2str(srcMBR) + ", dst2=" + mbr2str(dstMBR2));
                            break;
                        }
                    }
                }
                List<List<TupleWithMBR>> partitions = partitionTuplesBySTR(allTuples, numTuplesInPartition);
                for (List<TupleWithMBR> partition : partitions) {
                    partition.sort(new Comparator<TupleWithMBR>() {
                        @Override
                        public int compare(TupleWithMBR t1, TupleWithMBR t2) {
                            try {
                                return cursor.compare(t1.getTuple(), t2.getTuple());
                            } catch (HyracksDataException ex) {
                                return -1;
                            }
                        }
                    });
                    int dim = partition.get(0).getDim();
                    LSMComponentFileReferences refs = lsmRTree.getNextMergeFileReferencesAtLevel(levelTo);
                    mergeFileTargets.add(refs.getInsertIndexFileReference());
                    mergeBloomFilterTargets.add(refs.getBloomFilterFileReference());
                    ILSMDiskComponent newComponent = lsmRTree.createDiskComponent(refs.getInsertIndexFileReference(),
                            null, refs.getBloomFilterFileReference(), true);
                    newComponents.add(newComponent);
                    IPageWriteCallback pageWriteCallback =
                            lsmRTree.getPageWriteCallbackFactory().createPageWriteCallback();
                    ILSMDiskComponentBulkLoader componentBulkLoader = newComponent.createBulkLoader(operation, 1.0f,
                            false, 0L, false, false, false, pageWriteCallback);
                    componentBulkLoaders.add(componentBulkLoader);
                    MultiComparator filterCmp = newComponent.getLSMComponentFilter() == null ? null
                            : MultiComparator.create(newComponent.getLSMComponentFilter().getFilterCmpFactories());
                    ITupleReference minTuple = null;
                    ITupleReference maxTuple = null;
                    double[] minMBR = null;
                    double[] maxMBR = null;
                    for (TupleWithMBR tupleWithMBR : partition) {
                        ITupleReference tuple = tupleWithMBR.getTuple();
                        if (minTuple == null || (filterCmp != null && filterCmp.compare(tuple, minTuple) < 0)) {
                            minTuple = tuple;
                        }
                        if (maxTuple == null || (filterCmp != null && filterCmp.compare(tuple, maxTuple) > 0)) {
                            maxTuple = tuple;
                        }
                        componentBulkLoader.add(tuple);
                        double[] mbr = tupleWithMBR.getMBR();
                        if (minMBR == null) {
                            minMBR = clone(mbr, 0, dim);
                        } else {
                            for (int k = 0; k < dim; k++) {
                                if (Double.compare(mbr[k], minMBR[k]) < 0) {
                                    minMBR[k] = mbr[k];
                                }
                            }
                        }
                        if (maxMBR == null) {
                            maxMBR = clone(mbr, dim, dim);
                        } else {
                            for (int k = 0; k < dim; k++) {
                                if (Double.compare(mbr[dim + k], maxMBR[k]) > 0) {
                                    maxMBR[k] = mbr[dim + k];
                                }
                            }
                        }
                    }
                    newComponent.setMinKey(AbstractLSMRTree.doublesToBytes(minMBR));
                    newComponent.setMaxKey(AbstractLSMRTree.doublesToBytes(maxMBR));
                    newComponent.setTupleCount(partition.size());
                    if (filterCmp != null) {
                        List<ITupleReference> filterTuples = Arrays.asList(minTuple, maxTuple);
                        lsmRTree.getFilterManager().updateFilter(newComponent.getLSMComponentFilter(), filterTuples,
                                NoOpOperationCallback.INSTANCE);
                        lsmRTree.getFilterManager().writeFilter(newComponent.getLSMComponentFilter(),
                                newComponent.getMetadataHolder());
                    }
                }
            } finally {
                cursor.close();
            }
            for (ILSMDiskComponentBulkLoader componentBulkLoader : componentBulkLoaders) {
                componentBulkLoader.end();
            }
            mergeOp.setTargets(mergeFileTargets);
            mergeOp.setBloomFilterTargets(mergeBloomFilterTargets);
            return newComponents;
        }
    }

    @Override
    public List<ILSMDiskComponent> merge(ILSMIOOperation operation) throws HyracksDataException {
        return partition.compareTo(LevelRTreeMergePolicyFactory.PARTITION_STR) == 0 ? doSTROrderMerge(operation)
                : doDefaultMerge(operation);
    }

    public void orderTuplesBySTR(List<TupleWithMBR> tuplesToPartition, List<List<TupleWithMBR>> partitions,
            long numTuplesInPartition, int startDim) {
        if (tuplesToPartition == null || tuplesToPartition.isEmpty()) {
            return;
        }
        int numPartitions = partitions.size();
        int dim = tuplesToPartition.get(0).getDim();
        // Handle the last dimension, place tuples into partitions
        if (dim < 2 || startDim >= dim) {
            for (int i = 0; i < numPartitions; i++) {
                List<TupleWithMBR> p = partitions.get(i);
                long empties = numTuplesInPartition - p.size(); // Number of available space in the partition
                if (empties > 0) {
                    if (empties >= tuplesToPartition.size()) {
                        // Place all tuples in the slice to the partition
                        p.addAll(tuplesToPartition);
                        return;
                    } else {
                        // Place some tuples in the slice to the partition, then check the next partition
                        for (int j = 0; j < empties; j++) {
                            p.add(tuplesToPartition.get(0));
                            tuplesToPartition.remove(0);
                        }
                    }
                }
            }
            return;
        }

        int thisDim = dim - startDim; // The number of dimensions to check

        int numSlices = (int) Math.ceil(Math.pow(numPartitions, 1.0 / thisDim));
        int sliceCapacity =
                (int) numTuplesInPartition * (int) Math.ceil(Math.pow(numPartitions, (double) (thisDim - 1) / thisDim));
        for (int i = 0; i < numSlices; i++) {
            // Place tuples sorted by startDim-1 into slices
            List<TupleWithMBR> slice = new ArrayList<>();
            int bound = (i + 1) * sliceCapacity <= tuplesToPartition.size() ? sliceCapacity
                    : tuplesToPartition.size() - i * sliceCapacity;
            for (int j = 0; j < bound; j++) {
                TupleWithMBR t = tuplesToPartition.get(i * sliceCapacity + j);
                slice.add(t);
            }
            // Sort tuples in the slice by startDim
            slice.sort(new Comparator<TupleWithMBR>() {
                @Override
                public int compare(TupleWithMBR t1, TupleWithMBR t2) {
                    double[] c1 = t1.getCenter();
                    double[] c2 = t2.getCenter();
                    return Double.compare(c1[startDim], c2[startDim]);
                }
            });
            // Recursively process the slice using the next dimension
            orderTuplesBySTR(slice, partitions, numTuplesInPartition, startDim + 1);
        }
    }

    public List<List<TupleWithMBR>> partitionTuplesBySTR(List<TupleWithMBR> tuplesToPartition,
            long numTuplesInPartition) {
        if (tuplesToPartition == null || tuplesToPartition.isEmpty()) {
            return null;
        }
        int numPartitions = (int) Math.ceil((double) tuplesToPartition.size() / numTuplesInPartition);
        if (numPartitions == 1) {
            return Collections.singletonList(tuplesToPartition);
        }

        tuplesToPartition.sort(new Comparator<TupleWithMBR>() {
            @Override
            public int compare(TupleWithMBR t1, TupleWithMBR t2) {
                double[] c1 = t1.getCenter();
                double[] c2 = t2.getCenter();
                return Double.compare(c1[0], c2[0]);
            }
        });

        List<List<TupleWithMBR>> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new ArrayList<>());
        }
        orderTuplesBySTR(tuplesToPartition, partitions, numTuplesInPartition, 0);
        return partitions;
    }

    private static class TupleWithMBR {
        private final ITupleReference tuple;
        private final double[] mbr;
        private final int dim;
        private double[] center;

        public TupleWithMBR(ITupleReference tuple, double[] mbr) {
            this.tuple = tuple;
            this.mbr = mbr.clone();
            dim = mbr.length / 2;
            center = new double[dim];
            for (int i = 0; i < dim; i++) {
                center[i] = (mbr[i] + mbr[i + dim]) / 2;
            }
        }

        public ITupleReference getTuple() {
            return this.tuple;
        }

        public double[] getMBR() {
            return this.mbr;
        }

        public int getDim() {
            return this.dim;
        }

        public double[] getCenter() {
            return this.center;
        }
    }

    private static String mbr2str(double[] mbr) {
        int dim = mbr.length / 2;
        String[] mins = new String[dim];
        String[] maxs = new String[dim];
        for (int i = 0; i < dim; i++) {
            mins[i] = Double.toString(mbr[i]);
            maxs[i] = Double.toString(mbr[dim + i]);
        }
        return "[" + String.join(",", mins) + " " + String.join(",", maxs) + "]";
    }
}
