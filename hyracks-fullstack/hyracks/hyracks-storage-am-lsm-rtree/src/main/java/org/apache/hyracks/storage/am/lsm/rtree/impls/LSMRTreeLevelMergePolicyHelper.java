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
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

public class LSMRTreeLevelMergePolicyHelper extends AbstractLevelMergePolicyHelper {
    protected final AbstractLSMRTree lsmRTree;
    private Map<Long, Integer> lastCurveValue;

    public LSMRTreeLevelMergePolicyHelper(AbstractLSMIndex index) {
        super(index);
        lsmRTree = (AbstractLSMRTree) index;
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

    @Override
    public ILSMDiskComponent getBestComponent(List<ILSMDiskComponent> components, long level) {
        return pickNextZCurveComponent(components, level);
    }

    @Override
    public List<ILSMDiskComponent> getOverlappingComponents(ILSMDiskComponent component,
            List<ILSMDiskComponent> components) {
        long levelTo = component.getLevel() + 1;
        Map<Long, ILSMDiskComponent> map = new HashMap<>();
        for (ILSMDiskComponent c : components) {
            if (c.getLevel() == levelTo) {
                map.put(c.getLevelSequence(), c);
            }
        }
        if (map.isEmpty()) {
            return Collections.emptyList();
        }
        List<Long> seqs = new ArrayList<>(map.keySet());
        seqs.sort(Collections.reverseOrder());
        List<ILSMDiskComponent> overlapped = new ArrayList<>();

        double[] minMBR = null;
        double[] maxMBR = null;

        try {
            minMBR = LSMRTree.bytesToDoubles(component.getMinKey());
            maxMBR = LSMRTree.bytesToDoubles(component.getMaxKey());
        } catch (HyracksDataException ex) {
            for (long levelSeq : seqs) {
                ILSMDiskComponent c = map.get(levelSeq);
                overlapped.add(c);
            }
            return overlapped;
        }

        for (long levelSeq : seqs) {
            ILSMDiskComponent c = map.get(levelSeq);
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
        return overlapped;
    }

    private ILSMDiskComponent pickNextZCurveComponent(List<ILSMDiskComponent> components, long level) {
        try {
            List<ILSMDiskComponent> selectedComponents = getComponents(components, level);
            ZCurvePartitioner curve = new ZCurvePartitioner(selectedComponents);
            if (lastCurveValue.containsKey(level)) {
                int lastZ = lastCurveValue.get(level);
                int minZ = -1;
                int nextZ = -1;
                ILSMDiskComponent minPicked = null;
                ILSMDiskComponent nextPicked = null;
                for (ILSMDiskComponent c : selectedComponents) {
                    int z = curve.getValue(c);
                    if (minPicked == null) {
                        minPicked = c;
                        minZ = z;
                    } else {
                        if (z > minZ) {
                            minZ = z;
                            minPicked = c;
                        }
                        if (z == minZ) {
                            if (c.getLevelSequence() < minPicked.getLevelSequence()) {
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
                                if (c.getLevelSequence() < nextPicked.getLevelSequence()) {
                                    nextPicked = c;
                                }
                            }
                        }
                    }
                }
                return nextPicked == null ? minPicked : nextPicked;
            } else {
                int minZ = -1;
                ILSMDiskComponent picked = null;
                for (ILSMDiskComponent c : selectedComponents) {
                    int z = curve.getValue(c);
                    if (picked == null) {
                        picked = c;
                        minZ = z;
                    } else {
                        if (z > minZ) {
                            minZ = z;
                            picked = c;
                        }
                        if (z == minZ) {
                            if (c.getLevelSequence() < picked.getLevelSequence()) {
                                picked = c;
                            }
                        }
                    }
                }
                return picked;
            }
        } catch (HyracksDataException ex) {
            return getOldestComponent(components, level);
        }
    }

    private List<ILSMDiskComponent> doZOrderMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMRTreeMergeOperation mergeOp = (LSMRTreeMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        ILSMIndexOperationContext opCtx = ((LSMIndexSearchCursor) cursor).getOpCtx();
        lsmRTree.search(opCtx, cursor, rtreeSearchPred);

        List<ILSMDiskComponent> newComponents = new ArrayList<>();
        List<ILSMDiskComponentBulkLoader> componentBulkLoaders = new ArrayList<>();
        List<ITupleReference> minTuples = new ArrayList<>();
        List<ITupleReference> maxTuples = new ArrayList<>();

        List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();

        if (mergedComponents.size() == 1) {
            ILSMDiskComponent newComponent = null;
            ILSMDiskComponentBulkLoader componentBulkLoader = null;
            LSMComponentFileReferences refs = lsmRTree
                    .getNextMergeFileReferencesAtLevel(((ILSMDiskComponent) mergedComponents.get(0)).getLevel() + 1, 1);
            newComponent = lsmRTree.createDiskComponent(refs.getInsertIndexFileReference(), null, null, true);
            componentBulkLoader = newComponent.createBulkLoader(operation, 1.0f, false, 0L, false, false, false);
            componentBulkLoaders.add(componentBulkLoader);
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
            newComponents.add(newComponent);
            if (newComponent.getLSMComponentFilter() != null) {
                ITupleReference minTuple = mergedComponents.get(0).getLSMComponentFilter().getMinTuple();
                ITupleReference maxTuple = mergedComponents.get(0).getLSMComponentFilter().getMaxTuple();
                minTuples.add(minTuple);
                maxTuples.add(maxTuple);
            }
        } else {
            ILSMDiskComponent newComponent = null;
            ILSMDiskComponentBulkLoader componentBulkLoader = null;
            double[] minMBR = null;
            double[] maxMBR = null;
            long totalTuples = 0L;
            ITupleReference minTuple = null;
            ITupleReference maxTuple = null;
            MultiComparator filterCmp = null;
            long levelTo = ((ILSMDiskComponent) mergedComponents.get(0)).getLevel() + 1;
            long start = lsmRTree.getMaxLevelId(levelTo) + 1;
            List<FileReference> mergeFileTargets = new ArrayList<>();
            List<FileReference> mergeBloomFilterTargets = new ArrayList<>();
            try {
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
                        LSMComponentFileReferences refs = lsmRTree.getNextMergeFileReferencesAtLevel(levelTo, start++);
                        mergeFileTargets.add(refs.getInsertIndexFileReference());
                        mergeBloomFilterTargets.add(refs.getBloomFilterFileReference());
                        newComponent = lsmRTree.createDiskComponent(refs.getInsertIndexFileReference(), null,
                                refs.getBloomFilterFileReference(), true);
                        componentBulkLoader =
                                newComponent.createBulkLoader(operation, 1.0f, false, 0L, false, false, false);
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
                    if (newComponent.getComponentSize() >= lsmRTree.memTableSize) {
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

    public static long getMaxNumTuplesPerComponent(List<? extends ILSMComponent> components)
            throws HyracksDataException {
        long m = 0L;
        for (ILSMComponent c : components) {
            ILSMDiskComponent d = (ILSMDiskComponent) c;
            if (d.getTupleCount() > m) {
                m = d.getTupleCount();
            }
        }
        return m;
    }

    private List<ILSMDiskComponent> doSTROrderMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMRTreeMergeOperation mergeOp = (LSMRTreeMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        ILSMIndexOperationContext opCtx = ((LSMIndexSearchCursor) cursor).getOpCtx();
        lsmRTree.search(opCtx, cursor, rtreeSearchPred);

        List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();
        if (mergedComponents.size() == 1) {
            LSMComponentFileReferences refs = lsmRTree
                    .getNextMergeFileReferencesAtLevel(((ILSMDiskComponent) mergedComponents.get(0)).getLevel() + 1, 1);
            ILSMDiskComponent newComponent =
                    lsmRTree.createDiskComponent(refs.getInsertIndexFileReference(), null, null, true);
            ILSMDiskComponentBulkLoader componentBulkLoader =
                    newComponent.createBulkLoader(operation, 1.0f, false, 0L, false, false, false);
            byte[] minKey = ((ILSMDiskComponent) mergedComponents.get(0)).getMinKey();
            byte[] maxKey = ((ILSMDiskComponent) mergedComponents.get(0)).getMaxKey();
            long totalTuples = 0L;
            try {
                while (cursor.hasNext()) {
                    cursor.next();
                    componentBulkLoader.add(cursor.getTuple());
                    totalTuples++;
                }
            } finally {
                cursor.close();
            }
            newComponent.setMinKey(minKey);
            newComponent.setMaxKey(maxKey);
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
            long levelTo = ((ILSMDiskComponent) mergedComponents.get(0)).getLevel() + 1;
            long start = lsmRTree.getMaxLevelId(levelTo) + 1;
            List<FileReference> mergeFileTargets = new ArrayList<>();
            List<FileReference> mergeBloomFilterTargets = new ArrayList<>();
            List<TupleWithMBR> allTuples = new ArrayList<>();
            long numTuplesInPartition = getMaxNumTuplesPerComponent(mergedComponents);
            try {
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference frameTuple = cursor.getTuple();
                    allTuples.add(new TupleWithMBR(frameTuple, lsmRTree.getMBRFromTuple(frameTuple)));
                }
            } finally {
                cursor.close();
            }
            List<Partition> partitions = partitionTuplesBySTR(allTuples, numTuplesInPartition);
            List<ILSMDiskComponent> newComponents = new ArrayList<>();
            List<ILSMDiskComponentBulkLoader> componentBulkLoaders = new ArrayList<>();
            for (int i = 0; i < partitions.size(); i++) {
                Partition partition = partitions.get(i);
                int dim = partition.getDim();
                LSMComponentFileReferences refs = lsmRTree.getNextMergeFileReferencesAtLevel(levelTo, start++);
                mergeFileTargets.add(refs.getInsertIndexFileReference());
                mergeBloomFilterTargets.add(refs.getBloomFilterFileReference());
                ILSMDiskComponent newComponent = lsmRTree.createDiskComponent(refs.getInsertIndexFileReference(), null,
                        refs.getBloomFilterFileReference(), true);
                newComponents.add(newComponent);
                ILSMDiskComponentBulkLoader componentBulkLoader =
                        newComponent.createBulkLoader(operation, 1.0f, false, 0L, false, false, false);
                componentBulkLoaders.add(componentBulkLoader);
                MultiComparator filterCmp = newComponent.getLSMComponentFilter() == null ? null
                        : MultiComparator.create(newComponent.getLSMComponentFilter().getFilterCmpFactories());
                ITupleReference minTuple = null;
                ITupleReference maxTuple = null;
                for (int j = 0; j < partition.size(); j++) {
                    ITupleReference tuple = partition.getTuple(j);
                    if (minTuple == null) {
                        minTuple = tuple;
                    } else {
                        if (filterCmp != null && filterCmp.compare(tuple, minTuple) < 0) {
                            minTuple = tuple;
                        }
                    }
                    if (maxTuple == null) {
                        maxTuple = tuple;
                    } else {
                        if (filterCmp != null && filterCmp.compare(tuple, maxTuple) > 0) {
                            maxTuple = tuple;
                        }
                    }
                    componentBulkLoader.add(tuple);
                }
                double[] minMBR = clone(partition.getMBR(), 0, dim);
                double[] maxMBR = clone(partition.getMBR(), dim, dim);
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
            mergeOp.setTargets(mergeFileTargets);
            mergeOp.setBloomFilterTargets(mergeBloomFilterTargets);
            for (ILSMDiskComponentBulkLoader componentBulkLoader : componentBulkLoaders) {
                componentBulkLoader.end();
            }
            return newComponents;
        }
    }

    @Override
    public List<ILSMDiskComponent> merge(ILSMIOOperation operation) throws HyracksDataException {
        return doSTROrderMerge(operation);
    }

    public static void orderTuplesBySTR(List<TupleWithMBR> tuplesToPartition, List<Partition> partitions,
            long numTuplesInPartition, int startDim) {
        if (tuplesToPartition == null || tuplesToPartition.isEmpty()) {
            return;
        }
        int numPartitions = partitions.size();
        int dim = tuplesToPartition.get(0).getDim();
        // Handle the last dimension, place tuples into partitions
        if (dim < 2 || startDim >= dim) {
            for (int i = 0; i < numPartitions; i++) {
                Partition p = partitions.get(i);
                long empties = numTuplesInPartition - p.getTuples().size(); // Number of available space in the partition
                if (empties > 0) {
                    if (empties >= tuplesToPartition.size()) {
                        // Place all tuples in the slice to the partition
                        for (int j = 0; j < tuplesToPartition.size(); j++) {
                            p.addTuple(tuplesToPartition.get(j));
                        }
                        return;
                    } else {
                        // Place some tuples in the slice to the partition, then check the next partition
                        for (int j = 0; j < empties; j++) {
                            p.addTuple(tuplesToPartition.get(0));
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

    public static List<Partition> partitionTuplesBySTR(List<TupleWithMBR> tuplesToPartition,
            long numTuplesInPartition) {
        if (tuplesToPartition == null || tuplesToPartition.isEmpty()) {
            return Collections.emptyList();
        }
        int numPartitions = (int) Math.ceil((double) tuplesToPartition.size() / numTuplesInPartition);
        if (numPartitions == 1) {
            return Collections.singletonList(new Partition(tuplesToPartition));
        }

        int dim = tuplesToPartition.get(0).getDim();

        tuplesToPartition.sort(new Comparator<TupleWithMBR>() {
            @Override
            public int compare(TupleWithMBR t1, TupleWithMBR t2) {
                double[] c1 = t1.getCenter();
                double[] c2 = t2.getCenter();
                return Double.compare(c1[0], c2[0]);
            }
        });

        List<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new Partition(dim));
        }
        orderTuplesBySTR(tuplesToPartition, partitions, numTuplesInPartition, 0);

        /*
        int numVSlices = (int) Math.ceil(Math.sqrt(numPartitions));
        int sliceCapacity = (int) numTuplesInPartition * numVSlices;
        List<List<TupleWithMBR>> vSlices = new ArrayList<>();
        for (int i = 0; i < numVSlices; i++) {
            List<TupleWithMBR> sliceTuples = new ArrayList<>();
            int bound = (i + 1) * sliceCapacity <= tuplesToPartition.size() ? sliceCapacity : tuplesToPartition.size() - i * sliceCapacity;
            for (int j = 0; j < bound; j++) {
                TupleWithMBR t = tuplesToPartition.get(i * sliceCapacity + j);
                sliceTuples.add(t);
            }
            sliceTuples.sort(new Comparator<TupleWithMBR>() {
                @Override
                public int compare(TupleWithMBR t1, TupleWithMBR t2) {
                    double[] c1 = t1.getCenter();
                    double[] c2 = t2.getCenter();
                    return Double.compare(c1[1], c2[1]);
                }
            });
            vSlices.add(sliceTuples);
        }
        
        List<TuplesWithMBR> partitions = new ArrayList<>();
        TuplesWithMBR currentPartition = new TuplesWithMBR(dim);
        for (int i = 0; i < numVSlices; i++) {
            List<TupleWithMBR> sliceTuples = vSlices.get(i);
            for (int j = 0; j < sliceTuples.size(); j++) {
                currentPartition.addTuple(sliceTuples.get(j));
                if (currentPartition.getTuples().size() == numTuplesInPartition) {
                    partitions.add(currentPartition);
                    currentPartition = new TuplesWithMBR(dim);
                }
            }
        }
        if (!currentPartition.getTuples().isEmpty()) {
            partitions.add(currentPartition);
        }*/

        return partitions;
    }

    static class TupleWithMBR {
        private ITupleReference tuple;
        private double[] mbr;
        private int dim;
        private double[] center;

        public TupleWithMBR(ITupleReference tuple, double[] mbr) {
            this.tuple = tuple;
            this.mbr = LSMRTreeLevelMergePolicyHelper.clone(mbr);
            dim = mbr.length / 2;
            center = new double[dim];
            for (int i = 0; i < dim; i++) {
                center[i] = (mbr[i] + mbr[i + dim]) / 2;
            }
        }

        public ITupleReference getTuple() {
            return tuple;
        }

        public double[] getMBR() {
            return mbr;
        }

        public int getDim() {
            return dim;
        }

        public double[] getCenter() {
            return center;
        }
    }

    static class Partition {
        private List<ITupleReference> tuples;
        private double[] mbr;
        private int dim;

        public Partition(int dim) {
            this.tuples = new ArrayList<>();
            this.mbr = null;
            this.dim = dim;
        }

        public Partition(List<ITupleReference> tuples, double[] mbr) {
            this.tuples = new ArrayList<>(tuples);
            this.mbr = LSMRTreeLevelMergePolicyHelper.clone(mbr);
            dim = mbr.length / 2;
        }

        public Partition(List<TupleWithMBR> tuples) {
            this.tuples = new ArrayList<>();
            mbr = null;
            for (TupleWithMBR tuple : tuples) {
                double[] tupleMBR = tuple.getMBR();
                if (mbr == null) {
                    dim = tupleMBR.length / 2;
                    mbr = LSMRTreeLevelMergePolicyHelper.clone(tupleMBR);
                } else {
                    for (int i = 0; i < dim; i++) {
                        if (Double.compare(tupleMBR[i], mbr[i]) < 0) {
                            mbr[i] = tupleMBR[i];
                        }
                    }
                    for (int i = dim; i < dim * 2; i++) {
                        if (Double.compare(tupleMBR[i], mbr[i]) > 0) {
                            mbr[i] = tupleMBR[i];
                        }
                    }
                }
                this.tuples.add(tuple.getTuple());
            }
        }

        public List<ITupleReference> getTuples() {
            return tuples;
        }

        public double[] getMBR() {
            return mbr;
        }

        public int getDim() {
            return dim;
        }

        public int size() {
            return tuples.size();
        }

        public ITupleReference getTuple(int i) {
            return tuples.get(i);
        }

        public void addTuple(TupleWithMBR tuple) {
            tuples.add(tuple.getTuple());
            double[] tupleMBR = tuple.getMBR();
            if (mbr == null) {
                mbr = LSMRTreeLevelMergePolicyHelper.clone(tupleMBR);
            } else {
                for (int i = 0; i < dim; i++) {
                    if (Double.compare(tupleMBR[i], mbr[i]) < 0) {
                        mbr[i] = tupleMBR[i];
                    }
                }
                for (int i = dim; i < dim * 2; i++) {
                    if (Double.compare(tupleMBR[i], mbr[i]) > 0) {
                        mbr[i] = tupleMBR[i];
                    }
                }
            }
        }
    }

}
