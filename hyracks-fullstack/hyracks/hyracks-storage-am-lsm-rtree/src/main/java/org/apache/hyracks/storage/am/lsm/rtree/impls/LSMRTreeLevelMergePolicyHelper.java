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

    public LSMRTreeLevelMergePolicyHelper(AbstractLSMIndex index) {
        super(index);
        lsmRTree = (AbstractLSMRTree) index;
    }

    public static boolean isOverlapping(double[] min1, double[] max1, double[] min2, double[] max2) {
        if (min1 == null || max1 == null || min2 == null || max2 == null) {
            return true;
        }
        for (int i = 0; i < min1.length; i++) {
            if (min1[i] > max2[i] || min2[i] > max1[i]) {
                return false;
            }
        }
        return true;
    }

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
                        minMBR = new double[dim];
                        System.arraycopy(mbr, 0, minMBR, 0, dim);
                    } else {
                        for (int i = 0; i < dim; i++) {
                            if (mbr[i] < minMBR[i]) {
                                minMBR[i] = mbr[i];
                            }
                        }
                    }
                    if (maxMBR == null) {
                        maxMBR = new double[dim];
                        System.arraycopy(mbr, dim, maxMBR, 0, dim);
                    } else {
                        for (int i = 0; i < dim; i++) {
                            if (mbr[dim + i] > maxMBR[i]) {
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
                        minMBR = new double[dim];
                        System.arraycopy(mbr, 0, minMBR, 0, dim);
                    } else {
                        for (int i = 0; i < dim; i++) {
                            if (mbr[i] < minMBR[i]) {
                                minMBR[i] = mbr[i];
                            }
                        }
                    }
                    if (maxMBR == null) {
                        maxMBR = new double[dim];
                        System.arraycopy(mbr, dim, maxMBR, 0, dim);
                    } else {
                        for (int i = 0; i < dim; i++) {
                            if (mbr[dim + i] > maxMBR[i]) {
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
                        minMBR = new double[dim];
                        System.arraycopy(mbr, 0, minMBR, 0, dim);
                    } else {
                        for (int i = 0; i < dim; i++) {
                            if (mbr[i] < minMBR[i]) {
                                minMBR[i] = mbr[i];
                            }
                        }
                    }
                    if (maxMBR == null) {
                        maxMBR = new double[dim];
                        System.arraycopy(mbr, dim, maxMBR, 0, dim);
                    } else {
                        for (int i = 0; i < dim; i++) {
                            if (mbr[dim + i] > maxMBR[i]) {
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
            long levelTo = ((ILSMDiskComponent) mergedComponents.get(0)).getLevel() + 1;
            long start = lsmRTree.getMaxLevelId(levelTo) + 1;
            List<FileReference> mergeFileTargets = new ArrayList<>();
            List<FileReference> mergeBloomFilterTargets = new ArrayList<>();
            List<TupleWithMBR> allTuples = new ArrayList<>();
            long numTuplesInPartition = lsmRTree.getMaxNumTuplesPerComponent();
            try {
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference frameTuple = cursor.getTuple();
                    allTuples.add(new TupleWithMBR(frameTuple, lsmRTree.getMBRFromTuple(frameTuple)));
                }
            } finally {
                cursor.close();
            }
            List<TuplesWithMBR> partitionedTuples = partitionTuplesBySTR(allTuples, numTuplesInPartition);
            List<ILSMDiskComponent> newComponents = new ArrayList<>();
            for (TuplesWithMBR tuples : partitionedTuples) {
                LSMComponentFileReferences refs = lsmRTree.getNextMergeFileReferencesAtLevel(levelTo, start++);
                mergeFileTargets.add(refs.getInsertIndexFileReference());
                mergeBloomFilterTargets.add(refs.getBloomFilterFileReference());
                ILSMDiskComponent newComponent = lsmRTree.createDiskComponent(refs.getInsertIndexFileReference(), null,
                        refs.getBloomFilterFileReference(), true);
                newComponents.add(newComponent);
                ILSMDiskComponentBulkLoader componentBulkLoader =
                        newComponent.createBulkLoader(operation, 1.0f, false, 0L, false, false, false);
                MultiComparator filterCmp = newComponent.getLSMComponentFilter() == null ? null
                        : MultiComparator.create(newComponent.getLSMComponentFilter().getFilterCmpFactories());
                ITupleReference minTuple = null;
                ITupleReference maxTuple = null;
                for (ITupleReference tuple : tuples.getTuples()) {
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
                double[] minMBR = new double[tuples.getDim()];
                double[] maxMBR = new double[tuples.getDim()];
                System.arraycopy(tuples.getMBR(), 0, minMBR, 0, tuples.getDim());
                System.arraycopy(tuples.getMBR(), tuples.getDim(), maxMBR, 0, tuples.getDim());
                newComponent.setMinKey(AbstractLSMRTree.doublesToBytes(minMBR));
                newComponent.setMaxKey(AbstractLSMRTree.doublesToBytes(maxMBR));
                newComponent.setTupleCount(tuples.getTuples().size());
                if (filterCmp != null) {
                    List<ITupleReference> filterTuples = Arrays.asList(minTuple, maxTuple);
                    lsmRTree.getFilterManager().updateFilter(newComponent.getLSMComponentFilter(), filterTuples,
                            NoOpOperationCallback.INSTANCE);
                    lsmRTree.getFilterManager().writeFilter(newComponent.getLSMComponentFilter(),
                            newComponent.getMetadataHolder());
                }
                componentBulkLoader.end();
            }
            mergeOp.setTargets(mergeFileTargets);
            mergeOp.setBloomFilterTargets(mergeBloomFilterTargets);
            return newComponents;
        }
    }

    public List<ILSMDiskComponent> merge(ILSMIOOperation operation) throws HyracksDataException {
        return doSTROrderMerge(operation);
    }

    public static void orderTuplesBySTR(List<TupleWithMBR> tuples, List<TuplesWithMBR> partitions, long partitionTuples,
            int startDim) {
        if (tuples == null || tuples.isEmpty()) {
            return;
        }
        int numPartitions = partitions.size();
        int dim = tuples.get(0).getDim();
        // Handle the last dimension, place tuples into partitions
        if (dim < 2 || startDim >= dim) {
            for (int i = 0; i < numPartitions; i++) {
                TuplesWithMBR p = partitions.get(i);
                long empties = partitionTuples - p.getTuples().size(); // Number of available space in the partition
                if (empties > 0) {
                    if (empties >= tuples.size()) {
                        // Place all tuples in the slice to the partition
                        for (int j = 0; j < tuples.size(); j++) {
                            p.addTuple(tuples.get(j));
                        }
                        return;
                    } else {
                        // Place some tuples in the slice to the partition, then check the next partition
                        for (int j = 0; j < empties; j++) {
                            p.addTuple(tuples.get(0));
                            tuples.remove(0);
                        }
                    }
                }
            }
            return;
        }

        int thisDim = dim - startDim; // The number of dimensions to check

        int numSlices = (int) Math.ceil(Math.pow(numPartitions, 1.0 / thisDim));
        int sliceCapacity =
                (int) partitionTuples * (int) Math.ceil(Math.pow(numPartitions, (double) (thisDim - 1) / thisDim));
        for (int i = 0; i < numSlices; i++) {
            // Place tuples sorted by startDim-1 into slices
            List<TupleWithMBR> slice = new ArrayList<>();
            int bound = (i + 1) * sliceCapacity <= tuples.size() ? sliceCapacity : tuples.size() - i * sliceCapacity;
            for (int j = 0; j < bound; j++) {
                TupleWithMBR t = tuples.get(i * sliceCapacity + j);
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
            orderTuplesBySTR(slice, partitions, partitionTuples, startDim + 1);
        }
    }

    public static boolean intersect(double[] mbr1, double[] mbr2) {
        int dim = mbr1.length / 2;
        for (int i = 0; i < dim; i++) {
            if (mbr1[i] > mbr2[dim + i] || mbr2[i] > mbr1[dim + i]) {
                return false;
            }
        }
        return true;
    }

    public static List<TuplesWithMBR> partitionTuplesBySTR(List<TupleWithMBR> tuples, long partitionTuples) {
        if (tuples == null || tuples.isEmpty()) {
            return Collections.emptyList();
        }
        int numPartitions = (int) Math.ceil((double) tuples.size() / partitionTuples);
        if (numPartitions == 1) {
            return Collections.singletonList(new TuplesWithMBR(tuples));
        }

        int dim = tuples.get(0).getDim();

        tuples.sort(new Comparator<TupleWithMBR>() {
            @Override
            public int compare(TupleWithMBR t1, TupleWithMBR t2) {
                double[] c1 = t1.getCenter();
                double[] c2 = t2.getCenter();
                return Double.compare(c1[0], c2[0]);
            }
        });

        List<TuplesWithMBR> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new TuplesWithMBR(dim));
        }
        orderTuplesBySTR(tuples, partitions, partitionTuples, 0);

        /*
        int numVSlices = (int) Math.ceil(Math.sqrt(numPartitions));
        int sliceCapacity = (int) partitionTuples * numVSlices;
        List<List<TupleWithMBR>> vSlices = new ArrayList<>();
        for (int i = 0; i < numVSlices; i++) {
            List<TupleWithMBR> sliceTuples = new ArrayList<>();
            int bound = (i + 1) * sliceCapacity <= tuples.size() ? sliceCapacity : tuples.size() - i * sliceCapacity;
            for (int j = 0; j < bound; j++) {
                TupleWithMBR t = tuples.get(i * sliceCapacity + j);
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
                if (currentPartition.getTuples().size() == partitionTuples) {
                    partitions.add(currentPartition);
                    currentPartition = new TuplesWithMBR(dim);
                }
            }
        }
        if (!currentPartition.getTuples().isEmpty()) {
            partitions.add(currentPartition);
        }
        */
        return partitions;
    }

    static class TupleWithMBR {
        private ITupleReference tuple;
        private double[] mbr;
        private int dim;
        private double[] center;

        public TupleWithMBR(ITupleReference tuple, double[] mbr) {
            this.tuple = tuple;
            this.mbr = mbr;
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

    static class TuplesWithMBR {
        private List<ITupleReference> tuples;
        private double[] mbr = null;
        private int dim;

        public TuplesWithMBR(int dim) {
            this.tuples = new ArrayList<>();
            this.mbr = new double[dim * 2];
            this.dim = dim;
        }

        public TuplesWithMBR(List<ITupleReference> tuples, double[] mbr) {
            this.tuples = new ArrayList<>(tuples);
            this.mbr = mbr;
            dim = mbr.length / 2;
        }

        public TuplesWithMBR(List<TupleWithMBR> tuples) {
            this.tuples = new ArrayList<>();
            for (TupleWithMBR tuple : tuples) {
                double[] tupleMBR = tuple.getMBR();
                if (mbr == null) {
                    mbr = tupleMBR;
                    dim = tupleMBR.length / 2;
                } else {
                    for (int i = 0; i < dim; i++) {
                        if (tupleMBR[i] < mbr[i]) {
                            mbr[i] = tupleMBR[i];
                        }
                    }
                    for (int i = dim; i < dim * 2; i++) {
                        if (tupleMBR[i] > mbr[i]) {
                            mbr[i] = tupleMBR[i];
                        }
                    }
                }
                this.tuples.add(tuple.getTuple());
            }
            dim = mbr.length / 2;
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

        public void addTuple(TupleWithMBR tuple) {
            tuples.add(tuple.getTuple());
            double[] tupleMBR = tuple.getMBR();
            for (int i = 0; i < dim; i++) {
                if (tupleMBR[i] < mbr[i]) {
                    mbr[i] = tupleMBR[i];
                }
            }
            for (int i = dim; i < dim * 2; i++) {
                if (tupleMBR[i] > mbr[i]) {
                    mbr[i] = tupleMBR[i];
                }
            }
        }
    }

}
