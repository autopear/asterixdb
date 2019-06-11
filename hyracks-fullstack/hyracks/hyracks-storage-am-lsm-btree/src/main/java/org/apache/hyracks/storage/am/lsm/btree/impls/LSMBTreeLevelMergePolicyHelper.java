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

package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.AbstractLSMWithBloomFilterDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLevelMergePolicyHelper;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;

public class LSMBTreeLevelMergePolicyHelper extends AbstractLevelMergePolicyHelper {
    protected final LSMBTree lsmBTree;

    public LSMBTreeLevelMergePolicyHelper(AbstractLSMIndex index) {
        super(index);
        lsmBTree = (LSMBTree) index;
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

        byte[] minKey;
        byte[] maxKey;

        try {
            minKey = component.getMinKey();
            maxKey = component.getMaxKey();
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
                byte[] cMinKey = component.getMinKey();
                byte[] cMaxKey = component.getMaxKey();
                if (!(lsmBTree.compareKey(minKey, cMaxKey) > 0 || lsmBTree.compareKey(maxKey, cMinKey) < 0)) {
                    overlapped.add(c);
                }
            } catch (HyracksDataException ex) {
                overlapped.add(c);
            }
        }
        return overlapped;
    }

    public List<ILSMDiskComponent> merge(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTreeMergeOperation mergeOp = (LSMBTreeMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        List<ILSMDiskComponent> newComponents = new ArrayList<>();
        List<ILSMDiskComponentBulkLoader> componentBulkLoaders = new ArrayList<>();
        List<ITupleReference> minTuples = new ArrayList<>();
        List<ITupleReference> maxTuples = new ArrayList<>();
        try {
            try {
                RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
                lsmBTree.search(mergeOp.getAccessor().getOpContext(), cursor, rangePred);
                try {
                    List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();
                    long numElements = getNumberOfElements(mergedComponents);
                    ILSMDiskComponent newComponent = null;
                    ILSMDiskComponentBulkLoader componentBulkLoader = null;
                    if (mergedComponents.size() == 1) {
                        LSMComponentFileReferences refs = lsmBTree.getNextMergeFileReferencesAtLevel(
                                ((ILSMDiskComponent) mergedComponents.get(0)).getLevel() + 1, 1);
                        newComponent = lsmBTree.createDiskComponent(refs.getInsertIndexFileReference(), null,
                                refs.getBloomFilterFileReference(), true);
                        componentBulkLoader =
                                newComponent.createBulkLoader(operation, 1.0f, false, numElements, false, false, false);
                        componentBulkLoaders.add(componentBulkLoader);
                        byte[] minKey = null;
                        byte[] maxKey = null;
                        while (cursor.hasNext()) {
                            cursor.next();
                            ITupleReference frameTuple = cursor.getTuple();
                            componentBulkLoader.add(frameTuple);
                            byte[] key = LSMBTree.getKeyBytes(frameTuple);
                            if (key != null) {
                                if (minKey == null) {
                                    minKey = key;
                                } else {
                                    if (lsmBTree.compareKey(key, minKey) < 0) {
                                        minKey = key;
                                    }
                                }
                                if (maxKey == null) {
                                    maxKey = key;
                                } else {
                                    if (lsmBTree.compareKey(key, maxKey) > 0) {
                                        maxKey = key;
                                    }
                                }
                            }
                        }
                        newComponent.setMinKey(minKey);
                        newComponent.setMaxKey(maxKey);
                        newComponents.add(newComponent);
                        if (newComponent.getLSMComponentFilter() != null) {
                            ITupleReference minTuple = mergedComponents.get(0).getLSMComponentFilter().getMinTuple();
                            ITupleReference maxTuple = mergedComponents.get(0).getLSMComponentFilter().getMaxTuple();
                            minTuples.add(minTuple);
                            maxTuples.add(maxTuple);
                        }
                    } else {
                        ITupleReference minTuple = null;
                        ITupleReference maxTuple = null;
                        MultiComparator filterCmp = null;
                        long levelTo = ((ILSMDiskComponent) mergedComponents.get(0)).getLevel() + 1;
                        long start = lsmBTree.getMaxLevelId(levelTo) + 1;
                        List<FileReference> mergeFileTargets = new ArrayList<>();
                        List<FileReference> mergeBloomFilterTargets = new ArrayList<>();
                        byte[] minKey = null;
                        byte[] maxKey = null;
                        while (cursor.hasNext()) {
                            cursor.next();
                            ITupleReference frameTuple = cursor.getTuple();
                            if (newComponent == null) {
                                LSMComponentFileReferences refs =
                                        lsmBTree.getNextMergeFileReferencesAtLevel(levelTo, start++);
                                mergeFileTargets.add(refs.getInsertIndexFileReference());
                                mergeBloomFilterTargets.add(refs.getBloomFilterFileReference());
                                newComponent = lsmBTree.createDiskComponent(refs.getInsertIndexFileReference(), null,
                                        refs.getBloomFilterFileReference(), true);
                                componentBulkLoader =
                                        newComponent.createBulkLoader(operation, 1.0f, false, 0L, false, false, false);
                                newComponents.add(newComponent);
                                componentBulkLoaders.add(componentBulkLoader);
                                filterCmp = newComponent.getLSMComponentFilter() == null ? null
                                        : MultiComparator
                                                .create(newComponent.getLSMComponentFilter().getFilterCmpFactories());
                                minTuple = null;
                                maxTuple = null;
                            }
                            componentBulkLoader.add(frameTuple);
                            if (filterCmp != null) {
                                if (minTuple == null) {
                                    minTuple = frameTuple;
                                } else {
                                    if (filterCmp.compare(frameTuple, minTuple) < 0) {
                                        minTuple = frameTuple;
                                    }
                                }
                                if (maxTuple == null) {
                                    maxTuple = frameTuple;
                                } else {
                                    if (filterCmp.compare(frameTuple, maxTuple) > 0) {
                                        maxTuple = frameTuple;
                                    }
                                }
                            }
                            byte[] key = LSMBTree.getKeyBytes(frameTuple);
                            if (key != null) {
                                if (minKey == null) {
                                    minKey = key;
                                } else {
                                    if (lsmBTree.compareKey(key, minKey) < 0) {
                                        minKey = key;
                                    }
                                }
                                if (maxKey == null) {
                                    maxKey = key;
                                } else {
                                    if (lsmBTree.compareKey(key, maxKey) > 0) {
                                        maxKey = key;
                                    }
                                }
                            }
                            if (newComponent.getComponentSize() >= lsmBTree.memTableSize) {
                                newComponent.setMinKey(minKey);
                                newComponent.setMaxKey(maxKey);
                                if (filterCmp != null) {
                                    minTuples.add(minTuple);
                                    maxTuples.add(maxTuple);
                                    minTuple = null;
                                    maxTuple = null;
                                    filterCmp = null;
                                }
                                newComponent = null;
                                componentBulkLoader = null;
                                minKey = null;
                                maxKey = null;
                            }
                        }
                        if (newComponent != null) {
                            newComponent.setMinKey(minKey);
                            newComponent.setMaxKey(maxKey);
                            if (filterCmp != null) {
                                minTuples.add(minTuple);
                                maxTuples.add(maxTuple);
                            }
                        }
                        mergeOp.setTargets(mergeFileTargets);
                        mergeOp.setBloomFilterTargets(mergeBloomFilterTargets);
                    }
                } finally {
                    cursor.close();
                }
            } finally {
                cursor.destroy();
            }
            for (int i = 0; i < newComponents.size(); i++) {
                ILSMDiskComponent newComponent = newComponents.get(i);
                if (newComponent.getLSMComponentFilter() != null) {
                    List<ITupleReference> filterTuples = Arrays.asList(minTuples.get(i), maxTuples.get(i));
                    lsmBTree.getFilterManager().updateFilter(newComponent.getLSMComponentFilter(), filterTuples,
                            NoOpOperationCallback.INSTANCE);
                    lsmBTree.getFilterManager().writeFilter(newComponent.getLSMComponentFilter(),
                            newComponent.getMetadataHolder());
                }
            }
        } catch (Throwable e) { // NOSONAR.. As per the contract, we should either abort or end
            try {
                for (ILSMDiskComponentBulkLoader componentBulkLoader : componentBulkLoaders) {
                    componentBulkLoader.abort();
                }
            } catch (Throwable th) { // NOSONAR Don't lose the root failure
                e.addSuppressed(th);
            }
            throw e;
        }
        for (ILSMDiskComponentBulkLoader componentBulkLoader : componentBulkLoaders) {
            componentBulkLoader.end();
        }

        return newComponents;
    }

    private long getNumberOfElements(List<ILSMComponent> mergedComponents) throws HyracksDataException {
        long numElements = 0L;
        if (lsmBTree.hasBloomFilter()) {
            //count elements in btree for creating Bloomfilter
            for (int i = 0; i < mergedComponents.size(); ++i) {
                numElements += ((AbstractLSMWithBloomFilterDiskComponent) mergedComponents.get(i)).getBloomFilter()
                        .getNumElements();
            }
        }
        return numElements;
    }
}
