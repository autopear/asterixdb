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
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
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
    public LSMBTreeLevelMergePolicyHelper(AbstractLSMIndex index, long tableSize) {
        super(index, tableSize);
    }

    public ILSMDiskComponent getBestComponent(List<ILSMDiskComponent> components, long level) {
        return getRandomComponent(components, level, Distribution.Uniform);
    }

    public List<ILSMDiskComponent> getOverlappingComponents(ILSMDiskComponent component, List<ILSMDiskComponent> components, long level) {
        Map<Long, ILSMDiskComponent> map = new HashMap<>();
        for (ILSMDiskComponent c : components) {
            if (c.getLevel() == level) {
                map.put(c.getLevelSequence(), c);
            }
        }
        if (map.isEmpty()) {
            return Collections.emptyList();
        }
        List<Long> seqs = new ArrayList<>(map.keySet());
        Collections.sort(seqs);
        List<ILSMDiskComponent> overlapped = new ArrayList<>();

        byte[] minKey = null;
        byte[] maxKey = null;

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
                byte[] cMinKey = c.getMinKey();
                byte[] cMaxKey = c.getMaxKey();
                if (!(LSMBTree.compareBytes(cMinKey, maxKey) > 0 || LSMBTree.compareBytes(cMaxKey, minKey) < 0)) {
                    overlapped.add(c);
                }
            } catch (HyracksDataException ex) {
                overlapped.add(c);
            }
        }
        return overlapped;
    }

    public List<ILSMDiskComponent> merge(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTree btree = (LSMBTree)index;
        LSMBTreeMergeOperation mergeOp = (LSMBTreeMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        List<ILSMDiskComponent> newComponents = new ArrayList<>();
        List<ILSMDiskComponentBulkLoader> componentBulkLoaders = new ArrayList<>();
        List<ITupleReference> minTuples = new ArrayList<>();
        List<ITupleReference> maxTuples = new ArrayList<>();
        try {
            try {
                RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
                index.search(mergeOp.getAccessor().getOpContext(), cursor, rangePred);
                try {
                    List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();
                    long numElements = getNumberOfElements(mergedComponents);
                    ILSMDiskComponent newComponent = null;
                    ILSMDiskComponentBulkLoader componentBulkLoader = null;
                    if (mergedComponents.size() == 1) {
                        LSMComponentFileReferences refs = btree.getNextMergeFileReferencesAtLevel(
                                ((ILSMDiskComponent) mergedComponents.get(0)).getLevel() + 1, 1);
                        newComponent = btree.createDiskComponent(refs.getInsertIndexFileReference(),
                                null, refs.getBloomFilterFileReference(), true);
                        componentBulkLoader = newComponent.createBulkLoader(operation, 1.0f, false, numElements,
                                false, false, false);
                        componentBulkLoaders.add(componentBulkLoader);
                        byte[] minKey = null;
                        byte[] maxKey = null;
                        while (cursor.hasNext()) {
                            cursor.next();
                            ITupleReference frameTuple = cursor.getTuple();
                            componentBulkLoader.add(frameTuple);
                            byte[] key = btree.getTupleKey(frameTuple);
                            if (key != null) {
                                if (minKey == null) {
                                    minKey = key;
                                } else {
                                    if (btree.compareBytes(key, minKey) < 0) {
                                        minKey = key;
                                    }
                                }
                                if (maxKey == null) {
                                    maxKey = key;
                                } else {
                                    if (ByteArrayPointable.compare(maxKey, 0, maxKey.length, key, 0,
                                            key.length) <= 0) {
                                        maxKey = key;
                                    }
                                }
                            }
                        }
                        newComponent.setMinKey(minKey);
                        newComponent.setMaxKey(maxKey);
                        newComponents.add(newComponent);
                        if (newComponent.getLSMComponentFilter() != null) {
                            ITupleReference minTuple =
                                    mergedComponents.get(0).getLSMComponentFilter().getMinTuple();
                            ITupleReference maxTuple =
                                    mergedComponents.get(0).getLSMComponentFilter().getMaxTuple();
                            minTuples.add(minTuple);
                            maxTuples.add(maxTuple);
                        }
                    } else if (((ILSMDiskComponent) (mergedComponents.get(mergedComponents.size() - 1)))
                            .getLevel() == 0L) {
                        LSMComponentFileReferences refs = ((LSMBTree)index).getNextMergeFileReferencesAtLevel(1L, 1);
                        newComponent = btree.createDiskComponent(refs.getInsertIndexFileReference(),
                                null, refs.getBloomFilterFileReference(), true);
                        componentBulkLoader = newComponent.createBulkLoader(operation, 1.0f, false, numElements,
                                false, false, false);
                        componentBulkLoaders.add(componentBulkLoader);
                        byte[] minKey = null;
                        byte[] maxKey = null;
                        while (cursor.hasNext()) {
                            cursor.next();
                            ITupleReference frameTuple = cursor.getTuple();
                            componentBulkLoader.add(frameTuple);
                            byte[] key = btree.getTupleKey(frameTuple);
                            if (key != null) {
                                if (minKey == null) {
                                    minKey = key;
                                } else {
                                    if (btree.compareBytes(key, minKey) < 0) {
                                        minKey = key;
                                    }
                                }
                                if (maxKey == null) {
                                    maxKey = key;
                                } else {
                                    if (btree.compareBytes(maxKey, key) <= 0) {
                                        maxKey = key;
                                    }
                                }
                            }
                        }
                        newComponents.add(newComponent);
                        newComponent.setMinKey(minKey);
                        newComponent.setMaxKey(maxKey);
                        if (newComponent.getLSMComponentFilter() != null) {
                            ITupleReference minTuple = null;
                            ITupleReference maxTuple = null;
                            MultiComparator filterCmp = MultiComparator
                                    .create(newComponent.getLSMComponentFilter().getFilterCmpFactories());
                            for (ILSMComponent component : mergedComponents) {
                                ITupleReference minMergeTuple = component.getLSMComponentFilter().getMinTuple();
                                ITupleReference maxMergeTuple = component.getLSMComponentFilter().getMaxTuple();
                                if (minTuple == null) {
                                    minTuple = minMergeTuple;
                                } else {
                                    if (filterCmp.compare(minMergeTuple, minTuple) < 0) {
                                        minTuple = minMergeTuple;
                                    }
                                }
                                if (maxTuple == null) {
                                    maxTuple = maxMergeTuple;
                                } else {
                                    if (filterCmp.compare(maxMergeTuple, maxTuple) > 0) {
                                        maxTuple = maxMergeTuple;
                                    }
                                }
                            }
                            minTuples.add(minTuple);
                            maxTuples.add(maxTuple);
                        }
                    } else {
                        ITupleReference minTuple = null;
                        ITupleReference maxTuple = null;
                        MultiComparator filterCmp = null;
                        long levelTo =
                                ((ILSMDiskComponent) mergedComponents.get(mergedComponents.size() - 1)).getLevel();
                        long start = btree.getMaxLevelId(levelTo);
                        if (start < 1) {
                            start = 1;
                        }
                        List<FileReference> mergeFileTargets = new ArrayList<>();
                        List<FileReference> mergeBloomFilterTargets = new ArrayList<>();
                        byte[] minKey = null;
                        byte[] maxKey = null;
                        while (cursor.hasNext()) {
                            cursor.next();
                            ITupleReference frameTuple = cursor.getTuple();
                            byte[] key = btree.getTupleKey(frameTuple);
                            if (key != null) {
                                if (minKey == null) {
                                    minKey = key;
                                } else {
                                    if (btree.compareBytes(key, minKey) < 0) {
                                        minKey = key;
                                    }
                                }
                                if (maxKey == null) {
                                    maxKey = key;
                                } else {
                                    if (btree.compareBytes(maxKey, key) <= 0) {
                                        maxKey = key;
                                    }
                                }
                            }

                            if (newComponent == null) {
                                LSMComponentFileReferences refs =
                                        btree.getNextMergeFileReferencesAtLevel(levelTo, start++);
                                mergeFileTargets.add(refs.getInsertIndexFileReference());
                                mergeBloomFilterTargets.add(refs.getBloomFilterFileReference());
                                newComponent =
                                        btree.createDiskComponent(refs.getInsertIndexFileReference(),
                                                null, refs.getBloomFilterFileReference(), true);
                                componentBulkLoader = newComponent.createBulkLoader(operation, 1.0f, false, 0L,
                                        false, false, false);
                                newComponents.add(newComponent);
                                componentBulkLoaders.add(componentBulkLoader);
                                filterCmp = newComponent.getLSMComponentFilter() == null ? null
                                        : MultiComparator.create(
                                        newComponent.getLSMComponentFilter().getFilterCmpFactories());
                                minTuple = null;
                                maxTuple = null;
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
                            if (newComponent.getComponentSize() >= tableSize) {
                                newComponent.setMinKey(minKey);
                                newComponent.setMaxKey(maxKey);
                                minTuples.add(minTuple);
                                maxTuples.add(maxTuple);
                                newComponent = null;
                                componentBulkLoader = null;
                                minTuple = null;
                                maxTuple = null;
                                filterCmp = null;
                                minKey = null;
                                maxKey = null;
                            }
                        }
                        if (newComponent != null) {
                            newComponent.setMinKey(minKey);
                            newComponent.setMaxKey(maxKey);
                            minTuples.add(minTuple);
                            maxTuples.add(maxTuple);
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
                    btree.getFilterManager().updateFilter(newComponent.getLSMComponentFilter(), filterTuples,
                            NoOpOperationCallback.INSTANCE);
                    btree.getFilterManager().writeFilter(newComponent.getLSMComponentFilter(),
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
        if (((LSMBTree)index).hasBloomFilter()) {
            //count elements in btree for creating Bloomfilter
            for (int i = 0; i < mergedComponents.size(); ++i) {
                numElements += ((AbstractLSMWithBloomFilterDiskComponent) mergedComponents.get(i)).getBloomFilter()
                        .getNumElements();
            }
        }
        return numElements;
    }
}
