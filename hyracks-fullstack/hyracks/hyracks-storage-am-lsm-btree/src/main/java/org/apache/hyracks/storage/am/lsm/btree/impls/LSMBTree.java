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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.AbstractLSMWithBloomFilterDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor.ICursorFactory;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.trace.ITracer;

public class LSMBTree extends AbstractLSMIndex implements ITreeIndex {

    private static final ICursorFactory cursorFactory = LSMBTreeSearchCursor::new;
    // Common for in-memory and on-disk components.
    protected final ITreeIndexFrameFactory insertLeafFrameFactory;
    protected final ITreeIndexFrameFactory deleteLeafFrameFactory;
    protected final IBinaryComparatorFactory[] cmpFactories;
    private final boolean updateAware;

    private final boolean needKeyDupCheck;

    // Primary and Primary Key LSMBTree has a Bloomfilter, but Secondary one doesn't have.
    private final boolean hasBloomFilter;

    protected final LSMBTreeLevelMergePolicyHelper mergePolicyHelper;

    public LSMBTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, IBufferCache diskBufferCache,
            ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, boolean needKeyDupCheck, boolean hasBloomFilter,
            int[] btreeFields, int[] filterFields, boolean durable, boolean updateAware, ITracer tracer)
            throws HyracksDataException {
        super(ioManager, virtualBufferCaches, diskBufferCache, fileManager, bloomFilterFalsePositiveRate, mergePolicy,
                opTracker, ioScheduler, ioOpCallbackFactory, componentFactory, bulkLoadComponentFactory,
                filterFrameFactory, filterManager, filterFields, durable, filterHelper, btreeFields, tracer);
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.updateAware = updateAware;
        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            LSMBTreeMemoryComponent mutableComponent = new LSMBTreeMemoryComponent(this,
                    new BTree(virtualBufferCache, new VirtualFreePageManager(virtualBufferCache), interiorFrameFactory,
                            insertLeafFrameFactory, cmpFactories, fieldCount,
                            ioManager.resolveAbsolutePath(fileManager.getBaseDir() + "_virtual_" + i)),
                    virtualBufferCache, filterHelper == null ? null : filterHelper.createFilter());
            memoryComponents.add(mutableComponent);
            ++i;
        }
        this.needKeyDupCheck = needKeyDupCheck;
        this.hasBloomFilter = hasBloomFilter;
        if (isLeveled) {
            mergePolicyHelper = new LSMBTreeLevelMergePolicyHelper(this);
            levelMergePolicy.setHelper(mergePolicyHelper);
        } else {
            mergePolicyHelper = null;
        }
    }

    // Without memory components
    public LSMBTree(IIOManager ioManager, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, IBufferCache bufferCache, ILSMIndexFileManager fileManager,
            ILSMDiskComponentFactory componentFactory, ILSMDiskComponentFactory bulkLoadComponentFactory,
            double bloomFilterFalsePositiveRate, IBinaryComparatorFactory[] cmpFactories, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, boolean needKeyDupCheck, boolean durable,
            ITracer tracer) throws HyracksDataException {
        super(ioManager, bufferCache, fileManager, bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler,
                ioOpCallbackFactory, componentFactory, bulkLoadComponentFactory, durable, tracer);
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.needKeyDupCheck = needKeyDupCheck;
        this.hasBloomFilter = true;
        this.updateAware = false;
        if (isLeveled) {
            mergePolicyHelper = new LSMBTreeLevelMergePolicyHelper(this);
            levelMergePolicy.setHelper(mergePolicyHelper);
        } else {
            mergePolicyHelper = null;
        }
    }

    public boolean hasBloomFilter() {
        return this.hasBloomFilter;
    }

    @Override
    public boolean isPrimaryIndex() {
        return needKeyDupCheck;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        ITupleReference indexTuple;
        if (ctx.getIndexTuple() != null) {
            ctx.getIndexTuple().reset(tuple);
            indexTuple = ctx.getIndexTuple();
        } else {
            indexTuple = tuple;
        }

        switch (ctx.getOperation()) {
            case PHYSICALDELETE:
                ctx.getCurrentMutableBTreeAccessor().delete(indexTuple);
                break;
            case INSERT:
                insert(indexTuple, ctx);
                break;
            default:
                ctx.getCurrentMutableBTreeAccessor().upsert(indexTuple);
                break;
        }
        updateFilter(ctx, tuple);
    }

    private boolean insert(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException {
        LSMBTreePointSearchCursor searchCursor = ctx.getInsertSearchCursor();
        IIndexCursor memCursor = ctx.getMemCursor();
        RangePredicate predicate = (RangePredicate) ctx.getSearchPredicate();
        predicate.setHighKey(tuple);
        predicate.setLowKey(tuple);
        if (needKeyDupCheck) {
            // first check the inmemory component
            boolean found;
            ctx.getCurrentMutableBTreeAccessor().search(memCursor, predicate);
            try {
                found = memCursor.hasNext();
                if (found) {
                    memCursor.next();
                    LSMBTreeTupleReference lsmbtreeTuple = (LSMBTreeTupleReference) memCursor.getTuple();
                    if (!lsmbtreeTuple.isAntimatter()) {
                        throw HyracksDataException.create(ErrorCode.DUPLICATE_KEY);
                    }
                }
            } finally {
                memCursor.close();
            }
            if (found) {
                ctx.getCurrentMutableBTreeAccessor().upsertIfConditionElseInsert(tuple,
                        AntimatterAwareTupleAcceptor.INSTANCE);
                return true;
            }

            // TODO: Can we just remove the above code that search the mutable
            // component and do it together with the search call below? i.e. instead
            // of passing false to the lsmHarness.search(), we pass true to include
            // the mutable component?
            // the key was not in the inmemory component, so check the disk
            // components

            // This is a hack to avoid searching the current active mutable component twice. It is critical to add it back once the search is over.
            ILSMComponent firstComponent = ctx.getComponentHolder().remove(0);
            search(ctx, searchCursor, predicate);
            try {
                if (searchCursor.hasNext()) {
                    throw HyracksDataException.create(ErrorCode.DUPLICATE_KEY);
                }
            } finally {
                searchCursor.close();
                // Add the current active mutable component back
                ctx.getComponentHolder().add(0, firstComponent);
            }
        }
        ctx.getCurrentMutableBTreeAccessor().upsertIfConditionElseInsert(tuple, AntimatterAwareTupleAcceptor.INSTANCE);
        return true;
    }

    @Override
    public boolean mayMatchSearchPredicate(ILSMDiskComponent component, ISearchPredicate predicate) {
        RangePredicate rp = (RangePredicate) predicate;
        byte[] minKey = getTupleKey(rp.getLowKey());
        byte[] maxKey = getTupleKey(rp.getHighKey());
        if (minKey == null && maxKey == null) {
            return true;
        }
        try {
            byte[] minCKey = component.getMinKey();
            byte[] maxCKey = component.getMaxKey();

            if (minKey == null) {
                if (LSMBTree.compareBytes(maxKey, minCKey) >= 0) {
                    return true;
                }
            } else if (maxKey == null) {
                if (LSMBTree.compareBytes(minKey, maxCKey) <= 0) {
                    return true;
                }
            } else {
                if (!(LSMBTree.compareBytes(minKey, maxCKey) > 0 || LSMBTree.compareBytes(maxKey, minCKey) < 0)) {
                    return true;
                }
            }
        } catch (HyracksDataException ex) {
            return true;
        }
        return false;
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        ctx.getSearchInitialState().reset(pred, operationalComponents);
        cursor.open(ctx.getSearchInitialState(), pred);
    }

    @Override
    public void scanDiskComponents(ILSMIndexOperationContext ictx, IIndexCursor cursor) throws HyracksDataException {
        if (!isPrimaryIndex()) {
            throw HyracksDataException.create(ErrorCode.DISK_COMPONENT_SCAN_NOT_ALLOWED_FOR_SECONDARY_INDEX);
        }
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        MultiComparator comp = MultiComparator.create(getComparatorFactories());
        ISearchPredicate pred = new RangePredicate(null, null, true, true, comp, comp);
        ctx.getSearchInitialState().reset(pred, operationalComponents);
        ctx.getSearchInitialState().setDiskComponentScan(true);
        ((LSMBTreeSearchCursor) cursor).open(ctx.getSearchInitialState(), pred);
    }

    public static int compareBytes(byte[] thisBytes, byte[] thatBytes) {
        int thisLen = thisBytes == null ? 0 : thisBytes.length;
        int thatLen = thatBytes == null ? 0 : thatBytes.length;
        for (int i = 0; i < thisLen && i < thatLen; i++) {
            byte b1 = thisBytes[i];
            byte b2 = thatBytes[i];
            if (b1 < b2) {
                return -1;
            } else if (b1 > b2) {
                return 1;
            } else {
            }
        }
        return thisLen - thatLen;
    }

    @Override
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTreeFlushOperation flushOp = (LSMBTreeFlushOperation) operation;
        LSMBTreeMemoryComponent flushingComponent = (LSMBTreeMemoryComponent) flushOp.getFlushingComponent();
        IIndexAccessor accessor = flushingComponent.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        ILSMDiskComponent component;
        ILSMDiskComponentBulkLoader componentBulkLoader;

        try {
            RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
            long numElements = 0L;
            if (hasBloomFilter) {
                //count elements in btree for creating Bloomfilter
                IIndexCursor countingCursor = ((BTreeAccessor) accessor).createCountingSearchCursor();
                accessor.search(countingCursor, nullPred);
                try {
                    while (countingCursor.hasNext()) {
                        countingCursor.next();
                        ITupleReference countTuple = countingCursor.getTuple();
                        numElements =
                                IntegerPointable.getInteger(countTuple.getFieldData(0), countTuple.getFieldStart(0));
                    }
                } finally {
                    try {
                        countingCursor.close();
                    } finally {
                        countingCursor.destroy();
                    }
                }
            }
            component = createDiskComponent(componentFactory, flushOp.getTarget(), null, flushOp.getBloomFilterTarget(),
                    true);
            componentBulkLoader = component.createBulkLoader(operation, 1.0f, false, numElements, false, false, false);
            IIndexCursor scanCursor = accessor.createSearchCursor(false);
            accessor.search(scanCursor, nullPred);
            byte[] minKey = null;
            byte[] maxKey = null;
            try {
                while (scanCursor.hasNext()) {
                    scanCursor.next();
                    ITupleReference tuple = scanCursor.getTuple();
                    // we can safely throw away updated tuples in secondary BTree components, because they correspond to
                    // deleted tuples
                    if (updateAware && ((LSMBTreeTupleReference) tuple).isUpdated()) {
                        continue;
                    }
                    componentBulkLoader.add(tuple);
                    byte[] key = getTupleKey(tuple);
                    if (key != null) {
                        if (minKey == null) {
                            minKey = key;
                        } else {
                            if (compareBytes(key, minKey) < 0) {
                                minKey = key;
                            }
                        }
                        if (maxKey == null) {
                            maxKey = key;
                        } else {
                            if (compareBytes(key, maxKey) > 0) {
                                maxKey = key;
                            }
                        }
                    }
                }
            } finally {
                try {
                    scanCursor.close();
                } finally {
                    scanCursor.destroy();
                }
            }
            component.setMinKey(minKey);
            component.setMaxKey(maxKey);
        } finally {
            accessor.destroy();
        }
        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMinTuple());
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMaxTuple());
            getFilterManager().updateFilter(component.getLSMComponentFilter(), filterTuples,
                    NoOpOperationCallback.INSTANCE);
            getFilterManager().writeFilter(component.getLSMComponentFilter(), component.getMetadataHolder());
        }
        // Write metadata from memory component to disk
        // Q. what about the merge operation? how do we resolve conflicts
        // A. Through providing an appropriate ILSMIOOperationCallback
        // Must not reset the metadata before the flush is completed
        // Use the copy of the metadata in the opContext
        // TODO This code should be in the callback and not in the index
        flushingComponent.getMetadata().copy(component.getMetadata());
        componentBulkLoader.end();
        return component;
    }

    private ILSMDiskComponent stackedMerge(LSMBTreeMergeOperation mergeOp) throws HyracksDataException {
        IIndexCursor cursor = mergeOp.getCursor();
        ILSMDiskComponent newComponent = null;
        ILSMDiskComponentBulkLoader componentBulkLoader = null;
        ITupleReference minTuple = null;
        ITupleReference maxTuple = null;
        try {
            try {
                RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
                search(mergeOp.getAccessor().getOpContext(), cursor, rangePred);
                try {
                    List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();
                    long numElements = getNumberOfElements(mergedComponents);
                    newComponent = createDiskComponent(componentFactory, mergeOp.getTarget(), null,
                            mergeOp.getBloomFilterTarget(), true);
                    componentBulkLoader =
                            newComponent.createBulkLoader(mergeOp, 1.0f, false, numElements, false, false, false);
                    byte[] minKey = null;
                    byte[] maxKey = null;
                    while (cursor.hasNext()) {
                        cursor.next();
                        ITupleReference frameTuple = cursor.getTuple();
                        componentBulkLoader.add(frameTuple);
                        byte[] key = getTupleKey(frameTuple);
                        if (key != null) {
                            if (minKey == null) {
                                minKey = key;
                            } else {
                                if (compareBytes(key, minKey) < 0) {
                                    minKey = key;
                                }
                            }
                            if (maxKey == null) {
                                maxKey = key;
                            } else {
                                if (compareBytes(key, maxKey) > 0) {
                                    maxKey = key;
                                }
                            }
                        }
                    }
                    newComponent.setMinKey(minKey);
                    newComponent.setMaxKey(maxKey);
                    if (newComponent.getLSMComponentFilter() != null) {
                        MultiComparator filterCmp =
                                MultiComparator.create(newComponent.getLSMComponentFilter().getFilterCmpFactories());
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
                    }
                } finally {
                    cursor.close();
                }
            } finally {
                cursor.destroy();
            }
            if (newComponent.getLSMComponentFilter() != null) {
                List<ITupleReference> filterTuples = Arrays.asList(minTuple, maxTuple);
                //                    for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                //                        filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMinTuple());
                //                        filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMaxTuple());
                //                    }
                getFilterManager().updateFilter(newComponent.getLSMComponentFilter(), filterTuples,
                        NoOpOperationCallback.INSTANCE);
                getFilterManager().writeFilter(newComponent.getLSMComponentFilter(), newComponent.getMetadataHolder());
            }
        } catch (Throwable e) { // NOSONAR.. As per the contract, we should either abort or end
            try {
                componentBulkLoader.abort();
            } catch (Throwable th) { // NOSONAR Don't lose the root failure
                e.addSuppressed(th);
            }
            throw e;
        }
        componentBulkLoader.end();
        return newComponent;
    }

    @Override
    public List<ILSMDiskComponent> doMerge(ILSMIOOperation operation) throws HyracksDataException {
        if (isLeveled) {
            return mergePolicyHelper.merge(operation);
        } else {
            return Collections.singletonList(stackedMerge((LSMBTreeMergeOperation) operation));
        }
    }

    private long getNumberOfElements(List<ILSMComponent> mergedComponents) throws HyracksDataException {
        long numElements = 0L;
        if (hasBloomFilter) {
            //count elements in btree for creating Bloomfilter
            for (int i = 0; i < mergedComponents.size(); ++i) {
                numElements += ((AbstractLSMWithBloomFilterDiskComponent) mergedComponents.get(i)).getBloomFilter()
                        .getNumElements();
            }
        }
        return numElements;
    }

    @Override
    protected ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences componentFileRefs, ILSMIOOperationCallback callback) {
        ILSMIndexAccessor accessor = createAccessor(opCtx);
        return new LSMBTreeFlushOperation(accessor, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getBloomFilterFileReference(), callback, getIndexIdentifier());
    }

    @Override
    public LSMBTreeOpContext createOpContext(IIndexAccessParameters iap) {
        int numBloomFilterKeyFields = hasBloomFilter
                ? ((LSMBTreeWithBloomFilterDiskComponentFactory) componentFactory).getBloomFilterKeyFields().length : 0;
        return new LSMBTreeOpContext(this, memoryComponents, insertLeafFrameFactory, deleteLeafFrameFactory,
                (IExtendedModificationOperationCallback) iap.getModificationCallback(),
                iap.getSearchOperationCallback(), numBloomFilterKeyFields, getTreeFields(), getFilterFields(),
                getHarness(), getFilterCmpFactories(), tracer);
    }

    @Override
    public ILSMIndexAccessor createAccessor(IIndexAccessParameters iap) {
        return createAccessor(createOpContext(iap));
    }

    public ILSMIndexAccessor createAccessor(AbstractLSMIndexOperationContext opCtx) {
        return new LSMTreeIndexAccessor(getHarness(), opCtx, cursorFactory);
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getInteriorFrameFactory();
    }

    @Override
    public int getFieldCount() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getFieldCount();
    }

    @Override
    public int getFileId() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getFileId();
    }

    @Override
    public IPageManager getPageManager() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getPageManager();
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getLeafFrameFactory();
    }

    @Override
    public int getRootPageId() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getRootPageId();
    }

    @Override
    protected LSMComponentFileReferences getMergeFileReferences(List<ILSMDiskComponent> components)
            throws HyracksDataException {
        if (isLeveled) {
            long levelFrom = components.get(0).getLevel();
            long levelTo = components.get(components.size() - 1).getLevel();
            if (levelFrom == levelTo) {
                // Move to the next level
                String newName = (levelTo + 1) + AbstractLSMIndexFileManager.DELIMITER + "1";
                return fileManager.getRelMergeFileReference(newName);
            } else {
                long maxLevelId = getMaxLevelId(levelTo);
                String newName = levelTo + AbstractLSMIndexFileManager.DELIMITER + (maxLevelId + 1);
                return fileManager.getRelMergeFileReference(newName);
            }
        } else {
            BTree firstBTree = (BTree) components.get(0).getIndex();
            BTree lastBTree = (BTree) components.get(components.size() - 1).getIndex();
            FileReference firstFile = firstBTree.getFileReference();
            FileReference lastFile = lastBTree.getFileReference();
            return fileManager.getRelMergeFileReference(firstFile.getFile().getName(), lastFile.getFile().getName());
        }
    }

    @Override
    protected ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences mergeFileRefs, ILSMIOOperationCallback callback) {
        boolean returnDeletedTuples = false;
        List<ILSMComponent> mergingComponents = opCtx.getComponentHolder();
        if (!isLeveled && mergingComponents.get(mergingComponents.size() - 1) != diskComponents
                .get(diskComponents.size() - 1)) {
            returnDeletedTuples = true;
        }
        ILSMIndexAccessor accessor = createAccessor(opCtx);
        LSMBTreeRangeSearchCursor cursor = new LSMBTreeRangeSearchCursor(opCtx, returnDeletedTuples);
        return new LSMBTreeMergeOperation(accessor, cursor, mergeFileRefs.getInsertIndexFileReference(),
                mergeFileRefs.getBloomFilterFileReference(), callback, getIndexIdentifier());
    }

    public static byte[] getLongBytesFromTuple(ITupleReference tuple) {
        byte[] key = getTupleKey(tuple);
        int l = key == null ? 0 : key.length;
        if (key.length < Long.BYTES) {
            return null;
        }
        return Arrays.copyOfRange(key, l - Long.BYTES, l);
    }

    public static long bytesToLong(byte[] bytes) {
        if (bytes == null || bytes.length < Long.BYTES) {
            return Long.MAX_VALUE;
        }
        return ByteBuffer.wrap(bytes, bytes.length - Long.BYTES, Long.BYTES).getLong();
    }

    public static long getLongFromTuple(ITupleReference tuple) {
        byte[] key = getTupleKey(tuple);
        int l = key == null ? 0 : key.length;
        if (key.length < Long.BYTES) {
            return Long.MAX_VALUE;
        }
        return ByteBuffer.wrap(key, l - Long.BYTES, Long.BYTES).getLong();
    }

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    @Override
    public String componentToString(ILSMDiskComponent component) {
        String basename;
        String minKey;
        String maxKey;
        try {
            basename = component.getId().toString();
        } catch (HyracksDataException ex) {
            basename = "Unknown";
        }
        try {
            byte[] minData = component.getMinKey();
            if (minData == null) {
                minKey = "Unknown";
            } else {
                int l = minData.length;
                if (l >= Long.BYTES) {
                    minKey = Long.toString(bytesToLong(minData));
                } else {
                    minKey = bytesToHex(minData);
                }
            }
        } catch (HyracksDataException ex) {
            minKey = "Unknown";
        }
        try {
            byte[] maxData = component.getMaxKey();
            if (maxData == null) {
                maxKey = "Unknown";
            } else {
                int l = maxData.length;
                if (l >= Long.BYTES) {
                    maxKey = Long.toString(bytesToLong(maxData));
                } else {
                    maxKey = bytesToHex(maxData);
                }
            }
        } catch (HyracksDataException ex) {
            maxKey = "Unknown";
        }
        return "{ name: " + basename + ", size: " + component.getComponentSize() + ", min: " + minKey + ", max: "
                + maxKey + " }";
    }
}
