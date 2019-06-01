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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LSMBTree extends AbstractLSMIndex implements ITreeIndex {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final ICursorFactory cursorFactory = LSMBTreeSearchCursor::new;
    // Common for in-memory and on-disk components.
    protected final ITreeIndexFrameFactory insertLeafFrameFactory;
    protected final ITreeIndexFrameFactory deleteLeafFrameFactory;
    protected final IBinaryComparatorFactory[] cmpFactories;
    private final boolean updateAware;

    private final boolean needKeyDupCheck;

    // Primary and Primary Key LSMBTree has a Bloomfilter, but Secondary one doesn't have.
    private final boolean hasBloomFilter;

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

    public static byte[] getTuplePK(ITupleReference tuple) {
        int st = tuple.getFieldStart(0);
        int l = tuple.getFieldLength(0);
        return Arrays.copyOfRange(tuple.getFieldData(0), st, st + l);
    }

    public static String getTupleId(LSMBTreeTupleReference t) {
        int st = t.getFieldStart(0);
        int l = t.getFieldLength(0);
        byte[] data = Arrays.copyOfRange(t.getFieldData(0), st, st + l);
        return bytesToHex(data);
        //        return IntegerPointable.getInteger(data, 0);
    }

    public static String tuple2str(ITupleReference t) {
        if (t instanceof LSMBTreeTupleReference) {
            LSMBTreeTupleReference bt = (LSMBTreeTupleReference) t;
            return "id=" + getTupleId(bt);
            //            return bt.getTupleStart() + "-" + bt.getFieldStart(0) + "-" + bt.getFieldLength(0) + ":" + bytesToHex(
            //                    Arrays.copyOfRange(bt.getFieldData(0), bt.getTupleStart(), bt.getTupleStart() + bt.getTupleSize()));
            ////            if (bt.getFieldCount() == 0) {
            ////                return bt.getClass().getSimpleName();
            ////            }
            ////            String m = bt.getClass().getSimpleName() + ":id=" + getTupleId(bt) + ":" + bt.getFieldCount() + ":"
            ////                    + bt.getTupleStart() + ":" + bt.getTupleSize() + ";0:" + bt.getFieldStart(0) + ":"
            ////                    + bt.getFieldLength(0);
            ////            for (int i = 1; i < t.getFieldCount(); i++) {
            ////                m += ";" + i + ":" + bt.getFieldStart(i) + ":" + bt.getFieldLength(i);
            ////            }
            //            return m;
        }
        return t.getClass().getSimpleName();
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

    @Override
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTreeFlushOperation flushOp = (LSMBTreeFlushOperation) operation;
        LSMBTreeMemoryComponent flushingComponent = (LSMBTreeMemoryComponent) flushOp.getFlushingComponent();
        IIndexAccessor accessor = flushingComponent.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        ILSMDiskComponent component;
        ILSMDiskComponentBulkLoader componentBulkLoader;

        ITupleReference minTuple = null;
        byte[] minPK = null;
        ITupleReference maxTuple = null;
        byte[] maxPK = null;

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
                    if (fileManager.getBaseDir().getAbsolutePath().contains("spatial_table")) {
                        byte[] pk = getTuplePK(tuple);
                        if (minTuple == null) {
                            minTuple = tuple;
                            minPK = pk;
                        } else {
                            if (ByteArrayPointable.compare(pk, 0, pk.length, minPK, 0, minPK.length) < 0) {
                                minTuple = tuple;
                                minPK = pk;
                            }
                        }
                        if (maxTuple == null) {
                            maxTuple = tuple;
                            maxPK = pk;
                        } else {
                            if (ByteArrayPointable.compare(maxPK, 0, maxPK.length, pk, 0, pk.length) <= 0) {
                                maxTuple = tuple;
                                maxPK = pk;
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
        } finally {
            accessor.destroy();
        }
        if (minTuple != null) {
            LOGGER.info("[doFlush]\tminTuple = " + bytesToHex(minPK));
        }
        if (maxTuple != null) {
            LOGGER.info("[doFlush]\tmaxTuple = " + bytesToHex(maxPK));
        }
        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMinTuple());
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMaxTuple());
            if (fileManager.getBaseDir().getAbsolutePath().contains("spatial_table")) {
                LOGGER.info("[doFlush]\tminFilter = "
                        + bytesToHex(getTuplePK(flushingComponent.getLSMComponentFilter().getMinTuple())));
                LOGGER.info("[doFlush]\tmaxFilter = "
                        + bytesToHex(getTuplePK(flushingComponent.getLSMComponentFilter().getMaxTuple())));
            }
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

    private String diskComponentToString(ILSMComponent c) {
        if (c instanceof ILSMDiskComponent) {
            ILSMDiskComponent d = (ILSMDiskComponent) c;
            return d.getLevel() + "_" + d.getLevelSequence() + ":" + d.getComponentSize();
        }
        return "";
    }

    //    private ILSMDiskComponent stackedMerge(List<ILSMComponent> components, LSMBTreeMergeOperation mergeOp)
    //            throws HyracksDataException {
    //        IIndexCursor cursor = mergeOp.getCursor();
    //        ILSMDiskComponent newComponent = null;
    //        ILSMDiskComponentBulkLoader componentBulkLoader = null;
    //        ITupleReference minTuple = null;
    //        ITupleReference maxTuple = null;
    //        try {
    //            try {
    //                RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
    //                search(mergeOp.getAccessor().getOpContext(), cursor, rangePred);
    //                try {
    //                    long numElements = getNumberOfElements(components);
    //                    newComponent = createDiskComponent(componentFactory, mergeOp.getTarget(), null,
    //                            mergeOp.getBloomFilterTarget(), true);
    //                    componentBulkLoader =
    //                            newComponent.createBulkLoader(mergeOp, 1.0f, false, numElements, false, false, false);
    //                    while (cursor.hasNext()) {
    //                        cursor.next();
    //                        ITupleReference frameTuple = cursor.getTuple();
    //                        componentBulkLoader.add(frameTuple);
    //                    }
    //                    if (newComponent.getLSMComponentFilter() != null) {
    //                        MultiComparator filterCmp =
    //                                MultiComparator.create(newComponent.getLSMComponentFilter().getFilterCmpFactories());
    //                        for (ILSMComponent component : components) {
    //                            ITupleReference minMergeTuple = component.getLSMComponentFilter().getMinTuple();
    //                            ITupleReference maxMergeTuple = component.getLSMComponentFilter().getMaxTuple();
    //                            if (minTuple == null) {
    //                                minTuple = minMergeTuple;
    //                            } else {
    //                                if (filterCmp.compare(minMergeTuple, minTuple) < 0) {
    //                                    minTuple = minMergeTuple;
    //                                }
    //                            }
    //                            if (maxTuple == null) {
    //                                maxTuple = maxMergeTuple;
    //                            } else {
    //                                if (filterCmp.compare(maxMergeTuple, maxTuple) > 0) {
    //                                    maxTuple = maxMergeTuple;
    //                                }
    //                            }
    //                        }
    //                    }
    //                } finally {
    //                    cursor.close();
    //                }
    //            } finally {
    //                cursor.destroy();
    //            }
    //            if (newComponent != null && newComponent.getLSMComponentFilter() != null) {
    //                List<ITupleReference> filterTuples = Arrays.asList(minTuple, maxTuple);
    //                //                    for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
    //                //                        filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMinTuple());
    //                //                        filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMaxTuple());
    //                //                    }
    //                getFilterManager().updateFilter(newComponent.getLSMComponentFilter(), filterTuples,
    //                        NoOpOperationCallback.INSTANCE);
    //                getFilterManager().writeFilter(newComponent.getLSMComponentFilter(), newComponent.getMetadataHolder());
    //            }
    //        } catch (Throwable e) { // NOSONAR.. As per the contract, we should either abort or end
    //            try {
    //                if (componentBulkLoader != null) {
    //                    componentBulkLoader.abort();
    //                }
    //            } catch (Throwable th) { // NOSONAR Don't lose the root failure
    //                e.addSuppressed(th);
    //            }
    //            throw e;
    //        }
    //        if (componentBulkLoader != null) {
    //            componentBulkLoader.end();
    //        }
    //        return newComponent;
    //    }
    //
    //    private List<ILSMDiskComponent> leveledMerge(List<ILSMComponent> components, LSMBTreeMergeOperation mergeOp)
    //            throws HyracksDataException {
    //        LSMBTreeMergeOperation mergeOp = (LSMBTreeMergeOperation) operation;
    //        if (isLeveled) {
    //            return leveledMerge(mergeOp.getMergingComponents(), mergeOp);
    //        } else {
    //            ILSMDiskComponent newComponent = stackedMerge(mergeOp.getMergingComponents(), mergeOp);
    //            return newComponent == null ? Collections.emptyList() : Collections.singletonList(newComponent);
    //        }
    //
    //        IIndexCursor cursor = mergeOp.getCursor();
    //        List<ILSMDiskComponent> newComponents = new ArrayList<>();
    //        List<ILSMDiskComponentBulkLoader> componentBulkLoaders = new ArrayList<>();
    //        Map<ILSMDiskComponent, Pair<ITupleReference, ITupleReference>> tuples = new HashMap<>();
    //        String oldMsg = "";
    //        try {
    //            try {
    //                RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
    //                search(mergeOp.getAccessor().getOpContext(), cursor, rangePred);
    //                try {
    //                    List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();
    //                    oldMsg = diskComponentToString(mergedComponents.get(0));
    //                    for (int i = 1; i < mergedComponents.size(); i++) {
    //                        oldMsg += "; " + diskComponentToString(mergedComponents.get(i));
    //                    }
    //                    long numElements = getNumberOfElements(mergedComponents);
    //                    if (isLeveled) {
    //                        ILSMDiskComponent newComponent = null;
    //                        ILSMDiskComponentBulkLoader componentBulkLoader = null;
    //                        if (mergedComponents.size() == 1) {
    //                            LSMComponentFileReferences refs = getNextMergeFileReferencesAtLevel(maxLevels + 1, 0);
    //                            newComponent = createDiskComponent(componentFactory, refs.getInsertIndexFileReference(),
    //                                    null, refs.getBloomFilterFileReference(), true);
    //                            componentBulkLoader = newComponent.createBulkLoader(operation, 1.0f, false, numElements,
    //                                    false, false, false);
    //                            componentBulkLoaders.add(componentBulkLoader);
    //                            while (cursor.hasNext()) {
    //                                cursor.next();
    //                                ITupleReference frameTuple = cursor.getTuple();
    //                                componentBulkLoader.add(frameTuple);
    //                            }
    //                            newComponents.add(newComponent);
    //                            ITupleReference minTuple = mergedComponents.get(0).getLSMComponentFilter().getMinTuple();
    //                            ITupleReference maxTuple = mergedComponents.get(0).getLSMComponentFilter().getMaxTuple();
    //                            tuples.put(newComponent, Pair.of(minTuple, maxTuple));
    //                        } else if (((ILSMDiskComponent) (mergedComponents.get(mergedComponents.size() - 1)))
    //                                .getLevel() == 0L) {
    //                            LSMComponentFileReferences refs = getNextMergeFileReferencesAtLevel(1L, 1);
    //                            newComponent = createDiskComponent(componentFactory, refs.getInsertIndexFileReference(),
    //                                    null, refs.getBloomFilterFileReference(), true);
    //                            componentBulkLoader = newComponent.createBulkLoader(operation, 1.0f, false, numElements,
    //                                    false, false, false);
    //                            componentBulkLoaders.add(componentBulkLoader);
    //                            while (cursor.hasNext()) {
    //                                cursor.next();
    //                                ITupleReference frameTuple = cursor.getTuple();
    //                                componentBulkLoader.add(frameTuple);
    //                            }
    //                            newComponents.add(newComponent);
    //                            if (newComponent.getLSMComponentFilter() != null) {
    //                                ITupleReference minTuple = null;
    //                                ITupleReference maxTuple = null;
    //                                MultiComparator filterCmp = MultiComparator
    //                                        .create(newComponent.getLSMComponentFilter().getFilterCmpFactories());
    //                                for (ILSMComponent component : mergedComponents) {
    //                                    ITupleReference minMergeTuple = component.getLSMComponentFilter().getMinTuple();
    //                                    ITupleReference maxMergeTuple = component.getLSMComponentFilter().getMaxTuple();
    //                                    if (minTuple == null) {
    //                                        minTuple = minMergeTuple;
    //                                    } else {
    //                                        if (filterCmp.compare(minMergeTuple, minTuple) < 0) {
    //                                            minTuple = minMergeTuple;
    //                                        }
    //                                    }
    //                                    if (maxTuple == null) {
    //                                        maxTuple = maxMergeTuple;
    //                                    } else {
    //                                        if (filterCmp.compare(maxMergeTuple, maxTuple) > 0) {
    //                                            maxTuple = maxMergeTuple;
    //                                        }
    //                                    }
    //                                }
    //                                tuples.put(newComponent, Pair.of(minTuple, maxTuple));
    //                            }
    //                        } else {
    //                            ITupleReference minTuple = null;
    //                            ITupleReference maxTuple = null;
    //                            MultiComparator filterCmp = null;
    //                            long levelTo =
    //                                    ((ILSMDiskComponent) mergedComponents.get(mergedComponents.size() - 1)).getLevel();
    //                            long start = getMaxLevelId(levelTo) + 1;
    //                            List<FileReference> mergeFileTargets = new ArrayList<>();
    //                            List<FileReference> mergeBloomFilterTargets = new ArrayList<>();
    //                            while (cursor.hasNext()) {
    //                                cursor.next();
    //                                ITupleReference frameTuple = cursor.getTuple();
    //                                if (newComponent == null) {
    //                                    LSMComponentFileReferences refs =
    //                                            getNextMergeFileReferencesAtLevel(levelTo, start++);
    //                                    mergeFileTargets.add(refs.getInsertIndexFileReference());
    //                                    mergeBloomFilterTargets.add(refs.getBloomFilterFileReference());
    //                                    newComponent =
    //                                            createDiskComponent(componentFactory, refs.getInsertIndexFileReference(),
    //                                                    null, refs.getBloomFilterFileReference(), true);
    //                                    componentBulkLoader = newComponent.createBulkLoader(operation, 1.0f, false,
    //                                            numElements, false, false, false);
    //                                    newComponents.add(newComponent);
    //                                    componentBulkLoaders.add(componentBulkLoader);
    //                                    filterCmp = MultiComparator
    //                                            .create(newComponent.getLSMComponentFilter().getFilterCmpFactories());
    //                                    minTuple = null;
    //                                    maxTuple = null;
    //                                }
    //                                if (minTuple == null) {
    //                                    minTuple = frameTuple;
    //                                } else {
    //                                    if (filterCmp.compare(frameTuple, minTuple) < 0) {
    //                                        minTuple = frameTuple;
    //                                    }
    //                                }
    //                                if (maxTuple == null) {
    //                                    maxTuple = frameTuple;
    //                                } else {
    //                                    if (filterCmp.compare(frameTuple, maxTuple) > 0) {
    //                                        maxTuple = frameTuple;
    //                                    }
    //                                }
    //                                componentBulkLoader.add(frameTuple);
    //                                if (newComponent.getComponentSize() >= levelTableSize) {
    //                                    tuples.put(newComponent, Pair.of(minTuple, maxTuple));
    //                                    newComponent = null;
    //                                    componentBulkLoader = null;
    //                                    minTuple = null;
    //                                    maxTuple = null;
    //                                    filterCmp = null;
    //                                }
    //                            }
    //                            if (newComponent != null) {
    //                                tuples.put(newComponent, Pair.of(minTuple, maxTuple));
    //                            }
    //                            mergeOp.setTargets(mergeFileTargets);
    //                            mergeOp.setBloomFilterTargets(mergeBloomFilterTargets);
    //                        }
    //                    } else {
    //                    }
    //                } finally {
    //                    cursor.close();
    //                }
    //            } finally {
    //                cursor.destroy();
    //            }
    //            for (ILSMDiskComponent newComponent : newComponents) {
    //                if (newComponent.getLSMComponentFilter() != null) {
    //                    Pair<ITupleReference, ITupleReference> minMaxTuples = tuples.get(newComponent);
    //                    List<ITupleReference> filterTuples = Arrays.asList(minMaxTuples.getLeft(), minMaxTuples.getRight());
    //                    //                    for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
    //                    //                        filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMinTuple());
    //                    //                        filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMaxTuple());
    //                    //                    }
    //                    getFilterManager().updateFilter(newComponent.getLSMComponentFilter(), filterTuples,
    //                            NoOpOperationCallback.INSTANCE);
    //                    getFilterManager().writeFilter(newComponent.getLSMComponentFilter(),
    //                            newComponent.getMetadataHolder());
    //                }
    //            }
    //        } catch (Throwable e) { // NOSONAR.. As per the contract, we should either abort or end
    //            try {
    //                for (ILSMDiskComponentBulkLoader componentBulkLoader : componentBulkLoaders) {
    //                    componentBulkLoader.abort();
    //                }
    //            } catch (Throwable th) { // NOSONAR Don't lose the root failure
    //                e.addSuppressed(th);
    //            }
    //            throw e;
    //        }
    //        for (ILSMDiskComponentBulkLoader componentBulkLoader : componentBulkLoaders) {
    //            componentBulkLoader.end();
    //        }
    //
    //
    //        return null;
    //    }

    @Override
    public List<ILSMDiskComponent> doMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTreeMergeOperation mergeOp = (LSMBTreeMergeOperation) operation;
        //        if (isLeveled) {
        //            return leveledMerge(mergeOp.getMergingComponents(), mergeOp);
        //        } else {
        //            ILSMDiskComponent newComponent = stackedMerge(mergeOp.getMergingComponents(), mergeOp);
        //            return newComponent == null ? Collections.emptyList() : Collections.singletonList(newComponent);
        //        }

        IIndexCursor cursor = mergeOp.getCursor();
        List<ILSMDiskComponent> newComponents = new ArrayList<>();
        List<ILSMDiskComponentBulkLoader> componentBulkLoaders = new ArrayList<>();
        Map<ILSMDiskComponent, Pair<ITupleReference, ITupleReference>> tuples = new HashMap<>();
        String oldMsg = "";
        try {
            try {
                RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
                search(mergeOp.getAccessor().getOpContext(), cursor, rangePred);
                try {
                    List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();
                    oldMsg = diskComponentToString(mergedComponents.get(0));
                    for (int i = 1; i < mergedComponents.size(); i++) {
                        oldMsg += "; " + diskComponentToString(mergedComponents.get(i));
                    }
                    long numElements = getNumberOfElements(mergedComponents);
                    if (isLeveled) {
                        ILSMDiskComponent newComponent = null;
                        ILSMDiskComponentBulkLoader componentBulkLoader = null;
                        if (mergedComponents.size() == 1) {
                            LSMComponentFileReferences refs = getNextMergeFileReferencesAtLevel(
                                    ((ILSMDiskComponent) mergedComponents.get(0)).getLevel() + 1, 1);
                            newComponent = createDiskComponent(componentFactory, refs.getInsertIndexFileReference(),
                                    null, refs.getBloomFilterFileReference(), true);
                            componentBulkLoader = newComponent.createBulkLoader(operation, 1.0f, false, numElements,
                                    false, false, false);
                            componentBulkLoaders.add(componentBulkLoader);
                            while (cursor.hasNext()) {
                                cursor.next();
                                ITupleReference frameTuple = cursor.getTuple();
                                componentBulkLoader.add(frameTuple);
                            }
                            newComponents.add(newComponent);
                            if (newComponent.getLSMComponentFilter() != null) {
                                ITupleReference minTuple =
                                        mergedComponents.get(0).getLSMComponentFilter().getMinTuple();
                                ITupleReference maxTuple =
                                        mergedComponents.get(0).getLSMComponentFilter().getMaxTuple();
                                tuples.put(newComponent, Pair.of(minTuple, maxTuple));
                            }
                        } else if (((ILSMDiskComponent) (mergedComponents.get(mergedComponents.size() - 1)))
                                .getLevel() == 0L) {
                            LSMComponentFileReferences refs = getNextMergeFileReferencesAtLevel(1L, 1);
                            newComponent = createDiskComponent(componentFactory, refs.getInsertIndexFileReference(),
                                    null, refs.getBloomFilterFileReference(), true);
                            componentBulkLoader = newComponent.createBulkLoader(operation, 1.0f, false, numElements,
                                    false, false, false);
                            componentBulkLoaders.add(componentBulkLoader);
                            while (cursor.hasNext()) {
                                cursor.next();
                                ITupleReference frameTuple = cursor.getTuple();
                                componentBulkLoader.add(frameTuple);
                            }
                            newComponents.add(newComponent);
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
                                tuples.put(newComponent, Pair.of(minTuple, maxTuple));
                            }
                        } else {
                            ITupleReference minTuple = null;
                            ITupleReference maxTuple = null;
                            MultiComparator filterCmp = null;
                            long levelTo =
                                    ((ILSMDiskComponent) mergedComponents.get(mergedComponents.size() - 1)).getLevel();
                            LOGGER.info("[levelTo]\t" + levelTo);
                            long start = getMaxLevelId(levelTo);
                            if (start < 1) {
                                start = 1;
                            }
                            List<FileReference> mergeFileTargets = new ArrayList<>();
                            List<FileReference> mergeBloomFilterTargets = new ArrayList<>();
                            while (cursor.hasNext()) {
                                cursor.next();
                                ITupleReference frameTuple = cursor.getTuple();
                                if (newComponent == null) {
                                    LSMComponentFileReferences refs =
                                            getNextMergeFileReferencesAtLevel(levelTo, start++);
                                    mergeFileTargets.add(refs.getInsertIndexFileReference());
                                    mergeBloomFilterTargets.add(refs.getBloomFilterFileReference());
                                    newComponent =
                                            createDiskComponent(componentFactory, refs.getInsertIndexFileReference(),
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
                                if (newComponent.getComponentSize() >= levelTableSize) {
                                    tuples.put(newComponent, Pair.of(minTuple, maxTuple));
                                    newComponent = null;
                                    componentBulkLoader = null;
                                    minTuple = null;
                                    maxTuple = null;
                                    filterCmp = null;
                                }
                            }
                            if (maxTuple != null) {
                                LOGGER.info("[maxTuple]\t" + maxTuple.toString());
                                LOGGER.info("[maxTuple]\t" + maxTuple.getFieldCount());
                                for (int i = 0; i < maxTuple.getFieldCount(); i++) {
                                    ByteBuffer bb = ByteBuffer.wrap(maxTuple.getFieldData(i));

                                    LOGGER.info("[maxTuple]\t" + i + " " + bb + " " + bb.getLong());
                                }
                            }
                            if (newComponent != null) {
                                tuples.put(newComponent, Pair.of(minTuple, maxTuple));
                            }
                            mergeOp.setTargets(mergeFileTargets);
                            mergeOp.setBloomFilterTargets(mergeBloomFilterTargets);
                        }
                    } else {
                        ILSMDiskComponent newComponent = createDiskComponent(componentFactory, mergeOp.getTarget(),
                                null, mergeOp.getBloomFilterTarget(), true);
                        ILSMDiskComponentBulkLoader componentBulkLoader =
                                newComponent.createBulkLoader(operation, 1.0f, false, numElements, false, false, false);
                        componentBulkLoaders.add(componentBulkLoader);
                        while (cursor.hasNext()) {
                            cursor.next();
                            ITupleReference frameTuple = cursor.getTuple();
                            componentBulkLoader.add(frameTuple);
                        }
                        newComponents.add(newComponent);
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
                            tuples.put(newComponent, Pair.of(minTuple, maxTuple));
                        }
                    }
                } finally {
                    cursor.close();
                }
            } finally {
                cursor.destroy();
            }
            for (ILSMDiskComponent newComponent : newComponents) {
                if (newComponent.getLSMComponentFilter() != null) {
                    Pair<ITupleReference, ITupleReference> minMaxTuples = tuples.get(newComponent);
                    List<ITupleReference> filterTuples = Arrays.asList(minMaxTuples.getLeft(), minMaxTuples.getRight());
                    //                    for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                    //                        filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMinTuple());
                    //                        filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMaxTuple());
                    //                    }
                    getFilterManager().updateFilter(newComponent.getLSMComponentFilter(), filterTuples,
                            NoOpOperationCallback.INSTANCE);
                    getFilterManager().writeFilter(newComponent.getLSMComponentFilter(),
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

        String newMsg = diskComponentToString(newComponents.get(0));
        for (int i = 1; i < newComponents.size(); i++) {
            newMsg += "; " + diskComponentToString(newComponents.get(i));
        }
        LOGGER.info("[doMerge]\tOld: " + oldMsg + "\tnew: " + newMsg);
        return newComponents;
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

    protected LSMComponentFileReferences getNextMergeFileReferencesAtLevel(long level, long start)
            throws HyracksDataException {
        long maxId = getMaxLevelId(level);
        String newName = level + AbstractLSMIndexFileManager.DELIMITER + (start > maxId ? start : maxId + 1);
        return fileManager.getRelMergeFileReference(newName);
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
}
