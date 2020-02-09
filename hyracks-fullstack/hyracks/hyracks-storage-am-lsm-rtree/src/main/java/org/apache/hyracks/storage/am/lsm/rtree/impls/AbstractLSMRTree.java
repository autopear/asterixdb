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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.rtree.frames.RTreeFrameFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.trace.ITracer;

public abstract class AbstractLSMRTree extends AbstractLSMIndex implements ITreeIndex {

    protected final ILinearizeComparatorFactory linearizer;
    protected final int[] comparatorFields;
    protected final IBinaryComparatorFactory[] linearizerArray;
    protected final boolean isPointMBR;

    protected IBinaryComparatorFactory[] btreeCmpFactories;
    protected IBinaryComparatorFactory[] rtreeCmpFactories;

    // Common for in-memory and on-disk components.
    protected final ITreeIndexFrameFactory rtreeInteriorFrameFactory;
    protected final ITreeIndexFrameFactory btreeInteriorFrameFactory;
    protected final ITreeIndexFrameFactory rtreeLeafFrameFactory;
    protected final ITreeIndexFrameFactory btreeLeafFrameFactory;

    protected final LSMRTreeLevelMergePolicyHelper mergePolicyHelper;

    protected final IPrimitiveValueProvider[] valueProviders;

    public AbstractLSMRTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            RTreeFrameFactory rtreeInteriorFrameFactory, RTreeFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            IBufferCache diskBufferCache, ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, int fieldCount,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            ILinearizeComparatorFactory linearizer, int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            int[] rtreeFields, int[] filterFields, boolean durable, boolean isPointMBR) throws HyracksDataException {
        super(ioManager, virtualBufferCaches, diskBufferCache, fileManager, bloomFilterFalsePositiveRate, mergePolicy,
                opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, componentFactory,
                bulkLoadComponentFactory, filterFrameFactory, filterManager, filterFields, durable, filterHelper,
                rtreeFields, ITracer.NONE);
        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            RTree memRTree = new RTree(virtualBufferCache, new VirtualFreePageManager(virtualBufferCache),
                    rtreeInteriorFrameFactory, rtreeLeafFrameFactory, rtreeCmpFactories, fieldCount,
                    ioManager.resolveAbsolutePath(fileManager.getBaseDir() + "_virtual_r_" + i), isPointMBR);
            BTree memBTree = new BTree(virtualBufferCache, new VirtualFreePageManager(virtualBufferCache),
                    btreeInteriorFrameFactory, btreeLeafFrameFactory, btreeCmpFactories, btreeCmpFactories.length,
                    ioManager.resolveAbsolutePath(fileManager.getBaseDir() + "_virtual_b_" + i));
            LSMRTreeMemoryComponent mutableComponent = new LSMRTreeMemoryComponent(this, memRTree, memBTree,
                    virtualBufferCache, filterHelper == null ? null : filterHelper.createFilter());
            memoryComponents.add(mutableComponent);
            ++i;
        }

        this.rtreeInteriorFrameFactory = rtreeInteriorFrameFactory;
        this.rtreeLeafFrameFactory = rtreeLeafFrameFactory;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.btreeCmpFactories = btreeCmpFactories;
        this.rtreeCmpFactories = rtreeCmpFactories;
        this.linearizer = linearizer;
        this.comparatorFields = comparatorFields;
        this.linearizerArray = linearizerArray;
        this.isPointMBR = isPointMBR;
        this.valueProviders = ((RTreeNSMLeafFrameFactory) rtreeLeafFrameFactory).getKeyValueProviders();
        if (isLeveled) {
            mergePolicyHelper = new LSMRTreeLevelMergePolicyHelper(this);
            levelMergePolicy.setHelper(mergePolicyHelper);
        } else {
            mergePolicyHelper = null;
        }
    }

    /*
     * For External indexes with no memory components
     */
    public AbstractLSMRTree(IIOManager ioManager, ITreeIndexFrameFactory rtreeInteriorFrameFactory,
            ITreeIndexFrameFactory rtreeLeafFrameFactory, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, IBufferCache diskBufferCache,
            ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            ILinearizeComparatorFactory linearizer, int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory, boolean durable, boolean isPointMBR, ITracer tracer)
            throws HyracksDataException {
        super(ioManager, diskBufferCache, fileManager, bloomFilterFalsePositiveRate, mergePolicy, opTracker,
                ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, componentFactory, componentFactory, durable,
                tracer);
        this.rtreeInteriorFrameFactory = rtreeInteriorFrameFactory;
        this.rtreeLeafFrameFactory = rtreeLeafFrameFactory;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.btreeCmpFactories = btreeCmpFactories;
        this.rtreeCmpFactories = rtreeCmpFactories;
        this.linearizer = linearizer;
        this.comparatorFields = comparatorFields;
        this.linearizerArray = linearizerArray;
        this.isPointMBR = isPointMBR;
        this.valueProviders = ((RTreeNSMLeafFrameFactory) rtreeLeafFrameFactory).getKeyValueProviders();
        if (isLeveled) {
            mergePolicyHelper = new LSMRTreeLevelMergePolicyHelper(this);
            levelMergePolicy.setHelper(mergePolicyHelper);
        } else {
            mergePolicyHelper = null;
        }
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        cursor.open(ctx.getSearchInitialState(), pred);
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        LSMRTreeMemoryComponent mutableComponent =
                (LSMRTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getLeafFrameFactory();
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        LSMRTreeMemoryComponent mutableComponent =
                (LSMRTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getInteriorFrameFactory();
    }

    @Override
    public IPageManager getPageManager() {
        LSMRTreeMemoryComponent mutableComponent =
                (LSMRTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getPageManager();
    }

    @Override
    public int getFieldCount() {
        LSMRTreeMemoryComponent mutableComponent =
                (LSMRTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getFieldCount();
    }

    @Override
    public int getRootPageId() {
        LSMRTreeMemoryComponent mutableComponent =
                (LSMRTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getRootPageId();
    }

    @Override
    public int getFileId() {
        LSMRTreeMemoryComponent mutableComponent =
                (LSMRTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getFileId();
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        if (ctx.getOperation() == IndexOperation.PHYSICALDELETE) {
            throw new UnsupportedOperationException("Physical delete not supported in the LSM-RTree");
        }

        ITupleReference indexTuple;
        if (ctx.getIndexTuple() != null) {
            ctx.getIndexTuple().reset(tuple);
            indexTuple = ctx.getIndexTuple();
        } else {
            indexTuple = tuple;
        }

        ctx.getModificationCallback().before(indexTuple);
        ctx.getModificationCallback().found(null, indexTuple);
        if (ctx.getOperation() == IndexOperation.INSERT) {
            ctx.getCurrentMutableRTreeAccessor().insert(indexTuple);
        } else {
            // First remove all entries in the in-memory rtree (if any).
            ctx.getCurrentMutableRTreeAccessor().delete(indexTuple);
            // Insert key into the deleted-keys BTree.
            try {
                ctx.getCurrentMutableBTreeAccessor().insert(indexTuple);
            } catch (HyracksDataException e) {
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    // Do nothing, because one delete tuple is enough to indicate
                    // that all the corresponding insert tuples are deleted
                    throw e;
                }
            }
        }
        updateFilter(ctx, tuple);
    }

    @Override
    protected LSMRTreeOpContext createOpContext(IIndexAccessParameters iap) {
        return new LSMRTreeOpContext(this, memoryComponents, rtreeLeafFrameFactory, rtreeInteriorFrameFactory,
                btreeLeafFrameFactory, (IExtendedModificationOperationCallback) iap.getModificationCallback(),
                iap.getSearchOperationCallback(), getTreeFields(), getFilterFields(), getHarness(), comparatorFields,
                linearizerArray, getFilterCmpFactories(), tracer);
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return rtreeCmpFactories;
    }

    @Override
    public boolean isPrimaryIndex() {
        return false;
    }

    @Override
    protected LSMComponentFileReferences getMergeFileReferences(List<ILSMDiskComponent> components)
            throws HyracksDataException {
        if (isLeveled) {
            // Component name will be decided when creating the component
            long levelTo = components.get(0).getMinId() + 1;
            String newName = levelTo + AbstractLSMIndexFileManager.DELIMITER + 0;
            return fileManager.getRelMergeFileReference(newName);
        } else {
            RTree firstTree = (RTree) components.get(0).getIndex();
            RTree lastTree = (RTree) components.get(components.size() - 1).getIndex();
            FileReference firstFile = firstTree.getFileReference();
            FileReference lastFile = lastTree.getFileReference();
            return fileManager.getRelMergeFileReference(firstFile.getFile().getName(), lastFile.getFile().getName());
        }
    }

    @Override
    public boolean mayMatchSearchPredicate(ILSMDiskComponent component, ISearchPredicate predicate) {
        double[] keyMBR = getMBRFromTuple(predicate.getLowKey());
        if (keyMBR == null) {
            return true;
        }
        try {
            double[] minCMBR = bytesToDoubles(component.getMinKey());
            double[] maxCMBR = bytesToDoubles(component.getMaxKey());
            if (LSMRTreeLevelMergePolicyHelper.isOverlapping(minCMBR, maxCMBR, keyMBR)) {
                return true;
            }
        } catch (HyracksDataException ex) {
            return true;
        }
        return false;
    }

    @Override
    public int compareComponents(ILSMDiskComponent c1, ILSMDiskComponent c2) {
        try {
            double[] minMBR1 = bytesToDoubles(c1.getMinKey());
            double[] maxMBR1 = bytesToDoubles(c1.getMaxKey());
            double[] minMBR2 = bytesToDoubles(c2.getMinKey());
            double[] maxMBR2 = bytesToDoubles(c2.getMaxKey());
            int dim = minMBR1.length;
            for (int i = 0; i < dim; i++) {
                double cd1 = (minMBR1[i] + maxMBR1[i]) / 2;
                double cd2 = (minMBR2[i] + maxMBR2[i]) / 2;
                int r = Double.compare(cd1, cd2);
                if (r != 0) {
                    return r;
                }
            }
            return (int) (c2.getMaxId() - c1.getMaxId());
        } catch (HyracksDataException ex) {
        }
        return 0;
    }

    public long getMaxNumTuplesPerComponent() throws HyracksDataException {
        long m = 0L;
        synchronized (diskComponents) {
            for (ILSMDiskComponent c : diskComponents) {
                if (c.getTupleCount() > m) {
                    m = c.getTupleCount();
                }
            }
        }
        return m;
    }

    public double[] getMBRFromTuple(ITupleReference tuple) {
        if (tuple == null) {
            return null;
        }
        int size = Math.min(tuple.getFieldCount(), valueProviders.length);
        double[] mbr = new double[size];
        for (int i = 0; i < size; i++) {
            mbr[i] = valueProviders[i].getValue(tuple.getFieldData(i), tuple.getFieldStart(i));
        }
        return mbr;
    }

    public static double[] bytesToDoubles(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        int dim = bytes.length / Double.BYTES;
        double[] values = new double[dim];
        for (int i = 0; i < dim; i++) {
            values[i] = ByteBuffer.wrap(bytes, i * Double.BYTES, Double.BYTES).getDouble();
        }
        return values;
    }

    public static byte[] doublesToBytes(double[] values) {
        if (values == null || values.length == 0) {
            return null;
        }
        byte[] bytes = new byte[values.length * Double.BYTES];
        for (int i = 0; i < values.length; i++) {
            ByteBuffer.wrap(bytes, i * Double.BYTES, Double.BYTES).putDouble(values[i]);
        }
        return bytes;
    }

    @Override
    public String componentToString(ILSMDiskComponent component, int indent) {
        String basename = component.getBasename();
        String minMaxKeys = componentMinMaxKeys(component);
        long numTuples;
        try {
            numTuples = component.getTupleCount();
        } catch (HyracksDataException ex) {
            numTuples = -1L;
        }
        String spaces = getIndent(indent);
        return spaces + "{\n" + spaces + "  name: " + basename + ",\n" + spaces + "  size: "
                + component.getComponentSize() + ",\n" + spaces + "  keys: " + minMaxKeys + ",\n" + spaces
                + "  tuples: " + numTuples + "\n" + spaces + "}";
    }

    private String getComponentInfo(ILSMDiskComponent c) {
        String mbrStr = componentMinMaxKeys(c);
        long numTuples;
        try {
            numTuples = c.getTupleCount();
        } catch (HyracksDataException ex) {
            numTuples = -1L;
        }
        return c.getBasename() + ":" + numTuples + ":" + mbrStr;
    }

    @Override
    public String getComponentsInfo() {
        synchronized (diskComponents) {
            int size = diskComponents.size();
            if (size == 0) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            sb.append(getComponentInfo(diskComponents.get(0)));
            for (int i = 1; i < size; i++) {
                sb.append(";").append(diskComponents.get(i));
            }
            return sb.toString();
        }
    }

    @Override
    public String componentMinMaxKeys(ILSMDiskComponent c) {
        double[] minMBR;
        try {
            minMBR = bytesToDoubles(c.getMinKey());
        } catch (HyracksDataException ex) {
            minMBR = null;
        }
        double[] maxMBR;
        try {
            maxMBR = bytesToDoubles(c.getMaxKey());
        } catch (HyracksDataException ex) {
            maxMBR = null;
        }
        return mbrToString(minMBR, maxMBR);
    }

    private static String doubleArrayToString(double[] ds) {
        if (ds == null || ds.length == 0) {
            return "Unknown";
        } else if (ds.length == 1) {
            return Double.toString(ds[0]);
        } else {
            StringBuilder sb = new StringBuilder(Double.toString(ds[0]));
            for (int i = 1; i < ds.length; i++) {
                sb.append(",").append(ds[i]);
            }
            return sb.toString();
        }
    }

    public static String mbrToString(double[] mbr) {
        if (mbr == null || mbr.length == 0) {
            return "Unknown";
        }
        int dim = mbr.length;
        if (dim % 2 != 0) {
            return "Error";
        }
        dim /= 2;
        StringBuilder sb = new StringBuilder("[");
        sb.append(mbr[0]);
        for (int i = 1; i < dim; i++) {
            sb.append(",").append(mbr[i]);
        }
        sb.append(" ").append(mbr[dim]);
        for (int i = 1; i < dim; i++) {
            sb.append(",").append(mbr[dim + i]);
        }
        sb.append("]");
        return sb.toString();
    }

    public static String mbrToString(double[] minMBR, double[] maxMBR) {
        return "[" + doubleArrayToString(minMBR) + " " + doubleArrayToString(maxMBR) + "]";
    }
}
