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

import java.util.List;
import java.nio.file.Paths;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LSMBTreePointSearchCursor extends EnforcedIndexCursor implements ILSMIndexCursor {

    private ITreeIndexCursor[] btreeCursors;
    private final ILSMIndexOperationContext opCtx;
    private ISearchOperationCallback searchCallback;
    private RangePredicate predicate;
    private boolean includeMutableComponent;
    private int numBTrees;
    private BTreeAccessor[] btreeAccessors;
    private BloomFilter[] bloomFilters;
    private ILSMHarness lsmHarness;
    private boolean nextHasBeenCalled;
    private boolean foundTuple;
    private int foundIn = -1;
    private ITupleReference frameTuple;
    private List<ILSMComponent> operationalComponents;
    private boolean resultOfSearchCallbackProceed = false;

    private final long[] hashes = BloomFilter.createHashArray();

    private boolean bloomFilterDisabled = false;
    private long startTime;
    private String diskComponents;
    private String accessTrace;
    private static final Logger LOGGER = LogManager.getLogger();

    public LSMBTreePointSearchCursor(ILSMIndexOperationContext opCtx) {
        this.opCtx = opCtx;
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        if (nextHasBeenCalled) {
            return false;
        } else if (foundTuple) {
            return true;
        }

        boolean reconciled = false;
        for (int i = 0; i < numBTrees; ++i) {
            long startTime = System.nanoTime();
            if (!bloomFilterDisabled && bloomFilters[i] != null
                    && !bloomFilters[i].contains(predicate.getLowKey(), hashes)) {
                if (accessTrace.isEmpty())
                    accessTrace = Integer.toString(i) + ":*:" + Long.toString(System.nanoTime() - startTime);
                else
                    accessTrace += (";" + Integer.toString(i) + ":*:" + Long.toString(System.nanoTime() - startTime));
                continue;
            }
            btreeAccessors[i].search(btreeCursors[i], predicate);
            if (btreeCursors[i].hasNext()) {
                btreeCursors[i].next();
                // We use the predicate's to lock the key instead of the tuple that we get from cursor
                // to avoid copying the tuple when we do the "unlatch dance".
                if (!reconciled) {
                    resultOfSearchCallbackProceed = searchCallback.proceed(predicate.getLowKey());
                }
                if (reconciled || resultOfSearchCallbackProceed) {
                    // if proceed is successful, then there's no need for doing the "unlatch dance"
                    if (((ILSMTreeTupleReference) btreeCursors[i].getTuple()).isAntimatter()) {
                        if (reconciled) {
                            searchCallback.cancel(predicate.getLowKey());
                        }
                        btreeCursors[i].close();
                        if (accessTrace.isEmpty())
                            accessTrace = Integer.toString(i) + ":-:" + Long.toString(System.nanoTime() - startTime);
                        else
                            accessTrace +=
                                    (";" + Integer.toString(i) + ":-:" + Long.toString(System.nanoTime() - startTime));
                        return false;
                    } else {
                        frameTuple = btreeCursors[i].getTuple();
                        foundTuple = true;
                        foundIn = i;
                        if (accessTrace.isEmpty())
                            accessTrace = Integer.toString(i) + ":+:" + Long.toString(System.nanoTime() - startTime);
                        else
                            accessTrace +=
                                    (";" + Integer.toString(i) + ":+:" + Long.toString(System.nanoTime() - startTime));
                        return true;
                    }
                }
                if (i == 0 && includeMutableComponent) {
                    // unlatch/unpin
                    btreeCursors[i].close();
                    searchCallback.reconcile(predicate.getLowKey());
                    reconciled = true;

                    // retraverse
                    btreeAccessors[0].search(btreeCursors[i], predicate);
                    if (btreeCursors[i].hasNext()) {
                        btreeCursors[i].next();
                        if (((ILSMTreeTupleReference) btreeCursors[i].getTuple()).isAntimatter()) {
                            searchCallback.cancel(predicate.getLowKey());
                            btreeCursors[i].close();
                            if (accessTrace.isEmpty())
                                accessTrace =
                                        Integer.toString(i) + ":-:" + Long.toString(System.nanoTime() - startTime);
                            else
                                accessTrace += (";" + Integer.toString(i) + ":-:"
                                        + Long.toString(System.nanoTime() - startTime));
                            return false;
                        } else {
                            frameTuple = btreeCursors[i].getTuple();
                            foundTuple = true;
                            searchCallback.complete(predicate.getLowKey());
                            foundIn = i;
                            if (accessTrace.isEmpty())
                                accessTrace =
                                        Integer.toString(i) + ":+:" + Long.toString(System.nanoTime() - startTime);
                            else
                                accessTrace += (";" + Integer.toString(i) + ":+:"
                                        + Long.toString(System.nanoTime() - startTime));
                            return true;
                        }
                    } else {
                        searchCallback.cancel(predicate.getLowKey());
                        btreeCursors[i].close();
                    }
                } else {
                    frameTuple = btreeCursors[i].getTuple();
                    searchCallback.reconcile(frameTuple);
                    searchCallback.complete(frameTuple);
                    foundTuple = true;
                    foundIn = i;
                    if (accessTrace.isEmpty())
                        accessTrace = Integer.toString(i) + ":+:" + Long.toString(System.nanoTime() - startTime);
                    else
                        accessTrace +=
                                (";" + Integer.toString(i) + ":+:" + Long.toString(System.nanoTime() - startTime));
                    return true;
                }
            } else {
                btreeCursors[i].close();
                if (accessTrace.isEmpty())
                    accessTrace = Integer.toString(i) + ":+:" + Long.toString(System.nanoTime() - startTime);
                else
                    accessTrace += (";" + Integer.toString(i) + ":+:" + Long.toString(System.nanoTime() - startTime));
            }
        }
        return false;
    }

    @Override
    public void doClose() throws HyracksDataException {
        try {
            closeCursors();
            nextHasBeenCalled = false;
            foundTuple = false;
        } finally {
            if (lsmHarness != null) {
                lsmHarness.endSearch(opCtx);

                long duration = System.nanoTime() - startTime;
                if (foundIn > -1 && LOGGER.isInfoEnabled() && !diskComponents.isEmpty()
                        && Paths.get(opCtx.getIndex().getIndexIdentifier()).getFileName().toString()
                                .compareTo("usertable") == 0) {
                    String msg = "[SEARCH]\t" + Integer.toString(foundIn) + "," + Long.toString(duration) + ","
                            + diskComponents + "," + accessTrace;
                    LOGGER.info(msg);
                }
            }
        }
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        diskComponents = "";
        accessTrace = "";
        startTime = System.nanoTime();
        boolean bfDisabled = false;

        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        searchCallback = lsmInitialState.getSearchOperationCallback();
        predicate = (RangePredicate) lsmInitialState.getSearchPredicate();
        numBTrees = operationalComponents.size();
        if (btreeCursors != null && btreeCursors.length != numBTrees) {
            Throwable failure = CleanupUtils.destroy(null, btreeCursors);
            btreeCursors = null;
            failure = CleanupUtils.destroy(failure, btreeAccessors);
            btreeAccessors = null;
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
        }
        if (btreeCursors == null) {
            // object creation: should be relatively low
            btreeCursors = new ITreeIndexCursor[numBTrees];
            btreeAccessors = new BTreeAccessor[numBTrees];
            bloomFilters = new BloomFilter[numBTrees];
        }
        includeMutableComponent = false;

        for (int i = 0; i < numBTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            BTree btree = (BTree) component.getIndex();
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
                if (bloomFilters[i] != null) {
                    destroyAndNullifyCursorAtIndex(i);
                }
            } else {
                if (bloomFilters[i] == null) {
                    destroyAndNullifyCursorAtIndex(i);
                }

                LSMBTreeWithBloomFilterDiskComponent c = (LSMBTreeWithBloomFilterDiskComponent) component;
                if (Double.compare(c.getLsmIndex().bloomFilterFalsePositiveRate(), 1.0) >= 0)
                    bfDisabled = true;
                bloomFilters[i] = c.getBloomFilter();
                if (diskComponents.isEmpty())
                    diskComponents = Long.toString(c.getComponentSize());
                else
                    diskComponents += ";" + Long.toString(c.getComponentSize());
            }

            if (btreeAccessors[i] == null) {
                btreeAccessors[i] = btree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                btreeCursors[i] = btreeAccessors[i].createPointCursor(false);
            } else {
                // re-use
                btreeAccessors[i].reset(btree, NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                btreeCursors[i].close();
            }
        }
        nextHasBeenCalled = false;
        foundTuple = false;

        if (Paths.get(opCtx.getIndex().getIndexIdentifier()).getFileName().toString().compareTo("usertable") == 0)
            bloomFilterDisabled = bfDisabled;
    }

    private void destroyAndNullifyCursorAtIndex(int i) throws HyracksDataException {
        // component at location i was a disk component before, and is now a memory component, or vise versa
        bloomFilters[i] = null;
        Throwable failure = CleanupUtils.destroy(null, btreeCursors[i]);
        btreeCursors[i] = null;
        failure = CleanupUtils.destroy(failure, btreeAccessors[i]);
        btreeAccessors[i] = null;
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    @Override
    public void doNext() throws HyracksDataException {
        nextHasBeenCalled = true;
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        if (btreeCursors != null) {
            for (int i = 0; i < numBTrees; ++i) {
                if (btreeCursors[i] != null) {
                    btreeCursors[i].destroy();
                }
            }
        }
    }

    @Override
    public ITupleReference doGetTuple() {
        return frameTuple;
    }

    @Override
    public ITupleReference getFilterMinTuple() {
        ILSMComponentFilter filter = getFilter();
        return filter == null ? null : filter.getMinTuple();
    }

    @Override
    public ITupleReference getFilterMaxTuple() {
        ILSMComponentFilter filter = getFilter();
        return filter == null ? null : filter.getMaxTuple();
    }

    private ILSMComponentFilter getFilter() {
        if (foundTuple) {
            return operationalComponents.get(foundIn).getLSMComponentFilter();
        }
        return null;
    }

    private void closeCursors() throws HyracksDataException {
        if (btreeCursors != null) {
            for (int i = 0; i < numBTrees; ++i) {
                if (btreeCursors[i] != null) {
                    btreeCursors[i].close();
                }
            }
        }
    }

    @Override
    public boolean getSearchOperationCallbackProceedResult() {
        return resultOfSearchCallbackProceed;
    }
}
