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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class LSMBTreePointSearchCursor extends EnforcedIndexCursor implements ILSMIndexCursor {

    private BTreeRangeSearchCursor[] btreeCursors;
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

    private long startTime;
    private int numDiskComponents;
    private int readAmpf;
    private String availComponents;
    private boolean isSearch;
    private String searchKey;
    private String timeStr = "";

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
            long stime = System.nanoTime();
            if (bloomFilters[i] != null && !bloomFilters[i].contains(predicate.getLowKey(), hashes)) {
                if (timeStr.isEmpty()) {
                    timeStr = i + ":BF:" + (System.nanoTime() - stime) + ":" + btreeCursors[i].getNumTotalPages() + ":"
                            + btreeCursors[i].getNumCachedPages() + ":" + btreeCursors[i].getNumUncachedPages();
                } else {
                    timeStr += ";" + i + ":BF:" + (System.nanoTime() - stime) + ":" + btreeCursors[i].getNumTotalPages()
                            + ":" + btreeCursors[i].getNumCachedPages() + ":" + btreeCursors[i].getNumUncachedPages();
                }
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
                        if (timeStr.isEmpty()) {
                            timeStr = i + ":RF:" + (System.nanoTime() - stime) + ":"
                                    + btreeCursors[i].getNumTotalPages() + ":" + btreeCursors[i].getNumCachedPages()
                                    + ":" + btreeCursors[i].getNumUncachedPages();
                        } else {
                            timeStr += ";" + i + ":RF:" + (System.nanoTime() - stime) + ":"
                                    + btreeCursors[i].getNumTotalPages() + ":" + btreeCursors[i].getNumCachedPages()
                                    + ":" + btreeCursors[i].getNumUncachedPages();
                        }
                        return false;
                    } else {
                        frameTuple = btreeCursors[i].getTuple();
                        foundTuple = true;
                        foundIn = i;
                        if (timeStr.isEmpty()) {
                            timeStr = i + ":RT:" + (System.nanoTime() - stime) + ":"
                                    + btreeCursors[i].getNumTotalPages() + ":" + btreeCursors[i].getNumCachedPages()
                                    + ":" + btreeCursors[i].getNumUncachedPages();
                        } else {
                            timeStr += ";" + i + ":RT:" + (System.nanoTime() - stime) + ":"
                                    + btreeCursors[i].getNumTotalPages() + ":" + btreeCursors[i].getNumCachedPages()
                                    + ":" + btreeCursors[i].getNumUncachedPages();
                        }
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
                            if (timeStr.isEmpty()) {
                                timeStr = i + ":MF:" + (System.nanoTime() - stime) + ":"
                                        + btreeCursors[i].getNumTotalPages() + ":" + btreeCursors[i].getNumCachedPages()
                                        + ":" + btreeCursors[i].getNumUncachedPages();
                            } else {
                                timeStr += ";" + i + ":MF:" + (System.nanoTime() - stime) + ":"
                                        + btreeCursors[i].getNumTotalPages() + ":" + btreeCursors[i].getNumCachedPages()
                                        + ":" + btreeCursors[i].getNumUncachedPages();
                            }
                            return false;
                        } else {
                            frameTuple = btreeCursors[i].getTuple();
                            foundTuple = true;
                            searchCallback.complete(predicate.getLowKey());
                            foundIn = i;
                            if (timeStr.isEmpty()) {
                                timeStr = i + ":MT:" + (System.nanoTime() - stime) + ":"
                                        + btreeCursors[i].getNumTotalPages() + ":" + btreeCursors[i].getNumCachedPages()
                                        + ":" + btreeCursors[i].getNumUncachedPages();
                            } else {
                                timeStr += ";" + i + ":MT:" + (System.nanoTime() - stime) + ":"
                                        + btreeCursors[i].getNumTotalPages() + ":" + btreeCursors[i].getNumCachedPages()
                                        + ":" + btreeCursors[i].getNumUncachedPages();
                            }
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
                    if (timeStr.isEmpty()) {
                        timeStr = i + ":DT:" + (System.nanoTime() - stime) + ":" + btreeCursors[i].getNumTotalPages()
                                + ":" + btreeCursors[i].getNumCachedPages() + ":"
                                + btreeCursors[i].getNumUncachedPages();
                    } else {
                        timeStr += ";" + i + ":DT:" + (System.nanoTime() - stime) + ":"
                                + btreeCursors[i].getNumTotalPages() + ":" + btreeCursors[i].getNumCachedPages() + ":"
                                + btreeCursors[i].getNumUncachedPages();
                    }
                    return true;
                }
            } else {
                btreeCursors[i].close();
            }
        }
        return false;
    }

    @Override
    public void doClose() throws HyracksDataException {
        if (isSearch && !operationalComponents.isEmpty()) {
            long duration = System.nanoTime() - startTime;
            AbstractLSMIndex index = (AbstractLSMIndex) lsmHarness.getLSMIndex();
            index.writeLog("[SEARCH]\t" + index.getIndexName() + "\t" + System.nanoTime() + "\t" + searchKey + "\t"
                    + duration + "\t" + numDiskComponents + "\t" + readAmpf + "\t" + availComponents + "\t" + timeStr);
        }

        try {
            closeCursors();
            nextHasBeenCalled = false;
            foundTuple = false;
        } finally {
            if (lsmHarness != null) {
                lsmHarness.endSearch(opCtx);
            }
        }
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        isSearch = (searchPred != null && searchPred.getLowKey() != null);

        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        searchCallback = lsmInitialState.getSearchOperationCallback();
        predicate = (RangePredicate) lsmInitialState.getSearchPredicate();

        if (isSearch) {
            byte[] searchData = LSMBTree.getKeyBytes(searchPred.getLowKey());
            searchKey =
                    (searchData == null) ? "Unknown" : ((LSMBTree) lsmHarness.getLSMIndex()).keyToString(searchData);
            StringBuilder s = new StringBuilder();
            for (int i = 0; i < operationalComponents.size(); i++) {
                ILSMComponent c = operationalComponents.get(i);
                String name = c.getType() == LSMComponentType.MEMORY ? "Mem" : ((ILSMDiskComponent) c).getBasename();
                if (i == 0) {
                    s.append(name);
                } else {
                    s.append(";").append(name);
                }
            }
            availComponents = s.toString();
            AbstractLSMIndex index = (AbstractLSMIndex) lsmHarness.getLSMIndex();
            if (index.isLeveledLSM()) {
                List<ILSMDiskComponent> diskComponents = index.getDiskComponents();
                numDiskComponents = diskComponents.size();
                Set<Long> levels = new HashSet<>();
                for (ILSMDiskComponent c : diskComponents) {
                    levels.add(c.getMinId());
                }
                readAmpf = levels.size() + index.level0Tables - 1;
            } else {
                numDiskComponents = index.getDiskComponents().size();
                readAmpf = numDiskComponents;
            }
            timeStr = "";
            startTime = System.nanoTime();
        }

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
            btreeCursors = new BTreeRangeSearchCursor[numBTrees];
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
                bloomFilters[i] = ((LSMBTreeWithBloomFilterDiskComponent) component).getBloomFilter();
            }

            if (btreeAccessors[i] == null) {
                btreeAccessors[i] = btree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                btreeCursors[i] = btreeAccessors[i].createPointCursor(false);
                btreeCursors[i].resetNumCachedUncachedPages();
            } else {
                // re-use
                btreeAccessors[i].reset(btree, NoOpIndexAccessParameters.INSTANCE);
                btreeCursors[i].close();
                btreeCursors[i].resetNumCachedUncachedPages();
            }
        }
        nextHasBeenCalled = false;
        foundTuple = false;
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
