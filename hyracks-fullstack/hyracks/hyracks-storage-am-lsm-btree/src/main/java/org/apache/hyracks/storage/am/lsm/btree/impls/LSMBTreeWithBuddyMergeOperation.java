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
import java.util.Collections;
import java.util.List;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.MergeOperation;
import org.apache.hyracks.storage.common.IIndexCursor;

public class LSMBTreeWithBuddyMergeOperation extends MergeOperation {

    private List<FileReference> buddyBtreeMergeTargets;
    private List<FileReference> bloomFilterMergeTargets;
    private final boolean keepDeletedTuples;

    public LSMBTreeWithBuddyMergeOperation(ILSMIndexAccessor accessor, IIndexCursor cursor, List<FileReference> targets,
            List<FileReference> buddyBtreeMergeTargets, List<FileReference> bloomFilterMergeTargets,
            ILSMIOOperationCallback callback, String indexIdentifier, boolean keepDeletedTuples) {
        super(accessor, targets, callback, indexIdentifier, cursor);
        this.buddyBtreeMergeTargets = buddyBtreeMergeTargets;
        this.bloomFilterMergeTargets = bloomFilterMergeTargets;
        this.keepDeletedTuples = keepDeletedTuples;
    }

    public LSMBTreeWithBuddyMergeOperation(ILSMIndexAccessor accessor, IIndexCursor cursor, FileReference target,
            FileReference buddyBtreeMergeTarget, FileReference bloomFilterMergeTarget, ILSMIOOperationCallback callback,
            String indexIdentifier, boolean keepDeletedTuples) {
        super(accessor, target, callback, indexIdentifier, cursor);
        this.buddyBtreeMergeTargets = Collections.singletonList(buddyBtreeMergeTarget);
        this.bloomFilterMergeTargets = Collections.singletonList(bloomFilterMergeTarget);
        this.keepDeletedTuples = keepDeletedTuples;
    }

    public List<FileReference> getBuddyBTreeTargets() {
        return buddyBtreeMergeTargets;
    }

    public FileReference getBuddyBTreeTarget() {
        return buddyBtreeMergeTargets.isEmpty() ? null : buddyBtreeMergeTargets.get(0);
    }

    public void setBuddyBTreeTargets(List<FileReference> targets) {
        this.buddyBtreeMergeTargets = targets;
    }

    public void setBuddyBTreeTarget(FileReference target) {
        this.buddyBtreeMergeTargets = Collections.singletonList(target);
    }

    public List<FileReference> getBloomFilterTargets() {
        return bloomFilterMergeTargets;
    }

    public FileReference getBloomFilterTarget() {
        return buddyBtreeMergeTargets.isEmpty() ? null : bloomFilterMergeTargets.get(0);
    }

    public void setBloomFilterTargets(List<FileReference> targets) {
        this.bloomFilterMergeTargets = targets;
    }

    public void setBloomFilterTarget(FileReference target) {
        this.bloomFilterMergeTargets = Collections.singletonList(target);
    }

    public boolean isKeepDeletedTuples() {
        return keepDeletedTuples;
    }

    @Override
    public List<LSMComponentFileReferences> getComponentsFiles() {
        List<LSMComponentFileReferences> refs = new ArrayList<>();
        for (int i = 0; i < targets.size(); i++) {
            refs.add(new LSMComponentFileReferences(targets.get(i), buddyBtreeMergeTargets.get(i),
                    bloomFilterMergeTargets.get(i)));
        }
        return refs;
    }

    @Override
    public LSMComponentFileReferences getComponentFiles() {
        return new LSMComponentFileReferences(targets.get(0), buddyBtreeMergeTargets.get(0),
                bloomFilterMergeTargets.get(0));
    }
}
