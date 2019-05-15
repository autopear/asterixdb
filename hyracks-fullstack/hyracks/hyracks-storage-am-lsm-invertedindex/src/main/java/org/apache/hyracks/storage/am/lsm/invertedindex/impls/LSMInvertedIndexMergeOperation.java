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

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.MergeOperation;
import org.apache.hyracks.storage.common.IIndexCursor;

public class LSMInvertedIndexMergeOperation extends MergeOperation {
    private List<FileReference> deletedKeysBTreeMergeTargets;
    private List<FileReference> bloomFilterMergeTargets;

    public LSMInvertedIndexMergeOperation(ILSMIndexAccessor accessor, IIndexCursor cursor, List<FileReference> targets,
            List<FileReference> deletedKeysBTreeMergeTargets, List<FileReference> bloomFilterMergeTargets,
            ILSMIOOperationCallback callback, String indexIdentifier) {
        super(accessor, targets, callback, indexIdentifier, cursor);
        this.deletedKeysBTreeMergeTargets = deletedKeysBTreeMergeTargets;
        this.bloomFilterMergeTargets = bloomFilterMergeTargets;
    }

    public LSMInvertedIndexMergeOperation(ILSMIndexAccessor accessor, IIndexCursor cursor, FileReference target,
            FileReference deletedKeysBTreeMergeTarget, FileReference bloomFilterMergeTarget,
            ILSMIOOperationCallback callback, String indexIdentifier) {
        super(accessor, target, callback, indexIdentifier, cursor);
        this.deletedKeysBTreeMergeTargets = Collections.singletonList(deletedKeysBTreeMergeTarget);
        this.bloomFilterMergeTargets = Collections.singletonList(bloomFilterMergeTarget);
    }

    public List<FileReference> getDeletedKeysBTreeTargets() {
        return deletedKeysBTreeMergeTargets;
    }

    public FileReference getDeletedKeysBTreeTarget() {
        return deletedKeysBTreeMergeTargets.isEmpty() ? null : deletedKeysBTreeMergeTargets.get(0);
    }

    public void setDeletedKeysBTreeTargets(List<FileReference> targets) {
        deletedKeysBTreeMergeTargets = targets;
    }

    public void setDeletedKeysBTreeTarget(FileReference target) {
        deletedKeysBTreeMergeTargets = Collections.singletonList(target);
    }

    public List<FileReference> getBloomFilterTargets() {
        return bloomFilterMergeTargets;
    }

    public FileReference getBloomFilterTarget() {
        return bloomFilterMergeTargets.isEmpty() ? null : bloomFilterMergeTargets.get(0);
    }

    public void setBloomFilterTargets(List<FileReference> targets) {
        bloomFilterMergeTargets = targets;
    }

    public void setBloomFilterTarget(FileReference target) {
        bloomFilterMergeTargets = Collections.singletonList(target);
    }

    @Override
    public List<LSMComponentFileReferences> getComponentsFiles() {
        List<LSMComponentFileReferences> refs = new ArrayList<>();
        for (int i = 0; i < targets.size(); i++) {
            refs.add(new LSMComponentFileReferences(targets.get(i), deletedKeysBTreeMergeTargets.get(i),
                    bloomFilterMergeTargets.get(i)));
        }
        return refs;
    }

    @Override
    public LSMComponentFileReferences getComponentFiles() {
        return new LSMComponentFileReferences(targets.get(0), deletedKeysBTreeMergeTargets.get(0),
                bloomFilterMergeTargets.get(0));
    }
}
