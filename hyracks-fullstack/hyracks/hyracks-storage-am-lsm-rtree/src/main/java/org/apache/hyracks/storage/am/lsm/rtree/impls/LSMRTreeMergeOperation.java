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

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.MergeOperation;
import org.apache.hyracks.storage.common.IIndexCursor;

import java.util.ArrayList;
import java.util.List;

public class LSMRTreeMergeOperation extends MergeOperation {
    private final FileReference btreeMergeTarget;
    private final FileReference bloomFilterMergeTarget;

    private final List<FileReference> btreeMergeTargets;
    private final List<FileReference> bloomFilterMergeTargets;

    public LSMRTreeMergeOperation(ILSMIndexAccessor accessor, IIndexCursor cursor, FileReference target,
            FileReference btreeMergeTarget, FileReference bloomFilterMergeTarget, ILSMIOOperationCallback callback,
            String indexIdentifier) {
        super(accessor, target, callback, indexIdentifier, cursor);
        this.btreeMergeTarget = btreeMergeTarget;
        this.bloomFilterMergeTarget = bloomFilterMergeTarget;

        this.btreeMergeTargets = null;
        this.bloomFilterMergeTargets = null;
    }

    public LSMRTreeMergeOperation(ILSMIndexAccessor accessor, IIndexCursor cursor, List<FileReference> targets,
            List<FileReference> btreeMergeTargets,  List<FileReference> bloomFilterMergeTargets, ILSMIOOperationCallback callback,
            String indexIdentifier) {
        super(accessor, targets, callback, indexIdentifier, cursor);
        this.btreeMergeTargets = btreeMergeTargets;
        this.bloomFilterMergeTargets = bloomFilterMergeTargets;

        this.btreeMergeTarget = null;
        this.bloomFilterMergeTarget = null;
    }


    public FileReference getBTreeTarget() {
        return btreeMergeTarget;
    }

    public FileReference getBloomFilterTarget() {
        return bloomFilterMergeTarget;
    }


    public List<FileReference> getBTreeTargets() {
        return btreeMergeTargets;
    }

    public List<FileReference> getBloomFilterTargets() {
        return bloomFilterMergeTargets;
    }


    @Override
    public LSMComponentFileReferences getComponentFiles() {
        return new LSMComponentFileReferences(btreeMergeTarget, null, bloomFilterMergeTarget);
    }

    @Override public List<LSMComponentFileReferences> getLeveledMergeComponentFiles() {
        List<LSMComponentFileReferences> refs = new ArrayList<>();
        for (int i=0;i <btreeMergeTargets.size();i++)
        {
            refs.add(new LSMComponentFileReferences(btreeMergeTargets.get(i), null, bloomFilterMergeTargets.get(i)));
        }
        return refs;
    }
}
