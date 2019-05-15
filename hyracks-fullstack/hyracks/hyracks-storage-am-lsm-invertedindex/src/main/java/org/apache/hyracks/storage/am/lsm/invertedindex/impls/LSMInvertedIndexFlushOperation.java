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
import org.apache.hyracks.storage.am.lsm.common.impls.FlushOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;

public class LSMInvertedIndexFlushOperation extends FlushOperation {
    private List<FileReference> deletedKeysBTreeFlushTargets;
    private List<FileReference> bloomFilterFlushTargets;

    public LSMInvertedIndexFlushOperation(ILSMIndexAccessor accessor, List<FileReference> flushTargets,
            List<FileReference> deletedKeysBTreeFlushTargets, List<FileReference> bloomFilterFlushTargets,
            ILSMIOOperationCallback callback, String indexIdentifier) {
        super(accessor, flushTargets, callback, indexIdentifier);
        this.deletedKeysBTreeFlushTargets = deletedKeysBTreeFlushTargets;
        this.bloomFilterFlushTargets = bloomFilterFlushTargets;
    }

    public LSMInvertedIndexFlushOperation(ILSMIndexAccessor accessor, FileReference flushTarget,
            FileReference deletedKeysBTreeFlushTarget, FileReference bloomFilterFlushTarget,
            ILSMIOOperationCallback callback, String indexIdentifier) {
        super(accessor, flushTarget, callback, indexIdentifier);
        this.deletedKeysBTreeFlushTargets = Collections.singletonList(deletedKeysBTreeFlushTarget);
        this.bloomFilterFlushTargets = Collections.singletonList(bloomFilterFlushTarget);
    }

    public List<FileReference> getDeletedKeysBTreeTargets() {
        return deletedKeysBTreeFlushTargets;
    }

    public FileReference getDeletedKeysBTreeTarget() {
        return deletedKeysBTreeFlushTargets.isEmpty() ? null : deletedKeysBTreeFlushTargets.get(0);
    }

    public void setDeletedKeysBTreeTargets(List<FileReference> targets) {
        deletedKeysBTreeFlushTargets = targets;
    }

    public void setDeletedKeysBTreeTarget(FileReference target) {
        deletedKeysBTreeFlushTargets = Collections.singletonList(target);
    }

    public List<FileReference> getBloomFilterTargets() {
        return bloomFilterFlushTargets;
    }

    public FileReference getBloomFilterTarget() {
        return bloomFilterFlushTargets.isEmpty() ? null : bloomFilterFlushTargets.get(0);
    }

    public void setBloomFilterTargets(List<FileReference> targets) {
        bloomFilterFlushTargets = targets;
    }

    public void setBloomFilterTarget(FileReference target) {
        bloomFilterFlushTargets = Collections.singletonList(target);
    }

    @Override
    public List<LSMComponentFileReferences> getComponentsFiles() {
        List<LSMComponentFileReferences> refs = new ArrayList<>();
        for (int i = 0; i < targets.size(); i++) {
            refs.add(new LSMComponentFileReferences(targets.get(i), deletedKeysBTreeFlushTargets.get(i),
                    bloomFilterFlushTargets.get(i)));
        }
        return refs;
    }

    @Override
    public LSMComponentFileReferences getComponentFiles() {
        return new LSMComponentFileReferences(targets.get(0), deletedKeysBTreeFlushTargets.get(0),
                bloomFilterFlushTargets.get(0));
    }
}
