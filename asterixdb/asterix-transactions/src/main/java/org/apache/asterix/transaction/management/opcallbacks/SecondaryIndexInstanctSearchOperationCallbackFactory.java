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

package org.apache.asterix.transaction.management.opcallbacks;

import org.apache.asterix.common.api.IJobEventListenerFactory;
import org.apache.asterix.common.context.ITransactionSubsystemProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.AbstractOperationCallbackFactory;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.common.ISearchOperationCallback;

/**
 * Instant-search-operation-callback class for the index-only plan. The purpose of this callback is executing an instant
 * tryLock on PK during a secondary index-search.
 *
 */
public class SecondaryIndexInstanctSearchOperationCallbackFactory extends AbstractOperationCallbackFactory
        implements ISearchOperationCallbackFactory {

    private static final long serialVersionUID = 1L;

    public SecondaryIndexInstanctSearchOperationCallbackFactory(int datasetId, int[] entityIdFields,
            ITransactionSubsystemProvider txnSubsystemProvider, byte resourceType) {
        super(datasetId, entityIdFields, txnSubsystemProvider, resourceType);
    }

    @Override
    public ISearchOperationCallback createSearchOperationCallback(long resourceId, IHyracksTaskContext ctx,
            IOperatorNodePushable operatorNodePushable) throws HyracksDataException {
        try {
            // If the plan is an index-only query plan, we need to try to get an instant try lock on PK.
            // If an instant tryLock on PK fails, we do not attempt to do a lock since the operations
            // will be dealt with in the operators after the given secondary-index search.
            ITransactionSubsystem txnSubsystem = txnSubsystemProvider.getTransactionSubsystem(ctx);
            IJobletEventListenerFactory fact = ctx.getJobletContext().getJobletEventListenerFactory();
            ITransactionContext txnCtx = txnSubsystem.getTransactionManager()
                    .getTransactionContext(((IJobEventListenerFactory) fact).getTxnId(datasetId));
            return new SecondaryIndexInstantSearchOperationCallback(new DatasetId(datasetId), resourceId,
                    primaryKeyFields, txnSubsystem.getLockManager(), txnCtx);
        } catch (ACIDException e) {
            throw HyracksDataException.create(e);
        }
    }
}
