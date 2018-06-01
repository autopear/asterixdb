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
package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.util.ComponentUtils;
import org.apache.hyracks.storage.am.lsm.common.util.LSMComponentIdUtils;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public abstract class AbstractLSMDiskComponent extends AbstractLSMComponent implements ILSMDiskComponent {

    private static final Logger LOGGER = LogManager.getLogger();
    public static final MutableArrayValueReference LEVEL_KEY = new MutableArrayValueReference("LSN".getBytes());
    private final DiskComponentMetadata metadata;
    private final ArrayBackedValueStorage buffer = new ArrayBackedValueStorage(Long.BYTES);
    private int level;
    private Rectangle rangeOrMBR;
    // a variable cache of componentId stored in metadata.
    // since componentId is immutable, we do not want to read from metadata every time the componentId
    // is requested.
    private ILSMComponentId componentId;

    public AbstractLSMDiskComponent(AbstractLSMIndex lsmIndex, IMetadataPageManager mdPageManager,
            ILSMComponentFilter filter) {
        super(lsmIndex, filter);
        state = ComponentState.READABLE_UNWRITABLE;
        metadata = new DiskComponentMetadata(mdPageManager);
        level = 0;
        rangeOrMBR = new Rectangle();
    }
    public AbstractLSMDiskComponent(AbstractLSMIndex lsmIndex, IMetadataPageManager mdPageManager,
            ILSMComponentFilter filter, int l) {
        super(lsmIndex, filter);
        state = ComponentState.READABLE_UNWRITABLE;
        metadata = new DiskComponentMetadata(mdPageManager);
        level = l;
        rangeOrMBR = new Rectangle();
    }
    public abstract List<Double> GetMBR () throws Exception;
    public void SetId (ILSMComponentId componentId) throws Exception
    {
        this.componentId = componentId;
    }
    @Override
    public boolean threadEnter(LSMOperationType opType, boolean isMutableComponent) {
        if (state == ComponentState.INACTIVE) {
            throw new IllegalStateException("Trying to enter an inactive disk component");
        }

        switch (opType) {
            case FORCE_MODIFICATION:
            case MODIFICATION:
            case REPLICATE:
            case SEARCH:
            case DISK_COMPONENT_SCAN:
                readerCount++;
                break;
            case MERGE:
                if (state == ComponentState.READABLE_MERGING) {
                    // This should never happen unless there are two concurrent merges that were scheduled
                    // concurrently and they have interleaving components to be merged.
                    // This should be handled properly by the merge policy, but we guard against that here anyway.
                    return false;
                }
                state = ComponentState.READABLE_MERGING;
                readerCount++;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
        return true;
    }

    @Override
    public void threadExit(LSMOperationType opType, boolean failedOperation, boolean isMutableComponent)
            throws HyracksDataException {
        switch (opType) {
            case MERGE:
                // In case two merge operations were scheduled to merge an overlapping set of components,
                // the second merge will fail and it must reset those components back to their previous state.
                if (failedOperation) {
                    state = ComponentState.READABLE_UNWRITABLE;
                }
                // Fallthrough
            case FORCE_MODIFICATION:
            case MODIFICATION:
            case REPLICATE:
            case SEARCH:
            case DISK_COMPONENT_SCAN:
                readerCount--;
                if (readerCount == 0 && state == ComponentState.READABLE_MERGING) {
                    state = ComponentState.INACTIVE;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }

        if (readerCount <= -1) {
            throw new IllegalStateException("Invalid LSM disk component readerCount: " + readerCount);
        }
    }

    @Override
    public DiskComponentMetadata getMetadata() {
        return metadata;
    }

    @Override
    public ILSMComponentId getId() throws HyracksDataException {
        if (componentId != null) {
            return componentId;
        }
        synchronized (this) {
            if (componentId == null) {
                componentId = LSMComponentIdUtils.readFrom(metadata, buffer);
            }
        }
        if (componentId.missing()) {
            // For normal datasets, componentId shouldn't be missing, since otherwise it'll be a bug.
            // However, we cannot throw an exception here to be compatible with legacy datasets.
            // In this case, the disk component would always get a garbage Id [-1, -1], which makes the
            // component Id-based optimization useless but still correct.
            LOGGER.warn("Component Id not found from disk component metadata");
        }
        return componentId;
    }

    /**
     * Mark the component as valid
     *
     * @param persist
     *            whether the call should force data to disk before returning
     * @throws HyracksDataException
     */
    @Override
    public void markAsValid(boolean persist) throws HyracksDataException {
        ComponentUtils.markAsValid(getMetadataHolder(), persist);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.log(Level.INFO, "Marked as valid component with id: " + getId());
        }
    }

    @Override
    public void activate(boolean createNewComponent) throws HyracksDataException {
        if (createNewComponent) {
            getIndex().create();
        }
        getIndex().activate();
        if (getLSMComponentFilter() != null && !createNewComponent) {
            getLsmIndex().getFilterManager().readFilter(getLSMComponentFilter(), getMetadataHolder());
        }
        //Load the level number from Disk Component Metadata
        if(!createNewComponent)
        {
            LongPointable pointable = new LongPointable();
            IMetadataPageManager metadataPageManager = this.getMetadata().getMetadataPageManager();
            metadataPageManager.get(metadataPageManager.createMetadataFrame(), LEVEL_KEY, pointable);
            int level =  pointable.getLength() == 0 ? 0 : (int)pointable.longValue();
            this.setLevel(level);
            Rectangle newComponentMBR;
            try {
                List<Double> mbrpoints = this.GetMBR();
                if(mbrpoints!=null)
                {
                    newComponentMBR = new Rectangle(mbrpoints);
                    newComponentMBR.print();
                    setRangeOrMBR(newComponentMBR);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }

    @Override
    public void deactivateAndDestroy() throws HyracksDataException {
        getIndex().deactivate();
        getIndex().destroy();
    }

    @Override
    public void destroy() throws HyracksDataException {
        getIndex().destroy();
    }

    @Override
    public void deactivate() throws HyracksDataException {
        getIndex().deactivate();
    }

    @Override
    public void deactivateAndPurge() throws HyracksDataException {
        getIndex().deactivate();
        getIndex().purge();
    }

    @Override
    public void validate() throws HyracksDataException {
        getIndex().validate();
    }

    @Override
    public IChainedComponentBulkLoader createFilterBulkLoader() throws HyracksDataException {
        return new FilterBulkLoader(getLSMComponentFilter(), getMetadataHolder(), getLsmIndex().getFilterManager(),
                getLsmIndex().getTreeFields(), getLsmIndex().getFilterFields(),
                MultiComparator.create(getLSMComponentFilter().getFilterCmpFactories()));
    }

    @Override
    public IChainedComponentBulkLoader createIndexBulkLoader(float fillFactor, boolean verifyInput,
            long numElementsHint, boolean checkIfEmptyIndex) throws HyracksDataException {
        return new LSMIndexBulkLoader(
                getIndex().createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex));
    }

    /**
     * Allows sub-class extend this method to use specialized bulkloader for merge
     */
    protected IChainedComponentBulkLoader createMergeIndexBulkLoader(float fillFactor, boolean verifyInput,
            long numElementsHint, boolean checkIfEmptyIndex) throws HyracksDataException {
        return this.createIndexBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex);
    }

    @Override
    public ChainedLSMDiskComponentBulkLoader createBulkLoader(LSMIOOperationType opType, float fillFactor,
            boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex, boolean withFilter,
            boolean cleanupEmptyComponent) throws HyracksDataException {
        ChainedLSMDiskComponentBulkLoader chainedBulkLoader =
                new ChainedLSMDiskComponentBulkLoader(this, cleanupEmptyComponent);
        if (withFilter && getLsmIndex().getFilterFields() != null) {
            chainedBulkLoader.addBulkLoader(createFilterBulkLoader());
        }
        IChainedComponentBulkLoader indexBulkloader = opType == LSMIOOperationType.MERGE
                ? createMergeIndexBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex)
                : createIndexBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex);
        chainedBulkLoader.addBulkLoader(indexBulkloader);
        return chainedBulkLoader;
    }

    @Override public int getLevel() {
        return level;
    }

    @Override public void setLevel(int level) {
        this.level = level;
    }

    @Override
    public String toString() {
        return "{\"class\":" + getClass().getSimpleName() + "\", \"index\":" + getIndex().toString() + "}";
    }

    public Rectangle getRangeOrMBR() {
        return rangeOrMBR;
    }
    public void setRangeOrMBR(Rectangle r) {
        rangeOrMBR.set(r);
    }

}