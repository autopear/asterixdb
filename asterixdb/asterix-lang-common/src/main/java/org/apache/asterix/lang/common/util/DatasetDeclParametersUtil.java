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
package org.apache.asterix.lang.common.util;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.ExploringMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LevelMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.SizeTieredMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LevelRTreeMergePolicyFactory;

public class DatasetDeclParametersUtil {
    /* ***********************************************
     * Merge Policy Parameters
     * ***********************************************
     */
    public static final String MERGE_POLICY_PARAMETER_NAME = "merge-policy";
    public static final String MERGE_POLICY_NAME_PARAMETER_NAME = "name";
    public static final String MERGE_POLICY_PARAMETERS_PARAMETER_NAME = "parameters";
    public static final String MERGE_POLICY_MERGABLE_SIZE_PARAMETER_NAME = "max-mergable-component-size";
    public static final String MERGE_POLICY_TOLERANCE_COUNT_PARAMETER_NAME = "max-tolerance-component-count";
    public static final String MERGE_POLICY_NUMBER_COMPONENTS_PARAMETER_NAME = "num-components";

    public static final String MERGE_POLICY_SIZE_RATIO_NAME = "size-ratio";
    public static final String MERGE_POLICY_MAX_COMPONENT_COUNT_NAME = "max-component-count";
    public static final String MERGE_POLICY_MIN_MERGE_COMPONENT_COUNT_NAME = "min-merge-component-count";
    public static final String MERGE_POLICY_MAX_MERGE_COMPONENT_COUNT_NAME = "max-merge-component-count";

    /* ***********************************************
     * Storage Block Compression Parameters
     * ***********************************************
     */
    public static final String STORAGE_BLOCK_COMPRESSION_PARAMETER_NAME = "storage-block-compression";
    public static final String STORAGE_BLOCK_COMPRESSION_SCHEME_PARAMETER_NAME = "scheme";

    /* ***********************************************
     * Private members
     * ***********************************************
     */
    private static final ARecordType WITH_OBJECT_TYPE = getWithObjectType();
    private static final AdmObjectNode EMPTY_WITH_OBJECT = new AdmObjectNode();

    private DatasetDeclParametersUtil() {
    }

    public static AdmObjectNode validateAndGetWithObjectNode(RecordConstructor withRecord,
            DatasetConfig.DatasetType datasetType) throws CompilationException {
        if (withRecord == null) {
            return EMPTY_WITH_OBJECT;
        }

        // Handle based on dataset type
        if (datasetType == DatasetConfig.DatasetType.INTERNAL) {
            final ConfigurationTypeValidator validator = new ConfigurationTypeValidator();
            final AdmObjectNode node = ExpressionUtils.toNode(withRecord);
            validator.validateType(WITH_OBJECT_TYPE, node);
            return node;
        } else {
            return ExpressionUtils.toNode(withRecord);
        }
    }

    private static ARecordType getWithObjectType() {
        final String[] withNames = { MERGE_POLICY_PARAMETER_NAME, STORAGE_BLOCK_COMPRESSION_PARAMETER_NAME };
        final IAType[] withTypes = { AUnionType.createUnknownableType(getMergePolicyType()),
                AUnionType.createUnknownableType(getStorageBlockCompressionType()) };
        return new ARecordType("withObject", withNames, withTypes, false);
    }

    private static ARecordType getMergePolicyType() {
        //merge-policy.parameters
        final String[] parameterNames = { ILSMMergePolicyFactory.BTREE_INDEX, ILSMMergePolicyFactory.INVERTED_INDEX,
                ILSMMergePolicyFactory.RTREE_INDEX, ILSMMergePolicyFactory.RTREE_COMPARATOR,
                MERGE_POLICY_MERGABLE_SIZE_PARAMETER_NAME, MERGE_POLICY_TOLERANCE_COUNT_PARAMETER_NAME,
                MERGE_POLICY_NUMBER_COMPONENTS_PARAMETER_NAME, MERGE_POLICY_SIZE_RATIO_NAME,
                MERGE_POLICY_MAX_COMPONENT_COUNT_NAME, MERGE_POLICY_MIN_MERGE_COMPONENT_COUNT_NAME,
                MERGE_POLICY_MAX_MERGE_COMPONENT_COUNT_NAME, SizeTieredMergePolicyFactory.LOW_BUCKET,
                SizeTieredMergePolicyFactory.HIGH_BUCKET, SizeTieredMergePolicyFactory.MIN_COMPONENTS,
                SizeTieredMergePolicyFactory.MAX_COMPONENTS, SizeTieredMergePolicyFactory.MIN_SSTABLE_SIZE,
                LevelMergePolicyFactory.PICK, LevelMergePolicyFactory.NUM_COMPONENTS_0,
                LevelMergePolicyFactory.NUM_COMPONENTS_1, LevelMergePolicyFactory.OVERLAP_MODE,
                ExploringMergePolicyFactory.LAMBDA, LevelRTreeMergePolicyFactory.PARTITION };
        final IAType[] parametersTypes = { AUnionType.createUnknownableType(BuiltinType.ASTRING), // BTREE_INDEX
                AUnionType.createUnknownableType(BuiltinType.ALL_TYPE), // INVERTED_INDEX
                AUnionType.createUnknownableType(BuiltinType.ASTRING), // RTREE_INDEX
                AUnionType.createUnknownableType(BuiltinType.ASTRING), // RTREE_COMPARATOR
                AUnionType.createUnknownableType(BuiltinType.AINT64), // MERGABLE_SIZE
                AUnionType.createUnknownableType(BuiltinType.AINT64), // TOLERANCE_COUNT
                AUnionType.createUnknownableType(BuiltinType.AINT64), // NUMBER_COMPONENTS
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE), // SIZE_RATIO
                AUnionType.createUnknownableType(BuiltinType.AINT64), // MAX_COMPONENT_COUNT
                AUnionType.createUnknownableType(BuiltinType.AINT64), // MIN_MERGE_COMPONENT_COUNT
                AUnionType.createUnknownableType(BuiltinType.AINT64), // MAX_MERGE_COMPONENT_COUNT
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE), // LOW_BUCKET
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE), // HIGH_BUCKET
                AUnionType.createUnknownableType(BuiltinType.AINT64), // MIN_COMPONENTS
                AUnionType.createUnknownableType(BuiltinType.AINT64), // MAX_COMPONENTS
                AUnionType.createUnknownableType(BuiltinType.AINT64), // MIN_SSTABLE_SIZE
                AUnionType.createUnknownableType(BuiltinType.ASTRING), // PICK
                AUnionType.createUnknownableType(BuiltinType.AINT64), // NUM_COMPONENTS_0
                AUnionType.createUnknownableType(BuiltinType.AINT64), // NUM_COMPONENTS_1
                AUnionType.createUnknownableType(BuiltinType.ASTRING), // OVERLAP_MODE
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE), // LAMBDA
                AUnionType.createUnknownableType(BuiltinType.ASTRING), // PARTITION
        };

        final ARecordType parameters =
                new ARecordType(MERGE_POLICY_PARAMETERS_PARAMETER_NAME, parameterNames, parametersTypes, false);

        //merge-policy
        final String[] mergePolicyNames = { MERGE_POLICY_NAME_PARAMETER_NAME, MERGE_POLICY_PARAMETERS_PARAMETER_NAME };
        final IAType[] mergePolicyTypes = { BuiltinType.ASTRING, AUnionType.createUnknownableType(parameters) };

        return new ARecordType(MERGE_POLICY_PARAMETER_NAME, mergePolicyNames, mergePolicyTypes, false);
    }

    private static ARecordType getStorageBlockCompressionType() {
        final String[] schemeName = { STORAGE_BLOCK_COMPRESSION_SCHEME_PARAMETER_NAME };
        final IAType[] schemeType = { BuiltinType.ASTRING };
        return new ARecordType(STORAGE_BLOCK_COMPRESSION_PARAMETER_NAME, schemeName, schemeType, false);
    }
}
