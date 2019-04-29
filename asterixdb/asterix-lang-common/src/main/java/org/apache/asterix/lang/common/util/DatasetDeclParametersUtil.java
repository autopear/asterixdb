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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

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

    // Slow merge policy
    public static final String MERGE_POLICY_MIN_COMPONENTS_PARAMETER_NAME = "min-components";
    public static final String MERGE_POLICY_MAX_COMPONENTS_PARAMETER_NAME = "max-components";
    public static final String MERGE_POLICY_MIN_DELAY_PARAMETER_NAME = "min-delay";
    public static final String MERGE_POLICY_MAX_DELAY_PARAMETER_NAME = "max-delay";

    // Exploring merge policy
    public static final String MERGE_POLICY_LAMBDA_PARAMETER_NAME = "lambda";

    // Random merge policy
    public static final String MERGE_POLICY_PROBABILITY_PARAMETER_NAME = "probability";
    public static final String MERGE_POLICY_DISTRIBUTION_PARAMETER_NAME = "distribution";

    // Size-Tiered merge policy
    public static final String MERGE_POLICY_LOW_BUCKET_PARAMETER_NAME = "low-bucket";
    public static final String MERGE_POLICY_HIGH_BUCKET_PARAMETER_NAME = "high-bucket";
    public static final String MERGE_POLICY_MIN_SSTABLE_SIZE_PARAMETER_NAME = "min_sstable_size";

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

    public static AdmObjectNode validateAndGetWithObjectNode(RecordConstructor withRecord) throws CompilationException {
        if (withRecord == null) {
            return EMPTY_WITH_OBJECT;
        }
        final ConfigurationTypeValidator validator = new ConfigurationTypeValidator();
        final AdmObjectNode node = ExpressionUtils.toNode(withRecord);
        validator.validateType(WITH_OBJECT_TYPE, node);
        return node;
    }

    private static ARecordType getWithObjectType() {
        final String[] withNames = { MERGE_POLICY_PARAMETER_NAME, STORAGE_BLOCK_COMPRESSION_PARAMETER_NAME };
        final IAType[] withTypes = { AUnionType.createUnknownableType(getMergePolicyType()),
                AUnionType.createUnknownableType(getStorageBlockCompressionType()) };
        return new ARecordType("withObject", withNames, withTypes, false);
    }

    private static ARecordType getMergePolicyType() {
        //merge-policy.parameters
        final String[] parameterNames = { MERGE_POLICY_MERGABLE_SIZE_PARAMETER_NAME,
                MERGE_POLICY_TOLERANCE_COUNT_PARAMETER_NAME, MERGE_POLICY_NUMBER_COMPONENTS_PARAMETER_NAME,

                MERGE_POLICY_MIN_COMPONENTS_PARAMETER_NAME, MERGE_POLICY_MAX_COMPONENTS_PARAMETER_NAME,
                MERGE_POLICY_MIN_DELAY_PARAMETER_NAME, MERGE_POLICY_MAX_DELAY_PARAMETER_NAME,
                MERGE_POLICY_LAMBDA_PARAMETER_NAME, MERGE_POLICY_PROBABILITY_PARAMETER_NAME,
                MERGE_POLICY_DISTRIBUTION_PARAMETER_NAME, MERGE_POLICY_LOW_BUCKET_PARAMETER_NAME,
                MERGE_POLICY_HIGH_BUCKET_PARAMETER_NAME, MERGE_POLICY_MIN_SSTABLE_SIZE_PARAMETER_NAME };
        final IAType[] parametersTypes = { AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.AINT64),

                AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE),
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE),
                AUnionType.createUnknownableType(BuiltinType.ASTRING),
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE),
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE),
                AUnionType.createUnknownableType(BuiltinType.AINT64) };
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
