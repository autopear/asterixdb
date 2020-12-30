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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LevelMergePolicyFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class LevelRTreeMergePolicyFactory implements ILSMMergePolicyFactory {

    private static final long serialVersionUID = 1L;
    public static final String NAME = "level-rtree";
    public static final String PARTITION = "partition";
    public static final String PARTITION_SIZE = "size";
    public static final String PARTITION_STR = "str";
    public static final String PARTITION_RSGROVE = "rs-grove";
    public static final Set<String> PROPERTIES_NAMES =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(BTREE_INDEX, INVERTED_INDEX, RTREE_INDEX,
                    RTREE_COMPARATOR, LevelMergePolicyFactory.PICK, LevelMergePolicyFactory.NUM_COMPONENTS_0,
                    LevelMergePolicyFactory.NUM_COMPONENTS_1, PARTITION/*, OVERLAP_MODE*/)));

    public static final Map<String, String> DEFAULT_PROPERTIES_FOR_PRIMARY_INDEX = new LinkedHashMap<String, String>() {
        {
            putAll(LevelMergePolicyFactory.DEFAULT_PROPERTIES_FOR_PRIMARY_INDEX);
            put(PARTITION, PARTITION_SIZE);
            put(RTREE_COMPARATOR, RTREE_COMPARATOR_HILBERT);
        }
    };

    public static final Map<String, String> DEFAULT_PROPERTIES_FOR_SECONDARY_INDEX =
            new LinkedHashMap<String, String>() {
                {
                    putAll(LevelMergePolicyFactory.DEFAULT_PROPERTIES_FOR_SECONDARY_INDEX);
                    put(PARTITION, PARTITION_SIZE);
                    put(RTREE_COMPARATOR, RTREE_COMPARATOR_HILBERT);
                }
            };

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Set<String> getPropertiesNames() {
        return PROPERTIES_NAMES;
    }

    @Override
    public Map<String, String> getDefaultPropertiesForPrimaryIndex() {
        return DEFAULT_PROPERTIES_FOR_PRIMARY_INDEX;
    }

    @Override
    public Map<String, String> getDefaultPropertiesForBTreeIndex() {
        return DEFAULT_PROPERTIES_FOR_SECONDARY_INDEX;
    }

    @Override
    public Map<String, String> getDefaultPropertiesForInvertedIndex() {
        return DEFAULT_PROPERTIES_FOR_SECONDARY_INDEX;
    }

    @Override
    public Map<String, String> getDefaultPropertiesForRTreeIndex() {
        return DEFAULT_PROPERTIES_FOR_SECONDARY_INDEX;
    }

    @Override
    public ILSMMergePolicy createMergePolicy(Map<String, String> configuration, INCServiceContext ctx) {
        ILSMMergePolicy policy = new LevelRTreeMergePolicy();
        policy.configure(configuration);
        return policy;
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        return registry.getClassIdentifier(getClass(), serialVersionUID);
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return new LevelMergePolicyFactory();
    }
}
