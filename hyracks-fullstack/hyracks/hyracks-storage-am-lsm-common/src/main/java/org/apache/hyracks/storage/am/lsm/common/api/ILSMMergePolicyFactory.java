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
package org.apache.hyracks.storage.am.lsm.common.api;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.io.IJsonSerializable;

public interface ILSMMergePolicyFactory extends Serializable, IJsonSerializable {

    public static final String BTREE_INDEX_PROPERTIES = "btree-properties";
    public static final String INVERTED_INDEX_PROPERTIES = "inverted-index-properties";
    public static final String RTREE_INDEX_PROPERTIES = "rtree-properties";

    public static final Set<String> INDEX_PROPERTIES_NAMES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(BTREE_INDEX_PROPERTIES, INVERTED_INDEX_PROPERTIES, RTREE_INDEX_PROPERTIES)));

    ILSMMergePolicy createMergePolicy(Map<String, String> configuration, INCServiceContext ctx);

    String getName();

    Set<String> getPropertiesNames();

    Map<String, String> getDefaultPropertiesForPrimaryIndex();

    Map<String, String> getDefaultPropertiesForBTreeIndex();

    Map<String, String> getDefaultPropertiesForInvertedIndex();

    Map<String, String> getDefaultPropertiesForRTreeIndex();
}
