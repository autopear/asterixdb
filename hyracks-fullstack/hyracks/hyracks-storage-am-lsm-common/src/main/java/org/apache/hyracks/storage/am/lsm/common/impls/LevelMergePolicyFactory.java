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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class LevelMergePolicyFactory implements ILSMMergePolicyFactory {

    private static final long serialVersionUID = 1L;
    public static final String NAME = "level";
    public static final String PICK = "pick";
    public static final String NUM_COMPONENTS_0 = "num-components-0";
    public static final String NUM_COMPONENTS_1 = "num-components-1";
    //    public static final Set<String> PROPERTIES_NAMES =
    //            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(PICK, NUM_COMPONENTS_0, NUM_COMPONENTS_1)));
    public static final Set<String> PROPERTIES_NAMES = Collections.emptySet();

    public static final String OLDEST = "oldest";
    public static final String NEWEST = "newest";
    public static final String BEST = "best";
    public static final String MIN_OVERLAP = "min-overlap";
    public static final String MAX_OVERLAP = "max-overlap";
    public static final String RAND_UNIFORM = "rand-uniform";
    public static final String RAND_BINOMIAL = "rand-binomial";
    public static final String RAND_OLDEST = "rand-oldest";
    public static final String RAND_LATEST = "rand-latest";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Set<String> getPropertiesNames() {
        return PROPERTIES_NAMES;
    }

    @Override
    public ILSMMergePolicy createMergePolicy(Map<String, String> configuration, INCServiceContext ctx) {
        ILSMMergePolicy policy = new LevelMergePolicy();
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
