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
package org.apache.hyracks.storage.am.rtree.linearize;

import org.apache.hyracks.api.dataflow.value.ILinearizeComparator;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.primitive.DoublePointable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SimpleDoubleComparatorFactory implements ILinearizeComparatorFactory {
    private static final long serialVersionUID = 1L;

    private int dim;

    private String typeName;

    public static SimpleDoubleComparatorFactory get(int dim) {
        return new SimpleDoubleComparatorFactory(dim);
    }

    public SimpleDoubleComparatorFactory(int dim) {
        this.dim = dim;
        StringBuilder s = new StringBuilder("<Double");
        for (int i = 1; i < dim; i++) {
            s.append(",Double");
        }
        s.append(">");
        this.typeName = s.toString();
    }

    @Override
    public ILinearizeComparator createBinaryComparator() {
        return new SimpleDoubleComparator(dim);
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

    @Override
    public String byteToString(byte[] b, int s, int l) {
        if (b == null || b.length == 0 || l == 0 || s >= b.length) {
            return "";
        } else {
            StringBuilder sb = new StringBuilder("[" + DoublePointable.getDouble(b, s));
            for (int i = 1; i < dim; i++) {
                sb.append("," + DoublePointable.getDouble(b, s + (i * l)));
            }
            sb.append("]");
            return sb.toString();
        }
    }

    @Override
    public String byteToString(byte[] b) {
        return (b == null || b.length == 0) ? "" : byteToString(b, 0, b.length);
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put("dim", dim);
        return json;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return get(json.get("dim").asInt());
    }
}
