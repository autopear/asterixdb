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
package org.apache.hyracks.data.std.accessors;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;

import com.fasterxml.jackson.databind.JsonNode;

public class RawBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;
    public static final IBinaryComparatorFactory INSTANCE = new RawBinaryComparatorFactory();

    private RawBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return RawBinaryComparatorFactory::compare;
    }

    @Override
    public String getTypeName() {
        return "ByteArray";
    }

    @Override
    public String byteToString(byte[] b, int s, int l) {
        if (b == null || b.length == 0 || l == 0 || s >= b.length) {
            return "";
        } else {
            byte[] b1 = new byte[l];
            System.arraycopy(b, s, b1, 0, l);
            return ByteArrayPointable.bytesToHex(b1);
        }
    }

    @Override
    public String byteToString(byte[] b) {
        return (b == null || b.length == 0) ? "" : byteToString(b, 0, b.length);
    }

    public static int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int commonLength = Math.min(l1, l2);
        for (int i = 0; i < commonLength; i++) {
            if (b1[s1 + i] != b2[s2 + i]) {
                return b1[s1 + i] - b2[s2 + i];
            }
        }
        return Integer.compare(l1, l2);
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        return registry.getClassIdentifier(getClass(), serialVersionUID);
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return INSTANCE;
    }
}
