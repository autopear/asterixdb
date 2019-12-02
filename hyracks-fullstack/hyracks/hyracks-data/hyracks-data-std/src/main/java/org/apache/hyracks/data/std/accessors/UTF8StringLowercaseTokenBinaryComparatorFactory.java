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
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.primitive.UTF8StringLowercaseTokenPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;

import com.fasterxml.jackson.databind.JsonNode;

public final class UTF8StringLowercaseTokenBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;
    public static final UTF8StringLowercaseTokenBinaryComparatorFactory INSTANCE =
            new UTF8StringLowercaseTokenBinaryComparatorFactory();

    private UTF8StringLowercaseTokenBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return UTF8StringLowercaseTokenPointable::compare;
    }

    @Override
    public String getTypeName() {
        return "String";
    }

    @Override
    public String byteToString(byte[] b, int s, int l) {
        if (b == null || b.length == 0 || l == 0 || s >= b.length) {
            return "";
        } else {
            byte[] b1 = new byte[l];
            System.arraycopy(b, s, b1, 0, l);
            return "\"" + UTF8StringUtil.toString(b1, 0).toLowerCase() + "\"";
        }
    }

    @Override
    public String byteToString(byte[] b) {
        return (b == null || b.length == 0) ? "" : byteToString(b, 0, b.length);
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) {
        return registry.getClassIdentifier(getClass(), serialVersionUID);
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return INSTANCE;
    }
}
