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
package org.apache.asterix.dataflow.data.nontagged.comparators;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The ascending interval comparator sorts intervals first by start point, then by end point. If the intervals have
 * the same point values, the final comparison orders the intervals by type (datetime, date, time).
 */
public class AIntervalAscPartialBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;
    public static final AIntervalAscPartialBinaryComparatorFactory INSTANCE =
            new AIntervalAscPartialBinaryComparatorFactory();

    private AIntervalAscPartialBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return AIntervalAscPartialBinaryComparatorFactory::compare;
    }

    @Override
    public String getTypeName() {
        return "Interval";
    }

    @Override
    public String byteToString(byte[] b, int s, int l) {
        if (b == null || b.length == 0 || l == 0 || s >= b.length) {
            return "";
        } else {
            long st = AIntervalSerializerDeserializer.getIntervalStart(b, s);
            long ed = AIntervalSerializerDeserializer.getIntervalEnd(b, s);
            long it = AIntervalSerializerDeserializer.getIntervalTimeType(b, s);

            return "[" + st + "," + ed + "," + it + "]";
        }
    }

    @Override
    public String byteToString(byte[] b) {
        return (b == null || b.length == 0) ? "" : byteToString(b, 0, b.length);
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int c = Long.compare(AIntervalSerializerDeserializer.getIntervalStart(b1, s1),
                AIntervalSerializerDeserializer.getIntervalStart(b2, s2));
        if (c == 0) {
            c = Long.compare(AIntervalSerializerDeserializer.getIntervalEnd(b1, s1),
                    AIntervalSerializerDeserializer.getIntervalEnd(b2, s2));
            if (c == 0) {
                c = Byte.compare(AIntervalSerializerDeserializer.getIntervalTimeType(b1, s1),
                        AIntervalSerializerDeserializer.getIntervalTimeType(b2, s2));
            }
        }
        return c;
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
