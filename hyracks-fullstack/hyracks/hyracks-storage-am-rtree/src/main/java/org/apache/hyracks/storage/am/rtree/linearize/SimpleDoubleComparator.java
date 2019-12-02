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
import org.apache.hyracks.data.std.primitive.DoublePointable;

public class SimpleDoubleComparator implements ILinearizeComparator {
    private final int dim; // dimension

    public SimpleDoubleComparator(int dimension) {
        dim = dimension;
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        for (int i = 0; i < dim; i++) {
            int diff = Double.compare(DoublePointable.getDouble(b1, s1 + (i * l1)),
                    DoublePointable.getDouble(b2, s2 + (i * l2)));
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }

    @Override
    public int getDimensions() {
        return dim;
    }
}
