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
package org.apache.asterix.common.annotations;

import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;

public class IntervalJoinExpressionAnnotation extends AbstractExpressionAnnotation {

    private static final String RAW_HINT_STRING = "interval-raw-join";
    private static final String PARTITION_HINT_STRING = "interval-partition-join";
    private static final String MERGE_HINT_STRING = "interval-merge-join";
    private static final String SPATIAL_HINT_STRING = "interval-spatial-join";
    private static final String INDEX_HINT_STRING = "interval-index-join";
    public static final IntervalJoinExpressionAnnotation INSTANCE = new IntervalJoinExpressionAnnotation();

    private IRangeMap map = null;
    private String joinType = null;
    private long leftMaxDuration = -1;
    private long rightMaxDuration = -1;
    private long leftRecordCount = -1;
    private long rightRecordCount = -1;
    private int tuplesPerFrame = -1;


    @Override
    public IExpressionAnnotation copy() {
        IntervalJoinExpressionAnnotation clone = new IntervalJoinExpressionAnnotation();
        clone.setObject(object);
        return clone;
    }

    @Override
    public void setObject(Object object) {
        super.setObject(object);
        parseHint();
    }

    private void parseHint() {
        String hint = (String) object;
        setJoinType(hint);

        hint = hint.substring(hint.indexOf(']', 0) + 2);
        String[] args = hint.split(" ");

        if (joinType.equals(PARTITION_HINT_STRING) && args.length == 5) {
            leftRecordCount = Long.valueOf(args[0]);
            rightRecordCount = Long.valueOf(args[1]);
            leftMaxDuration = Long.valueOf(args[2]);
            rightMaxDuration = Long.valueOf(args[3]);
            tuplesPerFrame = Integer.valueOf(args[4]);
        }
    }

    private void setJoinType(String hint) {
        if (hint.startsWith(RAW_HINT_STRING)) {
            joinType = RAW_HINT_STRING;
        } else if (hint.startsWith(PARTITION_HINT_STRING)) {
            joinType = PARTITION_HINT_STRING;
        } else if (hint.startsWith(MERGE_HINT_STRING)) {
            joinType = MERGE_HINT_STRING;
        } else if (hint.startsWith(SPATIAL_HINT_STRING)) {
            joinType = SPATIAL_HINT_STRING;
        } else if (hint.startsWith(INDEX_HINT_STRING)) {
            joinType = INDEX_HINT_STRING;
        }
    }

    public long getLeftMaxDuration() {
        return leftMaxDuration;
    }

    public long getRightMaxDuration() {
        return rightMaxDuration;
    }

    public long getLeftRecordCount() {
        return leftRecordCount;
    }

    public long getRightRecordCount() {
        return rightRecordCount;
    }

    public int getTuplesPerFrame() {
        return tuplesPerFrame;
    }

    public void setRangeMap(IRangeMap map) {
        this.map = map;
    }

    public IRangeMap getRangeMap() {
        return map;
    }

    public String getRangeType() {
        return joinType;
    }

    public boolean hasRangeArgument() {
        if (joinType.equals(RAW_HINT_STRING)) {
            return false;
        }
        return true;
    }

    public boolean isRawJoin() {
        if (joinType.equals(RAW_HINT_STRING)) {
            return true;
        }
        return false;
    }

    public boolean isPartitionJoin() {
        if (joinType.equals(PARTITION_HINT_STRING)) {
            return true;
        }
        return false;
    }

    public boolean isMergeJoin() {
        if (joinType.equals(MERGE_HINT_STRING)) {
            return true;
        }
        return false;
    }

    public boolean isSpatialJoin() {
        if (joinType.equals(SPATIAL_HINT_STRING)) {
            return true;
        }
        return false;
    }

    public boolean isIndexJoin() {
        if (joinType.equals(INDEX_HINT_STRING)) {
            return true;
        }
        return false;
    }

    public static boolean isIntervalJoinHint(String hint) {
        return hint.startsWith(RAW_HINT_STRING) || hint.startsWith(PARTITION_HINT_STRING)
                || hint.startsWith(MERGE_HINT_STRING) || hint.startsWith(SPATIAL_HINT_STRING)
                || hint.startsWith(INDEX_HINT_STRING);
    }

    public static int getHintLength(String hint) {
        if (hint.startsWith(RAW_HINT_STRING)) {
            return RAW_HINT_STRING.length();
        } else if (hint.startsWith(PARTITION_HINT_STRING)) {
            return PARTITION_HINT_STRING.length();
        } else if (hint.startsWith(MERGE_HINT_STRING)) {
            return MERGE_HINT_STRING.length();
        } else if (hint.startsWith(SPATIAL_HINT_STRING)) {
            return SPATIAL_HINT_STRING.length();
        } else if (hint.startsWith(INDEX_HINT_STRING)) {
            return INDEX_HINT_STRING.length();
        }
        return 0;
    }

}