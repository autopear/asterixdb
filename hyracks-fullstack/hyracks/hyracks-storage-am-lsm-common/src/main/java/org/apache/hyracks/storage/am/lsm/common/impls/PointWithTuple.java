package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Created by mohiuddin on 7/15/18.
 */
public class PointWithTuple {
    public Point point;
    public ITupleReference tuple;

    public PointWithTuple(Point p, ITupleReference t) {
        point = p;
        tuple = t;
    }


}

