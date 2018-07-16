package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Created by nakshikatha on 5/7/18.
 */


public class Point {
    double x;
    double y;
    public Point(double x, double y)
    {
        this.x = x;
        this.y = y;
    }
    public Point()
    {
        this.x = 0;
        this.y = 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(this.x);
        result = (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(this.y);
        result = 31 * result + (int) (temp ^ temp >>> 32);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        Point r2 = (Point) obj;
        return this.x == r2.x && this.y == r2.y;
    }

    public int compareTo(Point o) {
        if (x < o.x)
            return -1;
        if (x > o.x)
            return 1;
        if (y < o.y)
            return -1;
        if (y > o.y)
            return 1;
        return 0;
    }

    public double getX(){return x;}
    public double getY(){return y;}
    public void getY(double y){this.y = y;}
    public void getX(double x){this.x = x;}

}
