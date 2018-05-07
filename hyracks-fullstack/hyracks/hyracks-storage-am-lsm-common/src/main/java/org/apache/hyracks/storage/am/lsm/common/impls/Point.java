package org.apache.hyracks.storage.am.lsm.common.impls;

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
    public double getX(){return x;}
    public double getY(){return y;}
    public void getY(double y){this.y = y;}
    public void getX(double x){this.x = x;}

}
