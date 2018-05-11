package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.List;

/**
 * Created by mohiuddin on 5/9/18.
 */
public class Rectangle {

        public double x1;
        public double y1;
        public double x2;
        public double y2;

        public Rectangle() {
            this(0, 0, 0, 0);
        }

        public Rectangle(Rectangle r) {
            this(r.x1, r.y1, r.x2, r.y2);
        }

        public Rectangle(double x1, double y1, double x2, double y2) {
            this.set(x1, y1, x2, y2);
        }

        public Rectangle(List<Double> points) {
            this.set(points.get(0), points.get(1), points.get(2), points.get(3));
        }

        public void set(Rectangle mbr) {
            if (mbr == null) {
                //System.out.println("tozz");
                return;
            }
            set(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
        }
        public void set(double x1, double y1, double x2, double y2) {
            this.x1 = x1;
            this.y1 = y1;
            this.x2 = x2;
            this.y2 = y2;
        }
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            Rectangle r2 = (Rectangle) obj;
            boolean result = this.x1 == r2.x1 && this.y1 == r2.y1
                    && this.x2 == r2.x2 && this.y2 == r2.y2;
            return result;
        }
        public boolean isIntersected(Rectangle r) {

            if (r == null)
                return false;
            return (this.x2 > r.x1 && r.x2 > this.x1 && this.y2 > r.y1 && r.y2 > this.y1);
        }
        public boolean isIntersected(Point pt) {

            return pt.x >= x1 && pt.x < x2 && pt.y >= y1 && pt.y < y2;
        }

        public Rectangle getIntersection(Rectangle r) {
            if (!r.isIntersected(this))
                return null;
            double ix1 = Math.max(this.x1, r.x1);
            double ix2 = Math.min(this.x2, r.x2);
            double iy1 = Math.max(this.y1, r.y1);
            double iy2 = Math.min(this.y2, r.y2);
            return new Rectangle(ix1, iy1, ix2, iy2);
        }

        public Point getCenterPoint() {
            return new Point((x1 + x2) / 2, (y1 + y2)/2);
        }

        public boolean isLine() {
            return (x1 == x2 || y1 == y2);
        }

        public void adjustMBR(final Rectangle r) {
            if (r.x1 < this.x1)
                this.x1 = r.x1;
            if (r.x2 > this.x2)
                this.x2 = r.x2;
            if (r.y1 < this.y1)
                this.y1 = r.y1;
            if (r.y2 > this.y2)
                this.y2 = r.y2;
        }

        public double getHeight() {
            return y2 - y1;
        }

        public double getWidth() {
            return x2 - x1;
        }
    }
