package com.rogerguo.cymo.virtual.entity;



/**
 * @Description
 * @Author GUO Yang
 * @Date 2019-12-07 1:12 PM
 */
public class NormalizedLocation {

    private int x;

    private int y;

    private int t;

    public NormalizedLocation(int x, int y, int t) {
        this.x = x;
        this.y = y;
        this.t = t;
    }

    @Override
    public String toString() {
        return "NormalizedLocation{" +
                "x=" + x +
                ", y=" + y +
                ", t=" + t +
                '}';
    }

    public NormalizedLocation() {}

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public int getT() {
        return t;
    }

    public void setT(int t) {
        this.t = t;
    }
}
