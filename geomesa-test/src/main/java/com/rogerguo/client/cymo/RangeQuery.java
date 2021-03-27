package com.rogerguo.client.cymo;

/**
 * @Description
 * @Date 2020/4/1 21:41
 * @Created by X1 Carbon
 */
public class RangeQuery {

    private int xStart;

    private int xStop;

    private int yStart;

    private int yStop;

    private int tStart;

    private int tStop;

    @Override
    public String toString() {
        return "RangeQuery{" +
                "xStart=" + xStart +
                ", xStop=" + xStop +
                ", yStart=" + yStart +
                ", yStop=" + yStop +
                ", tStart=" + tStart +
                ", tStop=" + tStop +
                '}';
    }

    public boolean isInRange(int x, int y, int t) {
        if (x >= xStart && x <= xStop
        && y >= yStart && y <= yStop
        && t >= tStart && t <= tStop) {
            return true;
        }
        return false;
    }

    public int getxStart() {
        return xStart;
    }

    public void setxStart(int xStart) {
        this.xStart = xStart;
    }

    public int getxStop() {
        return xStop;
    }

    public void setxStop(int xStop) {
        this.xStop = xStop;
    }

    public int getyStart() {
        return yStart;
    }

    public void setyStart(int yStart) {
        this.yStart = yStart;
    }

    public int getyStop() {
        return yStop;
    }

    public void setyStop(int yStop) {
        this.yStop = yStop;
    }

    public int gettStart() {
        return tStart;
    }

    public void settStart(int tStart) {
        this.tStart = tStart;
    }

    public int gettStop() {
        return tStop;
    }

    public void settStop(int tStop) {
        this.tStop = tStop;
    }

    public RangeQuery(int xStart, int xStop, int yStart, int yStop, int tStart, int tStop) {
        this.xStart = xStart;
        this.xStop = xStop;
        this.yStart = yStart;
        this.yStop = yStop;
        this.tStart = tStart;
        this.tStop = tStop;
    }
}
