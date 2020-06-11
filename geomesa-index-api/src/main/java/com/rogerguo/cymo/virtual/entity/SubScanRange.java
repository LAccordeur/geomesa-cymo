package com.rogerguo.cymo.virtual.entity;

/**
 * @Description
 * @Author GUO Yang
 * @Date 2019-12-10 5:36 PM
 */
public class SubScanRange {

    private long lowBound;

    private long highBound;

    public SubScanRange() {}

    public SubScanRange(long lowBound, long highBound) {
        this.lowBound = lowBound;
        this.highBound = highBound;
    }

    public long getLowBound() {
        return lowBound;
    }

    public void setLowBound(long lowBound) {
        this.lowBound = lowBound;
    }

    public long getHighBound() {
        return highBound;
    }

    public void setHighBound(long highBound) {
        this.highBound = highBound;
    }

    @Override
    public String toString() {
        return "SubScanRange{" +
                "lowBound=" + lowBound +
                ", highBound=" + highBound +
                '}';
    }
}
