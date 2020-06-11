package com.rogerguo.cymo.entity;

/**
 * @Description
 * @Date 6/5/20 4:11 PM
 * @Created by rogerguo
 */
public class TimeRange {
    private long lowBound;

    private long highBound;

    public TimeRange(long lowBound, long highBound) {
        this.lowBound = lowBound;
        this.highBound = highBound;
    }

    public static boolean isInRange(long value, TimeRange timeRange) {
        if (value >= timeRange.getLowBound() && value <= timeRange.getHighBound()) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "TimeRange{" +
                "lowBound=" + lowBound +
                ", highBound=" + highBound +
                '}';
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
}

