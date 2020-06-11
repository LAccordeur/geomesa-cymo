package com.rogerguo.cymo.entity;

/**
 * @Description
 * @Date 6/5/20 4:11 PM
 * @Created by rogerguo
 */
public class SpatialRange {
    private double lowBound;

    private double highBound;

    public static boolean isInRange(double value, SpatialRange range) {
        if (value >= range.getLowBound() && value <= range.getHighBound()) {
            return true;
        }
        return false;
    }

    public SpatialRange(double lowBound, double highBound) {
        this.lowBound = lowBound;
        this.highBound = highBound;
    }

    @Override
    public String toString() {
        return "Range{" +
                "lowBound=" + lowBound +
                ", highBound=" + highBound +
                '}';
    }

    public double getLowBound() {
        return lowBound;
    }

    public void setLowBound(double lowBound) {
        this.lowBound = lowBound;
    }

    public double getHighBound() {
        return highBound;
    }

    public void setHighBound(double highBound) {
        this.highBound = highBound;
    }
}
