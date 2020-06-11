package com.rogerguo.cymo.virtual.entity;

/**
 * @Description
 * @Author GUO Yang
 * @Date 2019-12-18 7:31 PM
 */
public class NormalizedRange {

    private int lowBound;

    private int highBound;

    public NormalizedRange(int lowBound, int highBound) {
        this.lowBound = lowBound;
        this.highBound = highBound;
    }

    @Override
    public String toString() {
        return "NormalizedRange{" +
                "lowBound=" + lowBound +
                ", highBound=" + highBound +
                '}';
    }
    public static boolean isInRange(int value, NormalizedRange range) {
        if (value >= range.getLowBound() && value <= range.getHighBound()) {
            return true;
        }
        return false;
    }


    public int getLowBound() {
        return lowBound;
    }

    public void setLowBound(int lowBound) {
        this.lowBound = lowBound;
    }

    public int getHighBound() {
        return highBound;
    }

    public void setHighBound(int highBound) {
        this.highBound = highBound;
    }
}
