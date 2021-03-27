package com.rogerguo.client.cymo;

/**
 * @Description
 * @Date 2020/3/18 14:40
 * @Created by X1 Carbon
 */
public class QuerySegment {

    private long lowBound;

    private long highBound;

    private long distanceToNextSegment;

    @Override
    public String toString() {
        return "QuerySegment{" +
                "lowBound=" + lowBound +
                ", highBound=" + highBound +
                ", distanceToNextSegment=" + distanceToNextSegment +
                '}';
    }

    public long getDistanceToNextSegment() {
        return distanceToNextSegment;
    }

    public void setDistanceToNextSegment(long distanceToNextSegment) {
        this.distanceToNextSegment = distanceToNextSegment;
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
