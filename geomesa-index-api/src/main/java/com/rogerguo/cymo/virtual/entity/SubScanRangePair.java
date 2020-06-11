package com.rogerguo.cymo.virtual.entity;

/**
 * @Description
 * @Date 6/6/20 5:10 PM
 * @Created by rogerguo
 */
public class SubScanRangePair {

    private int partitionID;

    private long subspaceID;

    private long cellIDLowBound;

    private long cellIDHighBound;

    public SubScanRangePair(int partitionID, long subspaceID, long cellIDLowBound, long cellIDHighBound) {
        this.partitionID = partitionID;
        this.subspaceID = subspaceID;
        this.cellIDLowBound = cellIDLowBound;
        this.cellIDHighBound = cellIDHighBound;
    }

    @Override
    public String toString() {
        return "SubScanRangePair{" +
                "partitionID=" + partitionID +
                ", subspaceID=" + subspaceID +
                ", cellIDLowBound=" + cellIDLowBound +
                ", cellIDHighBound=" + cellIDHighBound +
                '}';
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
    }

    public long getSubspaceID() {
        return subspaceID;
    }

    public void setSubspaceID(long subspaceID) {
        this.subspaceID = subspaceID;
    }

    public long getCellIDLowBound() {
        return cellIDLowBound;
    }

    public void setCellIDLowBound(long cellIDLowBound) {
        this.cellIDLowBound = cellIDLowBound;
    }

    public long getCellIDHighBound() {
        return cellIDHighBound;
    }

    public void setCellIDHighBound(long cellIDHighBound) {
        this.cellIDHighBound = cellIDHighBound;
    }
}
