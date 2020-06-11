package com.rogerguo.cymo.virtual.entity;



/**
 * @Description
 * @Author GUO Yang
 * @Date 2019-12-10 5:53 PM
 */
public class SubScanItem {

    private SubScanRange subScanRange;

    private int blockNumber;

    private int distanceToNextSubScan;

    @Deprecated
    public SubScanItem(int blockNumber, int distanceToNextSubScan) {
        this.blockNumber = blockNumber;
        this.distanceToNextSubScan = distanceToNextSubScan;
    }

    @Deprecated
    public SubScanItem(SubScanRange subScanRange, int blockNumber, int distanceToNextSubScan) {
        this.subScanRange = subScanRange;
        this.blockNumber = blockNumber;
        this.distanceToNextSubScan = distanceToNextSubScan;
    }

    public SubScanItem(SubScanRange subScanRange) {
        this.subScanRange = subScanRange;
    }

    public SubScanItem(SubScanRange subScanRange, int blockNumber) {
        this.subScanRange = subScanRange;
        this.blockNumber = blockNumber;
    }

    public SubScanItem() {this.subScanRange = new SubScanRange();}

    @Override
    public String toString() {
        return "SubScanItem{" +
                "subScanRange=" + subScanRange +
                ", blockNumber=" + blockNumber +
                ", distanceToNextSubScan=" + distanceToNextSubScan +
                '}';
    }


    public int getBlockNumber() {
        return blockNumber;
    }

    public void setBlockNumber(int blockNumber) {
        this.blockNumber = blockNumber;
    }

    public SubScanRange getSubScanRange() {
        return subScanRange;
    }

    public void setSubScanRange(SubScanRange subScanRange) {
        this.subScanRange = subScanRange;
    }

    public int getDistanceToNextSubScan() {
        return distanceToNextSubScan;
    }

    public void setDistanceToNextSubScan(int distanceToNextSubScan) {
        this.distanceToNextSubScan = distanceToNextSubScan;
    }
}
