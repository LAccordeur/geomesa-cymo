package com.rogerguo.cymo.virtual.entity;


import com.rogerguo.cymo.config.VirtualLayerConfiguration;

/**
 * @Description
 * @Author GUO Yang
 * @Date 2019-12-18 3:57 PM
 */
public class PartitionLocation {

    private int partitionID;   // originalNormalizedTime / temporal interval

    private int normalizedPartitionLength; // unit: TEMPORAL_VIRTUAL_GRANULARITY

    private int originalNormalizedTime;

    public PartitionLocation(NormalizedLocation normalizedLocation) {
        this.originalNormalizedTime = normalizedLocation.getT();
        int temporalPartitionPairLength = VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH + VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH;
        int partitionIndex = (normalizedLocation.getT() + VirtualLayerConfiguration.TEMPORAL_PARTITION_OFFSET) / temporalPartitionPairLength;
        int partitionOffset = (normalizedLocation.getT() + VirtualLayerConfiguration.TEMPORAL_PARTITION_OFFSET) % temporalPartitionPairLength;
        if ((partitionOffset - VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH) < 0) {
            this.partitionID = 2 * partitionIndex;
            this.normalizedPartitionLength = VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH;
        } else {
            this.partitionID = 2 * partitionIndex + 1;
            this.normalizedPartitionLength = VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH;
        }

    }

    @Override
    public String toString() {
        return "PartitionLocation{" +
                "partitionID=" + partitionID +
                ", normalizedPartitionLength=" + normalizedPartitionLength +
                ", originalNormalizedTime=" + originalNormalizedTime +
                '}';
    }

    public int getNormalizedPartitionLength() {
        return normalizedPartitionLength;
    }

    public void setNormalizedPartitionLength(int normalizedPartitionLength) {
        this.normalizedPartitionLength = normalizedPartitionLength;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
    }

    public int getOriginalNormalizedTime() {
        return originalNormalizedTime;
    }

    public void setOriginalNormalizedTime(int originalNormalizedTime) {
        this.originalNormalizedTime = originalNormalizedTime;
    }
}
