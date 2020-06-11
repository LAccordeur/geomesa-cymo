package com.rogerguo.cymo.hbase;


import java.util.Arrays;

/**
 * @Description
 * @Author GUO Yang
 * @Date 2019-12-18 5:37 PM
 */
public class RowKeyItem {

    private byte[] bytesRowKey;

    private String stringRowKey;

    private long qualifier;

    private int partitionID;

    private long subspaceID;

    private long cellID;

    private byte type; // 0: table; 1: index

    private String testPayload;

    private Object bitmapPayload;  // CellLocation

    public RowKeyItem(byte[] bytesRowKey, String stringRowKey) {
        this.bytesRowKey = bytesRowKey;
        this.stringRowKey = stringRowKey;
    }

    public RowKeyItem(byte[] bytesRowKey, String stringRowKey, long qualifier, byte type) {
        this.bytesRowKey = bytesRowKey;
        this.stringRowKey = stringRowKey;
        this.qualifier = qualifier;
        this.type = type;
    }

    public RowKeyItem(byte[] bytesRowKey, String stringRowKey, long qualifier, byte type, Object bitmapPayload) {
        this.bytesRowKey = bytesRowKey;
        this.stringRowKey = stringRowKey;
        this.qualifier = qualifier;
        this.type = type;
        this.bitmapPayload = bitmapPayload;
    }

    public RowKeyItem(byte[] bytesRowKey, String stringRowKey, long qualifier, long subspaceID, byte type, Object bitmapPayload) {
        this.bytesRowKey = bytesRowKey;
        this.stringRowKey = stringRowKey;
        this.qualifier = qualifier;
        this.subspaceID = subspaceID;
        this.type = type;
        this.bitmapPayload = bitmapPayload;
    }

    public RowKeyItem(byte[] bytesRowKey, String stringRowKey, long qualifier, int partitionID, long subspaceID, byte type, Object bitmapPayload) {
        this.bytesRowKey = bytesRowKey;
        this.stringRowKey = stringRowKey;
        this.qualifier = qualifier;
        this.partitionID = partitionID;
        this.subspaceID = subspaceID;
        this.type = type;
        this.bitmapPayload = bitmapPayload;
    }

    public RowKeyItem(byte[] bytesRowKey, String stringRowKey, long qualifier, int partitionID, long subspaceID, long cellID, byte type) {
        this.bytesRowKey = bytesRowKey;
        this.stringRowKey = stringRowKey;
        this.qualifier = qualifier;
        this.partitionID = partitionID;
        this.subspaceID = subspaceID;
        this.cellID = cellID;
        this.type = type;
    }

    public RowKeyItem(byte[] bytesRowKey, String stringRowKey, long qualifier, int partitionID, long subspaceID, long cellID, byte type, String testPayload) {
        this.bytesRowKey = bytesRowKey;
        this.stringRowKey = stringRowKey;
        this.qualifier = qualifier;
        this.partitionID = partitionID;
        this.subspaceID = subspaceID;
        this.cellID = cellID;
        this.type = type;
        this.testPayload = testPayload;
    }

    public RowKeyItem(String stringRowKey, int partitionID, long subspaceID) {
        this.stringRowKey = stringRowKey;
        this.partitionID = partitionID;
        this.subspaceID = subspaceID;
    }

    @Override
    public String toString() {
        return "RowKeyItem{" +
                "bytesRowKey=" + Arrays.toString(bytesRowKey) +
                ", stringRowKey='" + stringRowKey + '\'' +
                ", qualifier=" + qualifier +
                ", partitionID=" + partitionID +
                ", subspaceID=" + subspaceID +
                ", cellID=" + cellID +
                ", type=" + type +
                '}';
    }

    public Object getBitmapPayload() {
        return bitmapPayload;
    }

    public void setBitmapPayload(Object bitmapPayload) {
        this.bitmapPayload = bitmapPayload;
    }

    public String getTestPayload() {
        return testPayload;
    }

    public void setTestPayload(String testPayload) {
        this.testPayload = testPayload;
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

    public long getCellID() {
        return cellID;
    }

    public void setCellID(long cellID) {
        this.cellID = cellID;
    }

    public byte[] getBytesRowKey() {
        return bytesRowKey;
    }

    public void setBytesRowKey(byte[] bytesRowKey) {
        this.bytesRowKey = bytesRowKey;
    }

    public String getStringRowKey() {
        return stringRowKey;
    }

    public void setStringRowKey(String stringRowKey) {
        this.stringRowKey = stringRowKey;
    }

    public long getQualifier() {
        return qualifier;
    }

    public void setQualifier(long qualifier) {
        this.qualifier = qualifier;
    }
}
