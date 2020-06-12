package com.rogerguo.cymo.entity;

/**
 * @Description
 * @Author GUO Yang
 * @Date 2019-12-03 2:43 PM
 */
public class SpatialTemporalRecord implements Comparable<SpatialTemporalRecord>  {

    private String id;

    private double longitude;

    private double latitude;

    private long timestamp;

    private String dataPayload;

    public SpatialTemporalRecord(String id, double longitude, double latitude, long timestamp) {
        this.id = id;
        this.longitude = longitude;
        this.latitude = latitude;
        this.timestamp = timestamp;
    }

    public SpatialTemporalRecord(String id, double longitude, double latitude, long timestamp, String dataPayload) {
        this.id = id;
        this.longitude = longitude;
        this.latitude = latitude;
        this.timestamp = timestamp;
        this.dataPayload = dataPayload;
    }

    @Override
    public String toString() {
        return "SpatialTemporalRecord{" +
                "id='" + id + '\'' +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", timestamp=" + timestamp +
                ", dataPayload='" + dataPayload + '\'' +
                '}';
    }

    @Override
    public int compareTo(SpatialTemporalRecord o) {
        return this.id.compareTo(o.getId());
    }

    public String getDataPayload() {
        return dataPayload;
    }

    public void setDataPayload(String dataPayload) {
        this.dataPayload = dataPayload;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
