package com.rogerguo.cymo.virtual.entity;


import com.rogerguo.cymo.config.VirtualLayerConfiguration;

/**
 * @Description
 * @Author GUO Yang
 * @Date 2019-12-18 4:34 PM
 */
public class CellLocation extends SubspaceLocation {

    private int cellLongitude;

    private int cellLatitude;

    private int cellTime;

    private int originalCellLongitude;

    private int originalCellLatitude;

    private int originalCellTime;

    public CellLocation(NormalizedLocation normalizedLocation) {
        super(normalizedLocation);
        this.originalCellLongitude = normalizedLocation.getX();
        this.originalCellLatitude = normalizedLocation.getY();
        this.originalCellTime = normalizedLocation.getT();
        this.cellLongitude = this.originalCellLongitude / VirtualLayerConfiguration.SPATIAL_VIRTUAL_LONGITUDE_GRANULARITY;
        this.cellLatitude = this.originalCellLatitude / VirtualLayerConfiguration.SPATIAL_VIRTUAL_LATITUDE_GRANULARITY;
        this.cellTime = this.originalCellTime;
    }

    @Override
    public String toString() {
        return super.toString() + "\nCellLocation{" +
                "cellLongitude=" + cellLongitude +
                ", cellLatitude=" + cellLatitude +
                ", cellTime=" + cellTime +
                ", originalCellLongitude=" + originalCellLongitude +
                ", originalCellLatitude=" + originalCellLatitude +
                ", originalCellTime=" + originalCellTime +
                '}';
    }

    public int getCellLongitude() {
        return cellLongitude;
    }

    public void setCellLongitude(int cellLongitude) {
        this.cellLongitude = cellLongitude;
    }

    public int getCellLatitude() {
        return cellLatitude;
    }

    public void setCellLatitude(int cellLatitude) {
        this.cellLatitude = cellLatitude;
    }

    public int getCellTime() {
        return cellTime;
    }

    public void setCellTime(int cellTime) {
        this.cellTime = cellTime;
    }

    public int getOriginalCellLongitude() {
        return originalCellLongitude;
    }

    public void setOriginalCellLongitude(int originalCellLongitude) {
        this.originalCellLongitude = originalCellLongitude;
    }

    public int getOriginalCellLatitude() {
        return originalCellLatitude;
    }

    public void setOriginalCellLatitude(int originalCellLatitude) {
        this.originalCellLatitude = originalCellLatitude;
    }

    public int getOriginalCellTime() {
        return originalCellTime;
    }

    public void setOriginalCellTime(int originalCellTime) {
        this.originalCellTime = originalCellTime;
    }
}
