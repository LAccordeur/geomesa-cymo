package com.rogerguo.cymo.virtual.entity;


import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.curve.CurveMeta;
import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.virtual.helper.PartitionCurveStrategyHelper;

/**
 * @Description
 * @Author GUO Yang
 * @Date 2019-12-18 4:03 PM
 */
public class SubspaceLocation extends PartitionLocation {

    private int subspaceLongitude;  // normalized value / partition longitude granularity

    private int subspaceLatitude;

    private int originalSubspaceLongitude;

    private int originalSubspaceLatitude;

    private CurveType curveType;

    private CurveMeta curveMeta;

    public SubspaceLocation(NormalizedLocation normalizedLocation) {
        super(normalizedLocation);
        this.originalSubspaceLongitude = normalizedLocation.getX();
        this.subspaceLongitude = normalizedLocation.getX() / VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY;

        this.originalSubspaceLatitude = normalizedLocation.getY();
        this.subspaceLatitude = normalizedLocation.getY() / VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY;

        this.curveType = PartitionCurveStrategyHelper.getCurveTypeByNormalizedLocation(normalizedLocation);
        this.curveMeta = PartitionCurveStrategyHelper.getCurveMetaByNormalizedLocation(normalizedLocation);
    }

    @Override
    public String toString() {
        return super.toString() + "\nSubspaceLocation{" +
                "subspaceLongitude=" + subspaceLongitude +
                ", subspaceLatitude=" + subspaceLatitude +
                ", originalSubspaceLongitude=" + originalSubspaceLongitude +
                ", originalSubspaceLatitude=" + originalSubspaceLatitude +
                ", curveType=" + curveType +
                '}';
    }

    public int getSubspaceLongitude() {
        return subspaceLongitude;
    }

    public void setSubspaceLongitude(int subspaceLongitude) {
        this.subspaceLongitude = subspaceLongitude;
    }

    public int getSubspaceLatitude() {
        return subspaceLatitude;
    }

    public void setSubspaceLatitude(int subspaceLatitude) {
        this.subspaceLatitude = subspaceLatitude;
    }

    public int getOriginalSubspaceLongitude() {
        return originalSubspaceLongitude;
    }

    public void setOriginalSubspaceLongitude(int originalSubspaceLongitude) {
        this.originalSubspaceLongitude = originalSubspaceLongitude;
    }

    public int getOriginalSubspaceLatitude() {
        return originalSubspaceLatitude;
    }

    public void setOriginalSubspaceLatitude(int originalSubspaceLatitude) {
        this.originalSubspaceLatitude = originalSubspaceLatitude;
    }

    public CurveMeta getCurveMeta() {
        return curveMeta;
    }

    public void setCurveMeta(CurveMeta curveMeta) {
        this.curveMeta = curveMeta;
    }

    public CurveType getCurveType() {
        return curveType;
    }

    public void setCurveType(CurveType curveType) {
        this.curveType = curveType;
    }
}
