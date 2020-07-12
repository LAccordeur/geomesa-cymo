package com.rogerguo.client;

import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.curve.CurveMeta;
import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.entity.SpatialRange;
import com.rogerguo.cymo.entity.TimeRange;
import com.rogerguo.cymo.hbase.RowKeyHelper;
import com.rogerguo.cymo.virtual.VirtualLayerGeoMesa;
import com.rogerguo.cymo.virtual.entity.NormalizedLocation;
import com.rogerguo.cymo.virtual.entity.NormalizedRange;
import com.rogerguo.cymo.virtual.helper.NormalizedDimensionHelper;
import com.rogerguo.cymo.virtual.helper.PartitionCurveStrategyHelper;
import com.rogerguo.cymo.virtual.normalization.NormalizedDimension;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

/**
 * @Description
 * @Date 2020/7/9 11:57
 * @Created by X1 Carbon
 */
public class CurveMetaHelper {

    public static void main(String[] args) {
        VirtualLayerGeoMesa virtualLayerGeoMesa = new VirtualLayerGeoMesa("127.0.0.1");

        CurveMeta curveMeta = new CurveMeta(CurveType.CURVE_X3Y3T8, 3, 3, 8, 0, 0, 0);

        SpatialRange longitudeRange = new SpatialRange(-73.807000, -73.797000);
        SpatialRange latitudeRange = new SpatialRange(40.670000,40.680000);
        TimeRange timeRange = new TimeRange(fromDateToTimestamp("2010-01-02 15:05:00"), fromDateToTimestamp("2010-01-31 15:25:00"));

        NormalizedRange normalizedLongitudeRange = new NormalizedRange(NormalizedDimensionHelper.normalizedLon(longitudeRange.getLowBound()), NormalizedDimensionHelper.normalizedLon(longitudeRange.getHighBound()));
        NormalizedRange normalizedLatitudeRange = new NormalizedRange(NormalizedDimensionHelper.normalizedLat(latitudeRange.getLowBound()), NormalizedDimensionHelper.normalizedLat(latitudeRange.getHighBound()));
        NormalizedRange normalizedTimeRange = new NormalizedRange(NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, timeRange.getLowBound()), NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, timeRange.getHighBound()));


        setCurveMetaForSubspace(curveMeta, new NormalizedLocation(normalizedLongitudeRange.getLowBound(), normalizedLatitudeRange.getLowBound(), normalizedTimeRange.getLowBound()));

    }

    public static void setCurveMetaForSubspace(CurveMeta curveMeta, NormalizedLocation location) {
        PartitionCurveStrategyHelper.insertCurveMetaForSubspace(curveMeta, location);
    }

    public static long fromDateToTimestamp(String dateString) {
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);
        long time = Date.from(LocalDateTime.parse(dateString, dateFormat).toInstant(ZoneOffset.UTC)).getTime();
        return time;
    }

}
