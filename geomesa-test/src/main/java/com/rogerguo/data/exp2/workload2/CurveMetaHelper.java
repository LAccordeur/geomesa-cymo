package com.rogerguo.data.exp2.workload2;

import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.curve.CurveMeta;
import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.entity.SpatialRange;
import com.rogerguo.cymo.entity.TimeRange;
import com.rogerguo.cymo.virtual.VirtualLayerGeoMesa;
import com.rogerguo.cymo.virtual.entity.NormalizedLocation;
import com.rogerguo.cymo.virtual.entity.NormalizedRange;
import com.rogerguo.cymo.virtual.helper.NormalizedDimensionHelper;
import com.rogerguo.cymo.virtual.helper.PartitionCurveStrategyHelper;

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

        // for 0.001
        CurveMeta curveMeta1 = new CurveMeta(CurveType.CURVE_X1Y1T8, 1, 1, 8, 0, 0, 0);
        SpatialRange longitudeRange1 = new SpatialRange(-73.977000, -73.967000);
        SpatialRange latitudeRange1 = new SpatialRange(40.725000,40.735000);
        TimeRange timeRange1 = new TimeRange(fromDateToTimestamp("2010-01-01 01:05:00"), fromDateToTimestamp("2010-01-31 15:25:00"));
        NormalizedRange normalizedLongitudeRange1 = new NormalizedRange(NormalizedDimensionHelper.normalizedLon(longitudeRange1.getLowBound()), NormalizedDimensionHelper.normalizedLon(longitudeRange1.getHighBound()));
        NormalizedRange normalizedLatitudeRange1 = new NormalizedRange(NormalizedDimensionHelper.normalizedLat(latitudeRange1.getLowBound()), NormalizedDimensionHelper.normalizedLat(latitudeRange1.getHighBound()));
        NormalizedRange normalizedTimeRange1 = new NormalizedRange(NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, timeRange1.getLowBound()), NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, timeRange1.getHighBound()));

        for (int i = normalizedTimeRange1.getLowBound(); i < normalizedTimeRange1.getHighBound(); i++) {

            setCurveMetaForSubspace(curveMeta1, new NormalizedLocation(normalizedLongitudeRange1.getLowBound(), normalizedLatitudeRange1.getLowBound(), i));
        }

        // for 0.01
        CurveMeta curveMeta = new CurveMeta(CurveType.CURVE_X3Y3T8, 3, 3, 8, 0, 0, 0);
        SpatialRange longitudeRange = new SpatialRange(-73.997000, -73.987000);
        SpatialRange latitudeRange = new SpatialRange(40.745000,40.755000);
        TimeRange timeRange = new TimeRange(fromDateToTimestamp("2010-01-01 01:05:00"), fromDateToTimestamp("2010-01-31 15:25:00"));

        NormalizedRange normalizedLongitudeRange = new NormalizedRange(NormalizedDimensionHelper.normalizedLon(longitudeRange.getLowBound()), NormalizedDimensionHelper.normalizedLon(longitudeRange.getHighBound()));
        NormalizedRange normalizedLatitudeRange = new NormalizedRange(NormalizedDimensionHelper.normalizedLat(latitudeRange.getLowBound()), NormalizedDimensionHelper.normalizedLat(latitudeRange.getHighBound()));
        NormalizedRange normalizedTimeRange = new NormalizedRange(NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, timeRange.getLowBound()), NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, timeRange.getHighBound()));


        for (int i = normalizedTimeRange.getLowBound(); i < normalizedTimeRange.getHighBound(); i++) {
            setCurveMetaForSubspace(curveMeta, new NormalizedLocation(normalizedLongitudeRange.getLowBound(), normalizedLatitudeRange.getLowBound(), i));
        }
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
