package com.rogerguo.test;

import com.rogerguo.client.cymo.QueryPattern;
import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.curve.CurveMeta;
import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.curve.ZCurve;
import com.rogerguo.cymo.entity.SpatialRange;
import com.rogerguo.cymo.entity.TimeRange;
import com.rogerguo.cymo.hbase.HBaseDriver;
import com.rogerguo.cymo.hbase.RowKeyHelper;
import com.rogerguo.cymo.hbase.RowKeyItem;
import com.rogerguo.cymo.virtual.entity.NormalizedLocation;
import com.rogerguo.cymo.virtual.entity.NormalizedRange;
import com.rogerguo.cymo.virtual.entity.PartitionLocation;
import com.rogerguo.cymo.virtual.entity.SubspaceLocation;
import com.rogerguo.cymo.virtual.helper.NormalizedDimensionHelper;
import com.rogerguo.cymo.virtual.helper.PartitionCurveStrategyHelper;
import com.rogerguo.cymo.virtual.helper.VirtualSpaceTransformationHelper;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @Description
 * @Date 2021/6/9 19:58
 * @Created by X1 Carbon
 */
public class CheckIndexingScheme {
    private static HBaseDriver hBaseDriver = new HBaseDriver("127.0.0.1");


    private static int SPATIAL_NORMALIZE_PRECISION = 21;

    public static void main(String[] args) {

        SpatialRange longitudeRange = new SpatialRange(-74.05, -73.75);
        SpatialRange latitudeRange = new SpatialRange(40.60, 40.90);
        TimeRange timeRange = new TimeRange(fromDateToTimestamp("2010-01-22 00:00:00"), fromDateToTimestamp("2010-01-29 23:59:59"));

        double predictionSpatialWidth = 0.02;
        int predictionTimeWidth = 12;  // 12 * 5min = 1 hour
        try {
            decideIndexingScheme(longitudeRange, latitudeRange, timeRange, predictionSpatialWidth, predictionTimeWidth);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static long fromDateToTimestamp(String dateString) {
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);
        long time = Date.from(LocalDateTime.parse(dateString, dateFormat).toInstant(ZoneOffset.UTC)).getTime();
        return time;
    }

    /***
     *
     * @param longitudeRange
     * @param latitudeRange
     * @param timeRange
     * @param predictionSpatialWidth
     * @param predictionTimeWidth unit is hour
     * @throws IOException
     */
    public static void decideIndexingScheme(SpatialRange longitudeRange, SpatialRange latitudeRange, TimeRange timeRange, double predictionSpatialWidth, int predictionTimeWidth) throws IOException {

        // normalize
        NormalizedRange normalizedLongitudeRange;
        NormalizedRange normalizedLatitudeRange;
        NormalizedRange normalizedTimeRange;

        if (VirtualLayerConfiguration.IS_NORMALIZED) {
            normalizedLongitudeRange = new NormalizedRange((int) longitudeRange.getLowBound(), (int) longitudeRange.getHighBound());
            normalizedLatitudeRange = new NormalizedRange((int) latitudeRange.getLowBound(), (int) latitudeRange.getHighBound());
            normalizedTimeRange = new NormalizedRange((int) timeRange.getLowBound(), (int) timeRange.getHighBound());
        } else {
            normalizedLongitudeRange = new NormalizedRange(NormalizedDimensionHelper.normalizedLon(longitudeRange.getLowBound()), NormalizedDimensionHelper.normalizedLon(longitudeRange.getHighBound()));
            normalizedLatitudeRange = new NormalizedRange(NormalizedDimensionHelper.normalizedLat(latitudeRange.getLowBound()), NormalizedDimensionHelper.normalizedLat(latitudeRange.getHighBound()));
            normalizedTimeRange = new NormalizedRange(NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, timeRange.getLowBound()), NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, timeRange.getHighBound()));
        }


        List<SubspaceLocation> subspaceLocationList = parseRangeOnVirtualLayer(normalizedLongitudeRange, normalizedLatitudeRange, normalizedTimeRange, predictionSpatialWidth, predictionTimeWidth);


        int total = 0;
        int matched = 0;
        int timeCountReal = 0;
        int spaceCountReal = 0;
        int timeCountPredicted = 0;
        int spaceCountPredicted = 0;
        ZCurve zCurve = new ZCurve();
        for (SubspaceLocation subspace : subspaceLocationList) {
//            CurveMeta curveMeta = PartitionCurveStrategyHelper.getCurveMetaByNormalizedLocation(new NormalizedLocation(subspace.getOriginalSubspaceLongitude(), subspace.getOriginalSubspaceLatitude(), subspace.getOriginalNormalizedTime()));
//            System.out.println(subspace + ":" +curveMeta);
            total++;
            NormalizedLocation normalizedLocation = new NormalizedLocation(subspace.getOriginalSubspaceLongitude(), subspace.getOriginalSubspaceLatitude(), subspace.getOriginalNormalizedTime());
            PartitionLocation partitionLocation = new PartitionLocation(normalizedLocation);

            int subspaceLongitude = normalizedLocation.getX() / VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY;
            int subspaceLatitude = normalizedLocation.getY() / VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY;
            long subspaceID = zCurve.getCurveValue(subspaceLongitude, subspaceLatitude);

            try {
                String curveMetaTableReal = "geomesa_virtual_space_metadata_table_nyc_synthetic_multiple_ratio-12-9";
                String curveMetaTablePredicted = "geomesa_virtual_space_metadata_table_nyc_synthetic_multiple_ratio-12-9_predicted";
                RowKeyItem rowKeyItem = RowKeyHelper.generateCurveMetaTableRowKey(partitionLocation.getPartitionID(), subspaceID);
                Result resultReal = hBaseDriver.get(curveMetaTableReal, rowKeyItem.getBytesRowKey());
                CurveType curveTypeReal = null;
                CurveType curveTypePredicted = null;
                if (resultReal.getRow() != null) {
                    List<Cell> cellList = resultReal.listCells();
                    if (cellList != null && cellList.size() != 0) {
                        Cell cell = cellList.get(0);
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        String[] valueItems = value.split(",");
                        curveTypeReal = CurveType.valueOf(valueItems[0]);
                    }
                }

                Result resultPredicted = hBaseDriver.get(curveMetaTablePredicted, rowKeyItem.getBytesRowKey());
                if (resultPredicted.getRow() != null) {
                    List<Cell> cellList = resultPredicted.listCells();
                    if (cellList != null && cellList.size() != 0) {
                        Cell cell = cellList.get(0);
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        String[] valueItems = value.split(",");
                        curveTypePredicted = CurveType.valueOf(valueItems[0]);
                    }
                }
                System.out.println(subspace);
                System.out.println("real: " + curveTypeReal);
                System.out.println("predicted: " + curveTypePredicted);
                System.out.println();
                if (curveTypeReal == curveTypePredicted) {
                    matched++;
                }
                if (curveTypeReal == CurveType.CURVE_TIME) {
                    timeCountReal++;
                } else {
                    spaceCountReal++;
                }
                if (curveTypePredicted == CurveType.CURVE_TIME) {
                    timeCountPredicted++;
                } else {
                    spaceCountPredicted++;
                }

            }
            catch(IOException e){

                e.printStackTrace();
            }
        }

        System.out.println("Match Ratio: " + (1.0 * matched / total));
        System.out.println("timeCountReal: " + timeCountReal);
        System.out.println("spaceCountReal: " + spaceCountReal);
        System.out.println("timeCountPredicted: " + timeCountPredicted);
        System.out.println("spaceCountPredicted: " + spaceCountPredicted);

    }

    private static List<SubspaceLocation> parseRangeOnVirtualLayer(NormalizedRange normalizedLongitudeRange, NormalizedRange normalizedLatitudeRange, NormalizedRange normalizedTimeRange, double predictionSpatialWidth, int predictionTimeWidth) throws IOException {
        // 1. normalize
        NormalizedLocation leftDownLocation = new NormalizedLocation(normalizedLongitudeRange.getLowBound(), normalizedLatitudeRange.getLowBound(), normalizedTimeRange.getLowBound());
        NormalizedLocation rightUpLocation = new NormalizedLocation(normalizedLongitudeRange.getHighBound(), normalizedLatitudeRange.getHighBound(), normalizedTimeRange.getHighBound());

        // 2. transform to virtual layer and generate row key list
        // left down subspace
        SubspaceLocation startSubspaceLocation = VirtualSpaceTransformationHelper.toSubspaceLocation(leftDownLocation);
        // right up subspace
        SubspaceLocation stopSubspaceLocation = VirtualSpaceTransformationHelper.toSubspaceLocation(rightUpLocation);


        List<SubspaceLocation> subspaceLocationList = new ArrayList<>();
        // 3. find the subspace list in the region
        for (int i = startSubspaceLocation.getPartitionID(); i <= stopSubspaceLocation.getPartitionID(); i++) {
            for (int j = startSubspaceLocation.getSubspaceLongitude(); j <= stopSubspaceLocation.getSubspaceLongitude(); j++) {
                for (int k = startSubspaceLocation.getSubspaceLatitude(); k <= stopSubspaceLocation.getSubspaceLatitude(); k++) {
                    SubspaceLocation subspaceLocation = VirtualSpaceTransformationHelper.toSubspaceLocation(VirtualSpaceTransformationHelper.fromSubspaceLocation(i, j, k));

                    subspaceLocationList.add(subspaceLocation);

                }
            }
        }


        return subspaceLocationList;
    }

    public static int getNormalizedLongitudeWidth(double spatialWidth) {
        int max = 180;
        int min = -180;

        long bins = 1L << SPATIAL_NORMALIZE_PRECISION;
        double normalizer = bins / (max - min);

        return (int) Math.floor(normalizer * spatialWidth);
    }

    public static int getNormalizedLatitudeWidth(double spatialWidth) {
        int max = 90;
        int min = -90;

        long bins = 1L << SPATIAL_NORMALIZE_PRECISION;
        double normalizer = bins / (max - min);

        return (int) Math.floor(normalizer * spatialWidth);
    }

}
