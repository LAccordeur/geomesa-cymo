package com.rogerguo.client.cymo;

import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.curve.CurveMeta;
import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.entity.SpatialRange;
import com.rogerguo.cymo.entity.TimeRange;
import com.rogerguo.cymo.hbase.HBaseDriver;
import com.rogerguo.cymo.virtual.entity.NormalizedLocation;
import com.rogerguo.cymo.virtual.entity.NormalizedRange;
import com.rogerguo.cymo.virtual.entity.SubspaceLocation;
import com.rogerguo.cymo.virtual.helper.NormalizedDimensionHelper;
import com.rogerguo.cymo.virtual.helper.PartitionCurveStrategyHelper;
import com.rogerguo.cymo.virtual.helper.VirtualSpaceTransformationHelper;
import com.rogerguo.cymo.virtual.normalization.NormalizedDimension;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @Description for each subspace, choose suitalbe indexing scheme and write into curve(indexing) meta table
 *
 * 1. prediction model will write predicted result for each query pattern in the hbase table. Prediction model has its own
 * location unit and time interval unit, the combination is a 3d space
 *
 * table schema (table_name: query_pattern_predicted_result):
 * row key = space id + day offset, column family = workload1, workload2, ... (each workload has its own column family),
 * quafiler = hour offset, value is the predicated frequency
 *
 * 2. Given the valid spatial-temporal space, the decider will do the following:
 *
 * for each subspace,
 * a. get the predicted result and count the frequency for each workload will happen in this subspace
 * b. input the frequency of each pattern into cost model to get the indexing scheme
 * c. write index scheme to the indexing meta table
 *
 * @Date 2021/3/23 16:45
 * @Created by X1 Carbon
 */
public class IndexingSchemeDecider {

    private static HBaseDriver hBaseDriver = new HBaseDriver("127.0.0.1");

    private static String tableName = "frequency_real_test_month1_workload_multiple_predicted";

    private static int SPATIAL_NORMALIZE_PRECISION = 21;

    public static void main(String[] args) {
        try {
            hBaseDriver.createTable(PartitionCurveStrategyHelper.CURVE_META_TABLE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SpatialRange longitudeRange = new SpatialRange(-74.05, -73.75);
        SpatialRange latitudeRange = new SpatialRange(40.60, 40.90);
        TimeRange timeRange = new TimeRange(fromDateToTimestamp("2010-01-01 00:00:00"), fromDateToTimestamp("2010-01-31 23:59:59"));

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


        Map<SubspaceLocation, List<Integer>> frequencyResultMap = parseRangeOnVirtualLayer(normalizedLongitudeRange, normalizedLatitudeRange, normalizedTimeRange, predictionSpatialWidth, predictionTimeWidth);


        Set<SubspaceLocation> subspaceLocations = frequencyResultMap.keySet();
        for (SubspaceLocation subspace : subspaceLocations) {
            List<Integer> frequencyResult = frequencyResultMap.get(subspace);
            double sum = frequencyResult.get(0) + frequencyResult.get(1);
            double workload1Frequency = (sum == 0 ? 0 : 1.0 * frequencyResult.get(0) / sum);
            double workload2Frequency = (sum == 0 ? 0 : 1.0 * frequencyResult.get(1) / sum);


            List<QueryPattern> queryPatterns = new ArrayList<>();

            //set workload1 as time-preferred query, workload2 as space-preferred query
            /*QueryPattern queryPattern1 = new QueryPattern(32, 32, 1, workload1Frequency, workload1Frequency*0.1);
            queryPatterns.add(queryPattern1);
            QueryPattern queryPattern2 = new QueryPattern(2, 2, 24, workload2Frequency, workload2Frequency*10);
            queryPatterns.add(queryPattern2);
            CurveMeta indexingScheme = EvaluateIndexingScheme.evaluateIndexing(queryPatterns);*/
            CurveMeta indexingScheme = null;
            if (workload1Frequency > workload2Frequency) {
                indexingScheme = new CurveMeta(CurveType.CURVE_TIME);
            } else {
                indexingScheme = new CurveMeta(CurveType.CURVE_SPACE);
            }

            System.out.println(subspace + ": " + indexingScheme);
            PartitionCurveStrategyHelper.insertCurveMetaForSubspace(indexingScheme, new NormalizedLocation(subspace.getOriginalSubspaceLongitude(), subspace.getOriginalSubspaceLatitude(), subspace.getOriginalNormalizedTime()));

        }

    }

    private static Map<SubspaceLocation, List<Integer>> parseRangeOnVirtualLayer(NormalizedRange normalizedLongitudeRange, NormalizedRange normalizedLatitudeRange, NormalizedRange normalizedTimeRange, double predictionSpatialWidth, int predictionTimeWidth) throws IOException {
        // 1. normalize
        NormalizedLocation leftDownLocation = new NormalizedLocation(normalizedLongitudeRange.getLowBound(), normalizedLatitudeRange.getLowBound(), normalizedTimeRange.getLowBound());
        NormalizedLocation rightUpLocation = new NormalizedLocation(normalizedLongitudeRange.getHighBound(), normalizedLatitudeRange.getHighBound(), normalizedTimeRange.getHighBound());

        // 2. transform to virtual layer and generate row key list
        // left down subspace
        SubspaceLocation startSubspaceLocation = VirtualSpaceTransformationHelper.toSubspaceLocation(leftDownLocation);
        // right up subspace
        SubspaceLocation stopSubspaceLocation = VirtualSpaceTransformationHelper.toSubspaceLocation(rightUpLocation);


        Map<SubspaceLocation, List<Integer>> patternFrequenciesInSubspaces = new HashMap<>();
        // 3. find the subspace list in the region
        for (int i = startSubspaceLocation.getPartitionID(); i <= stopSubspaceLocation.getPartitionID(); i++) {
            for (int j = startSubspaceLocation.getSubspaceLongitude(); j <= stopSubspaceLocation.getSubspaceLongitude(); j++) {
                for (int k = startSubspaceLocation.getSubspaceLatitude(); k <= stopSubspaceLocation.getSubspaceLatitude(); k++) {
                    SubspaceLocation subspaceLocation = VirtualSpaceTransformationHelper.toSubspaceLocation(VirtualSpaceTransformationHelper.fromSubspaceLocation(i, j, k));
                    int timeOffset = subspaceLocation.getOriginalNormalizedTime();
                    int longitudeOffset = subspaceLocation.getOriginalSubspaceLongitude();
                    int latitudeOffset = subspaceLocation.getOriginalSubspaceLatitude();

                    int normalizedLongitudeWidth = getNormalizedLongitudeWidth(predictionSpatialWidth);
                    int normalizedLatitudeWidth = getNormalizedLatitudeWidth(predictionSpatialWidth);
                    // get the mapped space in prediction
                    int longitudeNumMin = longitudeOffset / normalizedLongitudeWidth;
                    int latitudeNumMin = latitudeOffset / normalizedLatitudeWidth;
                    int longitudeNumMax = (longitudeOffset + VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY) / normalizedLongitudeWidth;
                    int latitudeNumMax = (latitudeOffset + VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY) / normalizedLatitudeWidth;
                    int timeNumMin = timeOffset / predictionTimeWidth;
                    int timeNumMax = (timeOffset + subspaceLocation.getNormalizedPartitionLength()) / predictionTimeWidth;

                    List<String> predicationTableRowKeyString = new ArrayList<>();
                    for (int m = timeNumMin; m < timeNumMax + 1; m++) {
                        for (int n = longitudeNumMin; n < longitudeNumMax + 1; n++) {
                            for (int l = latitudeNumMin; l < latitudeNumMax + 1; l++) {
                                int dayOffset = (int) Math.floor(m / 24);
                                String rowKey = new StringBuffer().append(n).append(",").append(l).append(",").append(dayOffset).toString();
                                predicationTableRowKeyString.add(rowKey);
                            }
                        }

                    }

                    //System.out.println(predicationTableRowKeyString.size());
                    // get the predicted frequency result from hbase for each workload
                    List<Get> getList = new ArrayList<>();
                    for (String rowKey : predicationTableRowKeyString) {
                        /*Get getWorkload1 = new Get(Bytes.toBytes(rowKey));
                        getWorkload1.addFamily(Bytes.toBytes("workload1"));
                        getList.add(getWorkload1);

                        Get getWorkload2 = new Get(Bytes.toBytes(rowKey));
                        getWorkload2.addFamily(Bytes.toBytes("workload2"));
                        getList.add(getWorkload2);*/

                        Get get = new Get(Bytes.toBytes(rowKey));
                        getList.add(get);
                    }
                    Result[] results = hBaseDriver.batchGet(tableName, getList);

                    int workload1Frequency = 0;
                    int workload2Frequency = 0;
                    for (Result result : results) {
                        if (result.getRow() != null) {
                            //String rowKey = Bytes.toString(result.getRow());
                            List<Cell> cellList = result.listCells();

                            for (Cell cell : cellList) {
                                String columnFamilyName = Bytes.toString(CellUtil.cloneFamily(cell));
                                int frequency = (int) Math.floor(Double.valueOf(Bytes.toString(CellUtil.cloneValue(cell))));
                                if (columnFamilyName.equals("workload_1_next_passenger")) {
                                    workload1Frequency += frequency;
                                } else if (columnFamilyName.equals("workload_2_heatmap_multiple")) {
                                    workload2Frequency += frequency;
                                    if (workload2Frequency > 0) {
                                        System.out.println(workload2Frequency);
                                    }
                                }
                            }
                        }
                    }

                    List<Integer> frequencyList = new ArrayList<>();
                    frequencyList.add(workload1Frequency);
                    frequencyList.add(workload2Frequency);
                    patternFrequenciesInSubspaces.put(subspaceLocation, frequencyList);

                }
            }
        }


        return patternFrequenciesInSubspaces;
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
