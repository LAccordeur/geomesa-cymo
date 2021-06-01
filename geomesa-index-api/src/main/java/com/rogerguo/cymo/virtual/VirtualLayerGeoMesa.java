package com.rogerguo.cymo.virtual;

import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.curve.CurveMeta;
import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.entity.SpatialRange;
import com.rogerguo.cymo.entity.TimeRange;
import com.rogerguo.cymo.hbase.HBaseDriver;
import com.rogerguo.cymo.hbase.RowKeyHelper;
import com.rogerguo.cymo.hbase.RowKeyItem;
import com.rogerguo.cymo.virtual.entity.*;
import com.rogerguo.cymo.virtual.helper.CurveTransformationHelper;
import com.rogerguo.cymo.virtual.helper.NormalizedDimensionHelper;
import com.rogerguo.cymo.virtual.helper.PartitionCurveStrategyHelper;
import com.rogerguo.cymo.virtual.helper.VirtualSpaceTransformationHelper;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @Description
 * @Date 6/5/20 4:07 PM
 * @Created by rogerguo
 */
public class VirtualLayerGeoMesa {


    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // TODO #cache# 并发环境

    private static Map<String, List<Long>> cachedCellIDMap = new HashMap<>();

    public static final String VIRTUAL_LAYER_INFO_TABLE = VirtualLayerConfiguration.VIRTUAL_LAYER_INFO_TABLE;

    private static final String COLUMN_FAMILY_NAME = "cf";

    private HBaseDriver hBaseDriver;

    private static Map<String, List<Long>> cachedBitmapMap = new HashMap<>();

    private static Set<String> fetchedBitmapRowKeySet = new HashSet<>();

    public VirtualLayerGeoMesa(String zookeeperUrl) {
        this.hBaseDriver = new HBaseDriver(zookeeperUrl);
        initialize();
    }

    public void initialize() {

        try {

            List<String> familyNameList = new ArrayList<>();
            familyNameList.add(COLUMN_FAMILY_NAME);
            familyNameList.add("agg");
            hBaseDriver.createTable(VIRTUAL_LAYER_INFO_TABLE, familyNameList);
            hBaseDriver.createTable(PartitionCurveStrategyHelper.CURVE_META_TABLE);


        } catch (IOException e) {
            logger.error("initialize failed");
            e.printStackTrace();
        }

    }

    static {

        String cachedCellIDMapKey1 = CurveType.Z_CURVE.toString() + "_" + VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH;
        List<Long> cellIDList1 = new ArrayList<>();
        for (int n = 0; n < VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH; n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {
                    cellIDList1.add(CurveTransformationHelper.generate3D(new CurveMeta(CurveType.Z_CURVE), i, j, n));
                }
            }
        }
        Collections.sort(cellIDList1);
        cachedCellIDMap.put(cachedCellIDMapKey1, cellIDList1);

        String cachedCellIDMapKey2 = CurveType.Z_CURVE.toString() + "_" + VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH;
        List<Long> cellIDList2 = new ArrayList<>();
        for (int n = 0; n < VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH; n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {
                    cellIDList2.add(CurveTransformationHelper.generate3D(new CurveMeta(CurveType.Z_CURVE), i, j, n));
                }
            }
        }
        Collections.sort(cellIDList2);
        cachedCellIDMap.put(cachedCellIDMapKey2, cellIDList2);

        String cachedCellIDMapKey3 = CurveType.Z_CURVE_TXY.toString() + "_" + VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH;
        List<Long> cellIDList3 = new ArrayList<>();
        for (int n = 0; n < VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH; n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {
                    cellIDList3.add(CurveTransformationHelper.generate3D(new CurveMeta(CurveType.Z_CURVE_TXY), i, j, n));
                }
            }
        }
        Collections.sort(cellIDList3);
        cachedCellIDMap.put(cachedCellIDMapKey3, cellIDList3);

        String cachedCellIDMapKey4 = CurveType.Z_CURVE_TXY.toString() + "_" + VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH;
        List<Long> cellIDList4 = new ArrayList<>();
        for (int n = 0; n < VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH; n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {
                    cellIDList4.add(CurveTransformationHelper.generate3D(new CurveMeta(CurveType.Z_CURVE_TXY), i, j, n));
                }
            }
        }
        Collections.sort(cellIDList4);
        cachedCellIDMap.put(cachedCellIDMapKey4, cellIDList4);

        String cachedCellIDMapKey5 = CurveType.CURVE_T1X7Y7.toString() + "_" + VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH;
        List<Long> cellIDList5 = new ArrayList<>();
        for (int n = 0; n < VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH; n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {
                    cellIDList5.add(CurveTransformationHelper.generate3D(new CurveMeta(CurveType.CURVE_T1X7Y7), i, j, n));
                }
            }
        }
        Collections.sort(cellIDList5);
        cachedCellIDMap.put(cachedCellIDMapKey5, cellIDList5);

        String cachedCellIDMapKey6 = CurveType.CURVE_T1X7Y7.toString() + "_" + VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH;
        List<Long> cellIDList6 = new ArrayList<>();
        for (int n = 0; n < VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH; n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {
                    cellIDList6.add(CurveTransformationHelper.generate3D(new CurveMeta(CurveType.CURVE_T1X7Y7), i, j, n));
                }
            }
        }
        Collections.sort(cellIDList6);
        cachedCellIDMap.put(cachedCellIDMapKey6, cellIDList6);

        String cachedCellIDMapKey7 = CurveType.CURVE_X3Y3T8.toString() + "_" + VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH;
        List<Long> cellIDList7 = new ArrayList<>();
        for (int n = 0; n < VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH; n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {
                    cellIDList7.add(CurveTransformationHelper.generate3D(new CurveMeta(CurveType.CURVE_X3Y3T8), i, j, n));
                }
            }
        }
        Collections.sort(cellIDList7);
        cachedCellIDMap.put(cachedCellIDMapKey7, cellIDList7);

        String cachedCellIDMapKey8 = CurveType.CURVE_X3Y3T8.toString() + "_" + VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH;
        List<Long> cellIDList8 = new ArrayList<>();
        for (int n = 0; n < VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH; n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {
                    cellIDList8.add(CurveTransformationHelper.generate3D(new CurveMeta(CurveType.CURVE_X3Y3T8), i, j, n));
                }
            }
        }
        Collections.sort(cellIDList8);
        cachedCellIDMap.put(cachedCellIDMapKey8, cellIDList8);

        String cachedCellIDMapKey9 = CurveType.HILBERT_CURVE.toString() + "_" + VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH;
        List<Long> cellIDList9 = new ArrayList<>();
        for (int n = 0; n < VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH; n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {
                    cellIDList9.add(CurveTransformationHelper.generate3D(new CurveMeta(CurveType.HILBERT_CURVE), i, j, n));
                }
            }
        }
        Collections.sort(cellIDList9);
        cachedCellIDMap.put(cachedCellIDMapKey9, cellIDList9);

        String cachedCellIDMapKey10 = CurveType.HILBERT_CURVE.toString() + "_" + VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH;
        List<Long> cellIDList10 = new ArrayList<>();
        for (int n = 0; n < VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH; n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {
                    cellIDList10.add(CurveTransformationHelper.generate3D(new CurveMeta(CurveType.HILBERT_CURVE), i, j, n));
                }
            }
        }
        Collections.sort(cellIDList10);
        cachedCellIDMap.put(cachedCellIDMapKey10, cellIDList10);


        String cachedCellIDMapKey11 = CurveType.CURVE_T1X4Y4.toString() + "_" + VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH;
        List<Long> cellIDList11 = new ArrayList<>();
        for (int n = 0; n < VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH; n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {
                    cellIDList11.add(CurveTransformationHelper.generate3D(new CurveMeta(CurveType.CURVE_T1X4Y4), i, j, n));
                }
            }
        }
        Collections.sort(cellIDList11);
        cachedCellIDMap.put(cachedCellIDMapKey11, cellIDList11);

        String cachedCellIDMapKey12 = CurveType.CURVE_T1X4Y4.toString() + "_" + VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH;
        List<Long> cellIDList12 = new ArrayList<>();
        for (int n = 0; n < VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH; n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {
                    cellIDList12.add(CurveTransformationHelper.generate3D(new CurveMeta(CurveType.CURVE_T1X4Y4), i, j, n));
                }
            }
        }
        Collections.sort(cellIDList12);
        cachedCellIDMap.put(cachedCellIDMapKey12, cellIDList12);

    }

    static {
        // bitmap cache initialization
        SpatialRange longitudeRange = new SpatialRange(-74.2, -73.5);
        SpatialRange latitudeRange = new SpatialRange(40.40, 41.10);
        TimeRange timeRange = new TimeRange(fromDateToTimestamp("2010-01-01 00:00:00"), fromDateToTimestamp("2010-01-31 23:00:00"));

        NormalizedRange normalizedLongitudeRange = new NormalizedRange(NormalizedDimensionHelper.normalizedLon(longitudeRange.getLowBound()), NormalizedDimensionHelper.normalizedLon(longitudeRange.getHighBound()));
        NormalizedRange normalizedLatitudeRange = new NormalizedRange(NormalizedDimensionHelper.normalizedLat(latitudeRange.getLowBound()), NormalizedDimensionHelper.normalizedLat(latitudeRange.getHighBound()));
        NormalizedRange normalizedTimeRange = new NormalizedRange(NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, timeRange.getLowBound()), NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, timeRange.getHighBound()));

        // 1. normalize
        NormalizedLocation leftDownLocation = new NormalizedLocation(normalizedLongitudeRange.getLowBound(), normalizedLatitudeRange.getLowBound(), normalizedTimeRange.getLowBound());
        NormalizedLocation rightUpLocation = new NormalizedLocation(normalizedLongitudeRange.getHighBound(), normalizedLatitudeRange.getHighBound(), normalizedTimeRange.getHighBound());

        // 2. transform to virtual layer and generate row key list
        List<RowKeyItem> bitmapTableRowKeyList = RowKeyHelper.generateBitmapTableRowKeyList(leftDownLocation, rightUpLocation);

        List<Get> basicGetList = new ArrayList<>();
        for (RowKeyItem key : bitmapTableRowKeyList) {
            if (!fetchedBitmapRowKeySet.contains(key.getStringRowKey())) {
                fetchedBitmapRowKeySet.add(key.getStringRowKey());
                Get basicGet = new Get(Bytes.toBytes(key.getStringRowKey()));
                basicGet.addFamily(Bytes.toBytes("agg"));
                basicGetList.add(basicGet);
            }

        }

        HBaseDriver staticHBaseDriver = new HBaseDriver("127.0.0.1");
        Result[] results = null;
        try {
            results = staticHBaseDriver.batchGet(VIRTUAL_LAYER_INFO_TABLE, basicGetList);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (results != null && results.length > 0) {
            // count of one cell in the subspace
            for (Result result : results) {
                List<Cell> cellList = result.listCells();
                if (cellList != null && cellList.size() == 2) {


                    byte[] rowKey = result.getRow();
                    Map<String, String> parsedRowKey = RowKeyHelper.fromIndexStringRowKey(Bytes.toString(rowKey));

                    Cell basicCell = cellList.get(0);
                    byte[] basicBitmapValue = CellUtil.cloneValue(basicCell);
                    String basicBitmapString = Bytes.toString(basicBitmapValue);
                    List<Long> basicBitmapResult = fromString2ListValues(basicBitmapString);

                    Cell extraCell = cellList.get(1);
                    byte[] extraBitmapValue = CellUtil.cloneValue(extraCell);
                    String extraBitmapString = Bytes.toString(extraBitmapValue);
                    List<Long> extraBitmapResult = fromString2ListValues(extraBitmapString);

                    // cache start
                    String basicCacheKey = String.format("%s,%s,%s", parsedRowKey.get("partitionID"), parsedRowKey.get("subspaceID"), "basic"); // partition_id + subspace_id + type
                    String extraCacheKey = String.format("%s,%s,%s", parsedRowKey.get("partitionID"), parsedRowKey.get("subspaceID"), "extra");
                    cachedBitmapMap.put(basicCacheKey, basicBitmapResult);
                    cachedBitmapMap.put(extraCacheKey, extraBitmapResult);
                    // cache end
                }
            }
        }

        System.out.println("finish bitmap cache");
    }

    public static long fromDateToTimestamp(String dateString) {
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);
        long time = Date.from(LocalDateTime.parse(dateString, dateFormat).toInstant(ZoneOffset.UTC)).getTime();
        return time;
    }

    public List<SubScanRangePair> getRanges(SpatialRange longitudeRange, SpatialRange latitudeRange, TimeRange timeRange) throws IOException {
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

        // parse the query on the virtual layer to get the subspace list
        List<RowKeyItem> subspaceInfoList = parseQueryOnVirtualLayer(normalizedLongitudeRange, normalizedLatitudeRange, normalizedTimeRange);


        // parse and transform bitmap
        //Map<Integer, Map<Long, Map<String, List<Long>>>> bitmapResult = parseBitmapInVirtualLayer(indexRowKeyList);
        Map<Integer, Map<Long, Map<String, List<Long>>>> bitmapResult = parseAggregatedBitmapInVirtualLayer(subspaceInfoList);


        // for each subspace, generate optimized scan range
        List<SubScanRangePair> ranges = new ArrayList<>();
        for (RowKeyItem subspaceInfo : subspaceInfoList) {

            Map<String, List<Long>> subspaceBitmap = bitmapResult.get(subspaceInfo.getPartitionID()).get(subspaceInfo.getSubspaceID());

            ScanOptimizer scanOptimizer = optimizeScanRange(normalizedLongitudeRange, normalizedLatitudeRange, normalizedTimeRange, subspaceInfo, subspaceBitmap);
            int partitionID = scanOptimizer.getPartitionID();
            long subspaceID = scanOptimizer.getSubspaceID();
            List<SubScanItem> subScanItems = scanOptimizer.getSubScanItemList();
            for (SubScanItem subScanItem : subScanItems) {
                SubScanRange scanRange = subScanItem.getSubScanRange();
                // if lowbound == highbound, geomesa will throw an error, need to be verified TODO
                //if (scanRange.getLowBound() == scanRange.getHighBound()) {
                scanRange.setHighBound(scanRange.getHighBound() + 1);
                //}

                SubScanRangePair rangePair = new SubScanRangePair(partitionID, subspaceID, scanRange.getLowBound(), scanRange.getHighBound());

                ranges.add(rangePair);
            }
        }

        logger.info("[Virtual Layer] size of scan range to geomesa: " + ranges.size());
        //logger.info("[Virtual Layer] scan range to geomesa: " + ranges);
        return ranges;

    }

    private List<RowKeyItem> parseQueryOnVirtualLayer(NormalizedRange normalizedLongitudeRange, NormalizedRange normalizedLatitudeRange, NormalizedRange normalizedTimeRange) {
        // 1. normalize
        NormalizedLocation leftDownLocation = new NormalizedLocation(normalizedLongitudeRange.getLowBound(), normalizedLatitudeRange.getLowBound(), normalizedTimeRange.getLowBound());
        NormalizedLocation rightUpLocation = new NormalizedLocation(normalizedLongitudeRange.getHighBound(), normalizedLatitudeRange.getHighBound(), normalizedTimeRange.getHighBound());

        // 2. transform to virtual layer and generate row key list
        List<RowKeyItem> bitmapTableRowKeyList = RowKeyHelper.generateBitmapTableRowKeyList(leftDownLocation, rightUpLocation);
        return bitmapTableRowKeyList;
    }

    public Map<Integer, Map<Long, Map<String, List<Long>>>> parseAggregatedBitmapInVirtualLayer(List<RowKeyItem> bitmapTableRowKeyList) throws IOException {
        //  partitionID subspaceID;

        long start0 = System.currentTimeMillis();
        Map<Integer, Map<Long, Map<String, List<Long>>>> virtualLayerDataMap = new HashMap<>();

        List<Get> basicGetList = new ArrayList<>();
        for (RowKeyItem key : bitmapTableRowKeyList) {

            /*Get basicGet = new Get(Bytes.toBytes(key.getStringRowKey()));
            basicGet.addFamily(Bytes.toBytes("agg"));
            basicGetList.add(basicGet);*/

            // init result map
            int partitionID = ((SubspaceLocation) key.getBitmapPayload()).getPartitionID();
            long subSubspaceID = key.getSubspaceID();

            if (virtualLayerDataMap.get(partitionID) == null) {
                // check if this partition exists
                Map<Long, Map<String, List<Long>>> subSpaceMap = new HashMap<>();
                Map<String, List<Long>> bitmapMap = new HashMap<>();
                bitmapMap.put("basic", new ArrayList<>());
                bitmapMap.put("extra", new ArrayList<>());
                subSpaceMap.put(subSubspaceID, bitmapMap);
                virtualLayerDataMap.put(partitionID, subSpaceMap);
            } else {
                // check if this subspace exists
                Map<Long, Map<String, List<Long>>> subspaceMap = virtualLayerDataMap.get(partitionID);
                if (subspaceMap.get(subSubspaceID) == null) {
                    Map<String, List<Long>> bitmapMap = new HashMap<>();
                    bitmapMap.put("basic", new ArrayList<>());
                    bitmapMap.put("extra", new ArrayList<>());
                    subspaceMap.put(subSubspaceID, bitmapMap);
                }

            }

        }
        long stop0 = System.currentTimeMillis();
        //System.out.println("Init data struct takes " + (stop0 - start0));
        if (!VirtualLayerConfiguration.IS_WITH_META) {
            return virtualLayerDataMap;
        }

        // cache start
        for (RowKeyItem key : bitmapTableRowKeyList) {
            if (!fetchedBitmapRowKeySet.contains(key.getStringRowKey())) {
                fetchedBitmapRowKeySet.add(key.getStringRowKey());
                Get basicGet = new Get(Bytes.toBytes(key.getStringRowKey()));
                basicGet.addFamily(Bytes.toBytes("agg"));
                basicGetList.add(basicGet);
            } else {
                // get bitmap from cache
                int partitionID = ((SubspaceLocation) key.getBitmapPayload()).getPartitionID();
                long subSubspaceID = key.getSubspaceID();

                Map<String, String> parsedRowKey = RowKeyHelper.fromIndexStringRowKey(key.getStringRowKey());
                String basicCacheKey = String.format("%s,%s,%s", parsedRowKey.get("partitionID"), parsedRowKey.get("subspaceID"), "basic"); // partition_id + subspace_id + type
                String extraCacheKey = String.format("%s,%s,%s", parsedRowKey.get("partitionID"), parsedRowKey.get("subspaceID"), "extra");
                if (cachedBitmapMap.containsKey(basicCacheKey) && cachedBitmapMap.get(basicCacheKey) != null) {
                    virtualLayerDataMap.get(partitionID).get(subSubspaceID).put("basic", cachedBitmapMap.get(basicCacheKey));
                }
                if (cachedBitmapMap.containsKey(extraCacheKey) && cachedBitmapMap.get(extraCacheKey) != null) {
                    virtualLayerDataMap.get(partitionID).get(subSubspaceID).put("extra", cachedBitmapMap.get(extraCacheKey));
                }

            }

        }
        // cache end


        long start1 = System.currentTimeMillis();
        Result[] results = hBaseDriver.batchGet(VIRTUAL_LAYER_INFO_TABLE, basicGetList);
        long stop1 = System.currentTimeMillis();
        System.out.println("Batch get bitmap takes " + (stop1 - start1));

        long start2 = System.currentTimeMillis();
        if (results != null && results.length > 0) {
            // count of one cell in the subspace
            for (Result result : results) {
                List<Cell> cellList = result.listCells();
                if (cellList != null && cellList.size() == 2) {

                    //System.out.println("I am here");
                    // partition id + subspace id
                    byte[] rowKey = result.getRow();
                    Map<String, String> parsedRowKey = RowKeyHelper.fromIndexStringRowKey(Bytes.toString(rowKey));

                    Cell basicCell = cellList.get(0);
                    byte[] basicBitmapValue = CellUtil.cloneValue(basicCell);
                    String basicBitmapString = Bytes.toString(basicBitmapValue);
                    List<Long> basicBitmapResult = fromString2ListValues(basicBitmapString);
                    virtualLayerDataMap.get(Integer.valueOf(parsedRowKey.get("partitionID"))).get(Long.valueOf(parsedRowKey.get("subspaceID"))).put("basic", basicBitmapResult);


                    Cell extraCell = cellList.get(1);
                    byte[] extraBitmapValue = CellUtil.cloneValue(extraCell);
                    String extraBitmapString = Bytes.toString(extraBitmapValue);
                    List<Long> extraBitmapResult = fromString2ListValues(extraBitmapString);
                    virtualLayerDataMap.get(Integer.valueOf(parsedRowKey.get("partitionID"))).get(Long.valueOf(parsedRowKey.get("subspaceID"))).put("extra", extraBitmapResult);


                    // cache start
                    String basicCacheKey = String.format("%s,%s,%s", parsedRowKey.get("partitionID"), parsedRowKey.get("subspaceID"), "basic"); // partition_id + subspace_id + type
                    String extraCacheKey = String.format("%s,%s,%s", parsedRowKey.get("partitionID"), parsedRowKey.get("subspaceID"), "extra");
                    cachedBitmapMap.put(basicCacheKey, basicBitmapResult);
                    cachedBitmapMap.put(extraCacheKey, extraBitmapResult);
                    // cache end
                }
            }
        }
        long stop2 = System.currentTimeMillis();
        System.out.println("Read and parse result takes " + (stop2 - start2));


        return virtualLayerDataMap;

    }

    public static String fromListValue2String(List<Long> values) {
        if (values == null || values.size() == 0) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (long value : values) {
            stringBuilder.append(value).append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);

        return stringBuilder.toString();
    }

    public static List<Long> fromString2ListValues(String stringValue) {
        List<Long> values = new ArrayList<>();

        if (stringValue == null || stringValue == "") {
            return values;
        }

        String[] valuesString = stringValue.split(",");

        for (int i = 0; i < valuesString.length; i++) {
            values.add(Long.valueOf(valuesString[i]));
        }

        return values;
    }

    private ScanOptimizer optimizeScanRange(NormalizedRange longitudeRange, NormalizedRange latitudeRange, NormalizedRange timeRange, RowKeyItem subspaceInfo, Map<String, List<Long>> subspaceBitmap) {
        int partitionID = subspaceInfo.getPartitionID();
        long subspaceID = subspaceInfo.getSubspaceID();
        BoundingBox queryRangeBoundBox = new BoundingBox(longitudeRange, latitudeRange, timeRange);

        /*if (subspaceBitmap == null || subspaceBitmap.size() == 0) {
            SubspaceLocation subspaceLocation = VirtualSpaceTransformationHelper.toSubspaceLocation(VirtualSpaceTransformationHelper.fromPartitionIDAndSubspaceID(partitionID, subspaceID));

            CellLocation beginCellOfThisSubspace = VirtualSpaceTransformationHelper.getFirstCellLocationOfThisSubspace(subspaceLocation);
            BoundingBox validQueryRange = computeValidQueryRangeInThisSubspace(subspaceLocation, queryRangeBoundBox);
            validQueryRange.computeOffsetRangeInThisBoundingBox(beginCellOfThisSubspace);

            //logger.info(String.format("[Virtual Layer] valid query range: %s", validQueryRange));

            double fillRateOfValidRange = validQueryRange.computeFillRateOfValidRange();
            logger.info(String.format("[Virtual Layer] Fill Rate: %f in (Partition ID: %d, Subspace ID: %d, CurveMeta: %s)", fillRateOfValidRange, partitionID, subspaceID, subspaceLocation.getCurveMeta().toString()));

            if (fillRateOfValidRange <= 0.5) {
                if (!VirtualLayerConfiguration.IS_WITH_META) {
                    return optimizeByAggregationWithoutMeta(subspaceLocation, validQueryRange);
                } else {
                    return optimizeByAggregationWithMeta(subspaceLocation, validQueryRange, subspaceBitmap.get("basic"), subspaceBitmap.get("extra"));
                }
            } else if (fillRateOfValidRange < 0.99) {
                if (!VirtualLayerConfiguration.IS_WITH_META) {
                    return optimizeBySplitWithoutMeta(subspaceLocation, validQueryRange);
                } else {
                    return optimizeBySplitWithMeta(subspaceLocation, validQueryRange, subspaceBitmap.get("basic"), subspaceBitmap.get("extra"));
                }
            } else {
                ScanOptimizer scanOptimizer = new ScanOptimizer(subspaceLocation);
                return scanOptimizer;
            }

        } else {
            return null;
        }*/

        SubspaceLocation subspaceLocation = VirtualSpaceTransformationHelper.toSubspaceLocation(VirtualSpaceTransformationHelper.fromPartitionIDAndSubspaceID(partitionID, subspaceID));

        CellLocation beginCellOfThisSubspace = VirtualSpaceTransformationHelper.getFirstCellLocationOfThisSubspace(subspaceLocation);
        BoundingBox validQueryRange = computeValidQueryRangeInThisSubspace(subspaceLocation, queryRangeBoundBox);
        validQueryRange.computeOffsetRangeInThisBoundingBox(beginCellOfThisSubspace);

        //logger.info(String.format("[Virtual Layer] valid query range: %s", validQueryRange));

        double fillRateOfValidRange = validQueryRange.computeFillRateOfValidRange();
        logger.info(String.format("[Virtual Layer] Fill Rate: %f in (Partition ID: %d, Subspace ID: %d, CurveMeta: %s)", fillRateOfValidRange, partitionID, subspaceID, subspaceLocation.getCurveMeta().toString()));

        if (fillRateOfValidRange <= 0.5) {
            if (!VirtualLayerConfiguration.IS_WITH_META) {
                System.out.println("aggregation without bitmap");
                return optimizeByAggregationWithoutMeta(subspaceLocation, validQueryRange);
            } else {
                System.out.println("aggregation with bitmap");
                return optimizeByAggregationWithMeta(subspaceLocation, validQueryRange, subspaceBitmap.get("basic"), subspaceBitmap.get("extra"));
            }
        } else if (fillRateOfValidRange < 0.95) {
            if (!VirtualLayerConfiguration.IS_WITH_META) {
                return optimizeBySplitWithoutMeta(subspaceLocation, validQueryRange);
            } else {
                return optimizeBySplitWithMeta(subspaceLocation, validQueryRange, subspaceBitmap.get("basic"), subspaceBitmap.get("extra"));
            }
        } else {
            ScanOptimizer scanOptimizer = new ScanOptimizer(subspaceLocation);
            return scanOptimizer;
        }

    }

    private ScanOptimizer optimizeByAggregationWithMeta(SubspaceLocation subspaceLocation, BoundingBox validQueryRange, List<Long> basicBitmap, List<Long> extraBitmap) {
        logger.info(String.format("[Virtual Layer] optimize by aggregation"));

        List<Long> cellIDListInRange = new ArrayList<>();
        List<Long> cellIDListInSubspace = new ArrayList<>();
        List<Long> extraCellIDListInSubspace = new ArrayList<>();

        SubScanRange initLargeScanRangeInQueryRange = validQueryRange.computeInitScanRangeInValidRange();
        logger.info(String.format("[Virtual Layer] original scan range: [%d, %d], size: %d", initLargeScanRangeInQueryRange.getLowBound(), initLargeScanRangeInQueryRange.getHighBound(), (initLargeScanRangeInQueryRange.getHighBound()-initLargeScanRangeInQueryRange.getLowBound()+1)));

        int subSpaceCount = 0;
        int rangeCount = 0;
        List<List<Long>> temporaryIDInSubspaceList = new ArrayList<>();
        List<List<Long>> temporaryIDInRangeList = new ArrayList<>();
        int batchSize = 4096;
        List<Long> temporaryIDInSubspace = new ArrayList<>(batchSize);
        List<Long> temporaryIDInRange = new ArrayList<>(batchSize);
        long startTime = System.currentTimeMillis();

        String cachedCellIDMapKey = subspaceLocation.getCurveMeta().getCurveType().toString() + "_" + subspaceLocation.getNormalizedPartitionLength();
        if (cachedCellIDMap.get(cachedCellIDMapKey) == null) {
            List<Long> cellIDList = new ArrayList<>();
            for (int n = 0; n < subspaceLocation.getNormalizedPartitionLength(); n++) {

                for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                    for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {

                        Long id = CurveTransformationHelper.generate3D(subspaceLocation.getCurveMeta(), i, j, n);
                        cellIDList.add(id);
                        if (id >= initLargeScanRangeInQueryRange.getLowBound() && id <= initLargeScanRangeInQueryRange.getHighBound()) {

                            temporaryIDInSubspace.add(id);
                            if (validQueryRange.isInBoundingBox(i, j, n)) {
                                temporaryIDInRange.add(id);
                                rangeCount++;
                                if ((rangeCount % batchSize) == 0) {
                                    Collections.sort(temporaryIDInRange);
                                    temporaryIDInRangeList.add(temporaryIDInRange);
                                    temporaryIDInRange = new ArrayList<>(batchSize);
                                }
                            }
                            subSpaceCount++;
                            if ((subSpaceCount % batchSize) == 0) {
                                Collections.sort(temporaryIDInSubspace);
                                temporaryIDInSubspaceList.add(temporaryIDInSubspace);
                                temporaryIDInSubspace = new ArrayList<>(batchSize);
                            }

                        }

                    }
                }
            }

            Collections.sort(cellIDList);
            cachedCellIDMap.put(cachedCellIDMapKey, cellIDList);
        } else {
            List<Long> cacheCellIdList = cachedCellIDMap.get(cachedCellIDMapKey);

            int lowBoundIndex = Collections.binarySearch(cacheCellIdList, initLargeScanRangeInQueryRange.getLowBound());
            int highBoundIndex = Collections.binarySearch(cacheCellIdList, initLargeScanRangeInQueryRange.getHighBound());

            List<Long> cellIDSubList = cacheCellIdList.subList(lowBoundIndex, highBoundIndex+1);

            temporaryIDInSubspace.addAll(cellIDSubList);

            for (int i = validQueryRange.getLongitudeStart(); i <= validQueryRange.getLongitudeStop(); i++) {
                for (int j = validQueryRange.getLatitudeStart(); j <= validQueryRange.getLatitudeStop(); j++) {
                    for (int n = validQueryRange.getTimeStart(); n <= validQueryRange.getTimeStop(); n++) {
                        temporaryIDInRange.add(CurveTransformationHelper.generate3D(subspaceLocation.getCurveMeta(), i, j, n));
                    }
                }
            }

        }

        long stopTime = System.currentTimeMillis();
        logger.info("[Virtual Layer] check in-range takes " + (stopTime - startTime));

        Collections.sort(temporaryIDInRange);
        temporaryIDInRangeList.add(temporaryIDInRange);
        Collections.sort(temporaryIDInSubspace);
        temporaryIDInSubspaceList.add(temporaryIDInSubspace);

        // merge and sort id lists
        for (List<Long> idListInSubspace : temporaryIDInSubspaceList) {
            cellIDListInSubspace.addAll(idListInSubspace);
        }
        for (List<Long> idListInRange : temporaryIDInRangeList) {
            cellIDListInRange.addAll(idListInRange);
        }
        extraCellIDListInSubspace.addAll(cellIDListInSubspace);


        Collections.sort(cellIDListInRange);
        Collections.sort(cellIDListInSubspace);
        Collections.sort(extraCellIDListInSubspace);

        Collections.sort(basicBitmap);
        Collections.sort(extraBitmap);


        List<Long> finalizedCellIDListInRange = andResultForTwoSortedList(cellIDListInRange, basicBitmap);
        List<Long> finalizedCellIDListInSubspace = andResultForTwoSortedList(cellIDListInSubspace, basicBitmap);
        List<Long> finalizedExtraCellIDListInSubspace = andResultForTwoSortedList(extraCellIDListInSubspace, extraBitmap);

        //logger.info(String.format("[Virtual Layer] cells in range: %s", finalizedCellIDListInRange));
        logger.info(String.format("[Virtual Layer] size of cells in range: %d", finalizedCellIDListInRange.size()));
        //logger.info(String.format("[Virtual Layer] cells in subspace: %s", cellIDListInSubspace));
        logger.info(String.format("[Virtual Layer] size of cells in subspace: %d", finalizedCellIDListInSubspace.size()));
        //logger.info(String.format("[Virtual Layer] extra cells in subspace: %s", extraCellIDListInSubspace));
        logger.info(String.format("[Virtual Layer] size of extra cells in subspace: %d", finalizedExtraCellIDListInSubspace.size()));

        //ScanOptimizer scanOptimizer = new ScanOptimizer(subspaceLocation, cellIDListInSubspace, extraCellIDListInSubspace, cellIDListInRange);
        ScanOptimizer scanOptimizer = new ScanOptimizer(subspaceLocation, finalizedCellIDListInSubspace, finalizedExtraCellIDListInSubspace, finalizedCellIDListInRange);
        scanOptimizer.aggregate();

        return scanOptimizer;
    }

    public static List<Long> andResultForTwoSortedList(List<Long> values1, List<Long> values2) {


        List<Long> resultList = new ArrayList<>();

        if (values1 == null || values2 == null || values1.size() == 0 || values2.size() == 0) {
            return resultList;
        }

        if (values1.get(values1.size() - 1) < values2.get(0) || values1.get(0) > values2.get(values2.size() - 1)) {
            return resultList;
        }

        int i = 0;
        int j = 0;
        while (i < values1.size() && j < values2.size()) {
            if (values1.get(i) == values2.get(j).longValue()) {
                resultList.add(values1.get(i));
                i++;
                j++;
            } else if (values1.get(i) < values2.get(j)) {
                i++;
            } else {
                j++;
            }
        }


        return resultList;
    }

    private ScanOptimizer optimizeByAggregationWithoutMeta(SubspaceLocation subspaceLocation, BoundingBox validQueryRange) {
        logger.info(String.format("[Virtual Layer] optimize by aggregation"));

        List<Long> cellIDListInRange = new ArrayList<>();
        List<Long> cellIDListInSubspace = new ArrayList<>();
        List<Long> extraCellIDListInSubspace = new ArrayList<>();

        SubScanRange initLargeScanRangeInQueryRange = validQueryRange.computeInitScanRangeInValidRange();
        logger.info(String.format("[Virtual Layer] original scan range: [%d, %d], size: %d", initLargeScanRangeInQueryRange.getLowBound(), initLargeScanRangeInQueryRange.getHighBound(), (initLargeScanRangeInQueryRange.getHighBound()-initLargeScanRangeInQueryRange.getLowBound()+1)));

        int subSpaceCount = 0;
        int rangeCount = 0;
        List<List<Long>> temporaryIDInSubspaceList = new ArrayList<>();
        List<List<Long>> temporaryIDInRangeList = new ArrayList<>();
        int batchSize = 4096;
        List<Long> temporaryIDInSubspace = new ArrayList<>(batchSize);
        List<Long> temporaryIDInRange = new ArrayList<>(batchSize);
        long startTime = System.currentTimeMillis();

        String cachedCellIDMapKey = subspaceLocation.getCurveMeta().getCurveType().toString() + "_" + subspaceLocation.getNormalizedPartitionLength();

        if (cachedCellIDMap.get(cachedCellIDMapKey) == null ) {

            List<Long> cellIDList = new ArrayList<>();
            for (int n = 0; n < subspaceLocation.getNormalizedPartitionLength(); n++) {
                for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                    for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {

                        Long id = CurveTransformationHelper.generate3D(subspaceLocation.getCurveMeta(), i, j, n);
                        cellIDList.add(id);
                        if (id >= initLargeScanRangeInQueryRange.getLowBound() && id <= initLargeScanRangeInQueryRange.getHighBound()) {

                            temporaryIDInSubspace.add(id);
                            if (validQueryRange.isInBoundingBox(i, j, n)) {
                                temporaryIDInRange.add(id);
                                rangeCount++;
                                if ((rangeCount % batchSize) == 0) {
                                    Collections.sort(temporaryIDInRange);
                                    temporaryIDInRangeList.add(temporaryIDInRange);
                                    temporaryIDInRange = new ArrayList<>(batchSize);
                                }
                            }
                            subSpaceCount++;
                            if ((subSpaceCount % batchSize) == 0) {
                                Collections.sort(temporaryIDInSubspace);
                                temporaryIDInSubspaceList.add(temporaryIDInSubspace);
                                temporaryIDInSubspace = new ArrayList<>(batchSize);
                            }

                        }

                    }
                }
            }
            Collections.sort(cellIDList);
            cachedCellIDMap.put(cachedCellIDMapKey, cellIDList);
        } else {
            List<Long> cacheCellIdList = cachedCellIDMap.get(cachedCellIDMapKey);

            int lowBoundIndex = Collections.binarySearch(cacheCellIdList, initLargeScanRangeInQueryRange.getLowBound());
            int highBoundIndex = Collections.binarySearch(cacheCellIdList, initLargeScanRangeInQueryRange.getHighBound());

            List<Long> cellIDSubList = cacheCellIdList.subList(lowBoundIndex, highBoundIndex+1);

            temporaryIDInSubspace.addAll(cellIDSubList);

            for (int i = validQueryRange.getLongitudeStart(); i <= validQueryRange.getLongitudeStop(); i++) {
                for (int j = validQueryRange.getLatitudeStart(); j <= validQueryRange.getLatitudeStop(); j++) {
                    for (int n = validQueryRange.getTimeStart(); n <= validQueryRange.getTimeStop(); n++) {
                        temporaryIDInRange.add(CurveTransformationHelper.generate3D(subspaceLocation.getCurveMeta(), i, j, n));
                    }
                }
            }
        }
        long stopTime = System.currentTimeMillis();
        logger.info("[Virtual Layer] check in-range takes " + (stopTime - startTime));
        Collections.sort(temporaryIDInRange);
        temporaryIDInRangeList.add(temporaryIDInRange);
        Collections.sort(temporaryIDInSubspace);
        temporaryIDInSubspaceList.add(temporaryIDInSubspace);

        // merge and sort id lists
        for (List<Long> idListInSubspace : temporaryIDInSubspaceList) {
            cellIDListInSubspace.addAll(idListInSubspace);
        }
        for (List<Long> idListInRange : temporaryIDInRangeList) {
            cellIDListInRange.addAll(idListInRange);
        }


        Collections.sort(cellIDListInRange);
        Collections.sort(cellIDListInSubspace);
        Collections.sort(extraCellIDListInSubspace);
        //logger.info(String.format("[Virtual Layer] cells in range: %s", cellIDListInRange));
        logger.info(String.format("[Virtual Layer] size of cells in range: %d", cellIDListInRange.size()));
        logger.info(String.format("[Virtual Layer] size of cells in subspace: %d", cellIDListInSubspace.size()));
        logger.info(String.format("[Virtual Layer] size of extra cells in subspace: %d", extraCellIDListInSubspace.size()));

        ScanOptimizer scanOptimizer = new ScanOptimizer(subspaceLocation, cellIDListInSubspace, extraCellIDListInSubspace, cellIDListInRange);
        scanOptimizer.aggregate();

        return scanOptimizer;
    }

    private ScanOptimizer optimizeBySplitWithoutMeta(SubspaceLocation subspaceLocation, BoundingBox validQueryRange) {
        logger.info(String.format("[Virtual Layer] optimize by split"));

        // compute the init large scan by computing the low bound and high bound of valid query range
        SubScanRange initLargeScanRangeInQueryRange = validQueryRange.computeInitScanRangeInValidRange();
        logger.info(String.format("[Virtual Layer] original scan range: [%d, %d], size: %d", initLargeScanRangeInQueryRange.getLowBound(), initLargeScanRangeInQueryRange.getHighBound(), (initLargeScanRangeInQueryRange.getHighBound()-initLargeScanRangeInQueryRange.getLowBound()+1)));

        List<Long> cellIDListOutRange = new ArrayList<>();
        List<Long> extraCellIDListOutRange = new ArrayList<>();

        int count = 0;
        List<List<Long>> temporalIDOutRangeList = new ArrayList<>();
        int batchSize = 4096;

        List<Long> temporalIDOutRange = new ArrayList<>(batchSize);
        long startTime = System.currentTimeMillis();
        for (int n = 0; n < subspaceLocation.getNormalizedPartitionLength(); n++) {

            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) { // the cell contains data points

                    if (!validQueryRange.isInBoundingBox(i, j, n)) {
                        Long id = CurveTransformationHelper.generate3D(subspaceLocation.getCurveMeta(), i, j, n);
                        if (id >= initLargeScanRangeInQueryRange.getLowBound() && id <= initLargeScanRangeInQueryRange.getHighBound()) {
                            temporalIDOutRange.add(id);
                            count++;
                            if ((count % batchSize) == 0) {
                                Collections.sort(temporalIDOutRange);
                                temporalIDOutRangeList.add(temporalIDOutRange);
                                temporalIDOutRange = new ArrayList<>(batchSize);
                            }

                        }
                    }

                }
            }
        }
        long stopTime = System.currentTimeMillis();
        logger.info("[Virtual Layer] check out-of-range takes " + (stopTime - startTime));
        Collections.sort(temporalIDOutRange);
        temporalIDOutRangeList.add(temporalIDOutRange);

        // merge and sort id lists
        for (List<Long> idListOutRange : temporalIDOutRangeList) {
            cellIDListOutRange.addAll(idListOutRange);
        }

        Collections.sort(cellIDListOutRange);
        Collections.sort(extraCellIDListOutRange);
        logger.info(String.format("[Virtual Layer] size of cells out of range: %d", cellIDListOutRange.size()));
        logger.info(String.format("[Virtual Layer] size of extra cells out of range: %d", extraCellIDListOutRange.size()));

        ScanOptimizer scanOptimizer = new ScanOptimizer(subspaceLocation, initLargeScanRangeInQueryRange, cellIDListOutRange, extraCellIDListOutRange);
        scanOptimizer.split();

        return scanOptimizer;
    }

    private ScanOptimizer optimizeBySplitWithMeta(SubspaceLocation subspaceLocation, BoundingBox validQueryRange, List<Long> basicBitmap, List<Long> extraBitmap) {
        logger.info(String.format("[Virtual Layer] optimize by split"));

        // compute the init large scan by computing the low bound and high bound of valid query range
        SubScanRange initLargeScanRangeInQueryRange = validQueryRange.computeInitScanRangeInValidRange();
        logger.info(String.format("[Virtual Layer] original scan range: [%d, %d], size: %d", initLargeScanRangeInQueryRange.getLowBound(), initLargeScanRangeInQueryRange.getHighBound(), (initLargeScanRangeInQueryRange.getHighBound()-initLargeScanRangeInQueryRange.getLowBound()+1)));

        List<Long> cellIDListOutRange = new ArrayList<>();
        List<Long> extraCellIDListOutRange = new ArrayList<>();

        int count = 0;
        List<List<Long>> temporalIDOutRangeList = new ArrayList<>();
        int batchSize = 4096;

        List<Long> temporalIDOutRange = new ArrayList<>(batchSize);
        long startTime = System.currentTimeMillis();


        for (int n = 0; n < subspaceLocation.getNormalizedPartitionLength(); n++) {
            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) { // the cell contains data points

                    if (!validQueryRange.isInBoundingBox(i, j, n)) {
                        Long id = CurveTransformationHelper.generate3D(subspaceLocation.getCurveMeta(), i, j, n);
                        if (id >= initLargeScanRangeInQueryRange.getLowBound() && id <= initLargeScanRangeInQueryRange.getHighBound()) {
                            temporalIDOutRange.add(id);
                            count++;
                            if ((count % batchSize) == 0) {
                                Collections.sort(temporalIDOutRange);
                                temporalIDOutRangeList.add(temporalIDOutRange);
                                temporalIDOutRange = new ArrayList<>(batchSize);
                            }

                        }
                    }

                }
            }
        }


        long stopTime = System.currentTimeMillis();
        logger.info("[Virtual Layer] check out-of-range takes " + (stopTime - startTime));
        Collections.sort(temporalIDOutRange);
        temporalIDOutRangeList.add(temporalIDOutRange);

        // merge and sort id lists
        for (List<Long> idListOutRange : temporalIDOutRangeList) {
            cellIDListOutRange.addAll(idListOutRange);
        }

        extraCellIDListOutRange.addAll(cellIDListOutRange);
        Collections.sort(cellIDListOutRange);
        Collections.sort(extraCellIDListOutRange);

        List<Long> finalizedCellIDListOutRange = andResultForTwoSortedList(cellIDListOutRange, basicBitmap);
        List<Long> finalizedExtraCellIDListOutRange = andResultForTwoSortedList(extraCellIDListOutRange, extraBitmap);
        logger.info(String.format("[Virtual Layer] size of cells out of range: %d", finalizedCellIDListOutRange.size()));
        logger.info(String.format("[Virtual Layer] size of extra cells out of range: %d", finalizedExtraCellIDListOutRange.size()));

        ScanOptimizer scanOptimizer = new ScanOptimizer(subspaceLocation, initLargeScanRangeInQueryRange, finalizedCellIDListOutRange, finalizedExtraCellIDListOutRange);
        scanOptimizer.split();

        return scanOptimizer;
    }

    private BoundingBox computeValidQueryRangeInThisSubspace(SubspaceLocation subspaceLocation, BoundingBox queryRangeBoundBox) {
        CellLocation beginCellOfThisSubspace = VirtualSpaceTransformationHelper.getFirstCellLocationOfThisSubspace(subspaceLocation);
        BoundingBox subspaceBoundBox = new BoundingBox(beginCellOfThisSubspace, VirtualSpaceTransformationHelper.getLastCellLocationOfThisSubspace(subspaceLocation));
        BoundingBox validQueryRange = BoundingBox.computeIntersection(subspaceBoundBox, queryRangeBoundBox);
        return validQueryRange;
    }

}
