package com.rogerguo.client.cymo;

import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.entity.SpatialRange;
import com.rogerguo.cymo.entity.SpatialTemporalRecord;
import com.rogerguo.cymo.entity.TimeRange;
import com.rogerguo.cymo.hbase.HBaseDriver;
import com.rogerguo.cymo.hbase.RowKeyHelper;
import com.rogerguo.cymo.hbase.RowKeyItem;
import com.rogerguo.cymo.virtual.VirtualLayerGeoMesa;
import com.rogerguo.cymo.virtual.entity.NormalizedLocation;
import com.rogerguo.cymo.virtual.entity.NormalizedRange;
import com.rogerguo.cymo.virtual.entity.SubspaceLocation;
import com.rogerguo.cymo.virtual.helper.NormalizedDimensionHelper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @Description generate aggregated bitmap
 * @Date 6/11/20 9:58 PM
 * @Created by rogerguo
 */
public class VirtualLayerBitmapGenerator {

    private HBaseDriver hBaseDriver;

    public VirtualLayerBitmapGenerator(String zookeeperUrl) {
        this.hBaseDriver = new HBaseDriver(zookeeperUrl);

    }

    public static void main(String[] args) throws IOException {
        VirtualLayerBitmapGenerator generator = new VirtualLayerBitmapGenerator("127.0.0.1");
        /*generator.generatePointCount();
         generator.generateAggregatedBitmap(new SpatialRange(-75.960000, -71.860000),
                new SpatialRange(40.000000,42.999999),
                new TimeRange(fromDateToTimestamp("2010-01-01 00:00:00"), fromDateToTimestamp("2011-01-31 23:59:59")));
*/
        long start = System.currentTimeMillis();
        String dataTable = "geomesa_cymo_param_test_nyc_2dtaxi_2ddata_2dproduction_2ddynamic_2dmultiple_2dprediction_cymo_geom_dtg_v1";
        //dataTable = "geomesa_cymo_test_new_nyc_2dtaxi_2ddata_2dtest_2dnew_2done_2drecord_cymo_geom_dtg_v1";
        generator.generatePointCount(dataTable);
        long stop = System.currentTimeMillis();
        System.out.println((stop-start));

    }

    public static long fromDateToTimestamp(String dateString) {
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);
        long time = Date.from(LocalDateTime.parse(dateString, dateFormat).toInstant(ZoneOffset.UTC)).getTime();
        return time;
    }


    /**
     * new version through hbase table
     * @param dataTableName
     */
    public void generatePointCount(String dataTableName) throws IOException {

        // for index table, row key = partition id + subspace id, qualifier name = virtual cell id, value = count number of this cell
        // for data table, partitionId 3 bytes, subspaceId 5 bytes, virtualCellId 8 bytes
        ResultScanner scanner = hBaseDriver.scan(dataTableName);

        int count = 0;
        Map<String, Integer> cellCount = new HashMap<>();
        for (Result result : scanner) {
            if (result.getRow() != null) {
                //needs to know rowkey format of geomesa: shard(1 byte) + partitionId 3 bytes, subspaceId 5 bytes, virtualCellId 8 bytes
                byte[] rowKey = result.getRow();
                byte[] cymoInfoBytes = new byte[16];
                System.arraycopy(rowKey, 1, cymoInfoBytes, 0, 16);
                Map<String, Object> parsedResult = RowKeyHelper.fromDataByteRowKey(cymoInfoBytes);
                int partitionId = (int) parsedResult.get("partitionID");
                long subspaceId = (long) parsedResult.get("subspaceID");
                long cellId = (long) parsedResult.get("cellID");
                StringBuilder builder = new StringBuilder();
                String rowKeyString = builder.append(partitionId).append(",").append(subspaceId).append(",").append(cellId).toString();
                List<Cell> cellList = result.listCells();

                if (!cellCount.containsKey(rowKeyString)) {
                    cellCount.put(rowKeyString, cellList.size());
                } else {
                    cellCount.put(rowKeyString, cellCount.get(rowKeyString) + cellList.size());
                }
                count++;
                System.out.println(count);
            }
        }
        System.out.println("finsish");

        Set<String> rowkeySet = cellCount.keySet();
        Map<String, List<Long>> subspaceBasicBitmap = new HashMap<>();
        Map<String, List<Long>> subspaceExtraBitmap = new HashMap<>();
        for (String key : rowkeySet) {
            String[] items = key.split(",");
            String subspaceKey = RowKeyHelper.concatIndexTableStringRowKey(Integer.valueOf(items[0]), Long.valueOf(items[1]));
            Long cellID = Long.valueOf(items[2]);
            int pointCountInCell = cellCount.get(key);

            if (subspaceBasicBitmap.containsKey(subspaceKey)) {
                subspaceBasicBitmap.get(subspaceKey).add(cellID);
            } else {
                subspaceBasicBitmap.put(subspaceKey, new ArrayList<>());
                subspaceBasicBitmap.get(subspaceKey).add(cellID);
            }

            if (pointCountInCell > VirtualLayerConfiguration.BASIC_BITMAP_UP_BOUND) {
                if (subspaceExtraBitmap.containsKey(subspaceKey)) {
                    subspaceExtraBitmap.get(subspaceKey).add(cellID);
                } else {
                    subspaceExtraBitmap.put(subspaceKey, new ArrayList<>());
                    subspaceExtraBitmap.get(subspaceKey).add(cellID);
                }
            }
        }

        List<Put> putList = new ArrayList<>();
        Set<String> bitmapKeySet = subspaceBasicBitmap.keySet();
        for (String bitmapKey : bitmapKeySet) {
            Put basicPut = new Put(Bytes.toBytes(bitmapKey));
            String basicListString = fromListValue2String(subspaceBasicBitmap.get(bitmapKey));
            basicPut.addColumn(Bytes.toBytes("agg"), Bytes.toBytes("agg_basic"), Bytes.toBytes(basicListString));
            putList.add(basicPut);

            Put extraPut = new Put(Bytes.toBytes(bitmapKey));
            String extraListString = fromListValue2String(subspaceExtraBitmap.get(bitmapKey));
            extraPut.addColumn(Bytes.toBytes("agg"), Bytes.toBytes("agg_extra"), Bytes.toBytes(extraListString));
            putList.add(extraPut);
        }

        hBaseDriver.batchPut(VirtualLayerGeoMesa.VIRTUAL_LAYER_INFO_TABLE, putList);

        /*int batchSize = 8192;

        List<Put> putList = new ArrayList<>();
        Set<String> rowKeySet = subspaceCount.keySet();
        for (String rowKey : rowKeySet) {
            String[] items = rowKey.split(",");
            String indexRowKey = items[0] + "," + items[1];
            Put put = new Put(Bytes.toBytes(indexRowKey));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(Long.valueOf(items[2])), Bytes.toBytes(subspaceCount.get(rowKey)));
            putList.add(put);

            if (putList.size() % batchSize == 0) {
                hBaseDriver.batchPut(VirtualLayerGeoMesa.VIRTUAL_LAYER_INFO_TABLE, new ArrayList<>(putList));
                putList.clear();
            }

        }
        hBaseDriver.batchPut(VirtualLayerGeoMesa.VIRTUAL_LAYER_INFO_TABLE, new ArrayList<>(putList));*/


    }

    public List<SpatialTemporalRecord> generatePointCount() {
        List<SpatialTemporalRecord> recordList = new ArrayList<>();
        URL input = getClass().getClassLoader().getResource("dataset/trip_data_1_pickup.csv");
        //URL input = getClass().getClassLoader().getResource("G:\\DataSet\\FOIL2010\\format\\trip_data_1_pickup.csv");
        if (input == null) {
            throw new RuntimeException("Couldn't load resource trip_data_1.csv");
        }

        int count = 0;
        // date parser corresponding to the CSV format
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);
        try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.DEFAULT)) {
            for (CSVRecord record : parser) {
                String seqID = record.get(0);
                String medallion = record.get(1);
                String tripTimeInSecs = record.get(3);
                String tripDistance = record.get(4);
                long time = Date.from(LocalDateTime.parse(record.get(2), dateFormat).toInstant(ZoneOffset.UTC)).getTime();
                double longitude = Double.parseDouble(record.get(5));
                double latitude = Double.parseDouble(record.get(6));
                SpatialTemporalRecord nycTaxiRecord = new SpatialTemporalRecord(seqID, longitude, latitude, time);
                recordList.add(nycTaxiRecord);
                count++;
                if (((count) % 8192) == 0) {
                    System.out.println(recordList.size());
                    batchUpdatePointCountForEachCell(recordList);
                    recordList.clear();
                    System.out.println("finish " + count);
                }

            }
            batchUpdatePointCountForEachCell(recordList);
            System.out.println("final count value: " + count);
        } catch (IOException e) {
            throw new RuntimeException("Error reading taxi data:", e);
        }

        return recordList;
    }

    public void batchUpdatePointCountForEachCell(List<SpatialTemporalRecord> recordList) throws IOException {
        List<Put> dataTablePutList = new ArrayList<>();
        Map<String, Integer> indexTablePutMap = new HashMap<>(); // key: rowkey+qualifier; value: count
        List<Put> indexTablePutList = new ArrayList<>();
        List<Get> indexTableGetList = new ArrayList<>();


        for (int i = 0; i < recordList.size(); i++) {
            SpatialTemporalRecord record = recordList.get(i);


            // for index table, row key = partition id + subspace id, qualifier name = virtual cell id, value = count number of this cell
            RowKeyItem indexRowKeyItem = RowKeyHelper.generateIndexTableRowKey(record.getLongitude(), record.getLatitude(), record.getTimestamp());
            Get indexGet = new Get(Bytes.toBytes(indexRowKeyItem.getStringRowKey()));
            //Get indexGet = new Get((indexRowKeyItem.getBytesRowKey()));
            indexGet.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(indexRowKeyItem.getQualifier()));
            indexTableGetList.add(indexGet);


            String putMapKey = indexRowKeyItem.getStringRowKey() + "_" + indexRowKeyItem.getQualifier();
            if (!indexTablePutMap.containsKey(putMapKey)) {
                indexTablePutMap.put(putMapKey, 1);
            } else {
                int newCount = indexTablePutMap.get(putMapKey) + 1;
                indexTablePutMap.put(putMapKey, newCount);
            }
        }


        Result[] indexResults = hBaseDriver.batchGet(VirtualLayerGeoMesa.VIRTUAL_LAYER_INFO_TABLE, indexTableGetList);
        indexTableGetList.clear();

        Map<String, Integer> indexResultMap = new HashMap<>();
        for (Result indexResult : indexResults) {
            if (indexResult.getRow() != null) {
                String rowKey = Bytes.toString(indexResult.getRow());
                List<Cell> cellList = indexResult.listCells();
                int count = 0;
                if (cellList != null && cellList.size() != 0) {
                    Cell cell = cellList.get(0);
                    String qualifier = String.valueOf(Bytes.toLong(CellUtil.cloneQualifier(cell)));
                    String resultPutMapKey = rowKey + "_" + qualifier;
                    count = Bytes.toInt(CellUtil.cloneValue(cell));

                    indexResultMap.put(resultPutMapKey, count);

                }
            }
        }

        Map<String, Integer> mergedCellCountMap = new HashMap<>();
        for (String key : indexTablePutMap.keySet()) {
            if (indexResultMap.containsKey(key)) {
                int addCount = indexResultMap.get(key) + indexTablePutMap.get(key);
                String[] keyItem = key.split("_");
                Put indexPut = new Put(Bytes.toBytes(keyItem[0]));
                indexPut.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(Long.valueOf(keyItem[1])), Bytes.toBytes(addCount));
                indexTablePutList.add(indexPut);
                mergedCellCountMap.put(key, addCount);
            } else {
                String[] keyItem = key.split("_");
                Put indexPut = new Put(Bytes.toBytes(keyItem[0]));
                indexPut.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(Long.valueOf(keyItem[1])), Bytes.toBytes(indexTablePutMap.get(key)));
                indexTablePutList.add(indexPut);
                mergedCellCountMap.put(key, indexTablePutMap.get(key));
            }
        }
        hBaseDriver.batchPut(VirtualLayerGeoMesa.VIRTUAL_LAYER_INFO_TABLE, new ArrayList<>(indexTablePutList));
        indexTablePutMap.clear();
        indexResultMap.clear();
        indexTablePutList.clear();


        System.out.println("----------------------------------\n");
        //logger.info("Finish one batch insert");

    }

    public void generateAggregatedBitmap(SpatialRange longitudeRange, SpatialRange latitudeRange, TimeRange timeRange) throws IOException {
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


        List<RowKeyItem> indexRowKeyList = parseQueryInVirtualLayer(normalizedLongitudeRange, normalizedLatitudeRange, normalizedTimeRange);
        Map<Integer, Map<Long, Map<String, List<Long>>>> bitmapResult = parseBitmapInVirtualLayer(indexRowKeyList);

        List<Put> putList = new ArrayList<>();
        for (RowKeyItem key : indexRowKeyList) {
            int partitionID = ((SubspaceLocation) key.getBitmapPayload()).getPartitionID();
            long subSubspaceID = key.getSubspaceID();

            Put basicPut = new Put(Bytes.toBytes(key.getStringRowKey()));
            String basicListString = fromListValue2String(bitmapResult.get(partitionID).get(subSubspaceID).get("basic"));
            basicPut.addColumn(Bytes.toBytes("agg"), Bytes.toBytes("agg_basic"), Bytes.toBytes(basicListString));
            putList.add(basicPut);

            Put extraPut = new Put(Bytes.toBytes(key.getStringRowKey()));
            String extraListString = fromListValue2String(bitmapResult.get(partitionID).get(subSubspaceID).get("extra"));
            extraPut.addColumn(Bytes.toBytes("agg"), Bytes.toBytes("agg_extra"), Bytes.toBytes(extraListString));
            putList.add(extraPut);
        }

        hBaseDriver.batchPut(VirtualLayerGeoMesa.VIRTUAL_LAYER_INFO_TABLE, putList);

    }

    public List<RowKeyItem> parseQueryInVirtualLayer(NormalizedRange normalizedLongitudeRange, NormalizedRange normalizedLatitudeRange, NormalizedRange normalizedTimeRange) {
        // 1. normalize

        NormalizedLocation leftDownLocation = new NormalizedLocation(normalizedLongitudeRange.getLowBound(), normalizedLatitudeRange.getLowBound(), normalizedTimeRange.getLowBound());
        NormalizedLocation rightUpLocation = new NormalizedLocation(normalizedLongitudeRange.getHighBound(), normalizedLatitudeRange.getHighBound(), normalizedTimeRange.getHighBound());

        // 2. transform to virtual layer and generate row key list
        List<RowKeyItem> bitmapTableRowKeyList = RowKeyHelper.generateBitmapTableRowKeyList(leftDownLocation, rightUpLocation);
        return bitmapTableRowKeyList;
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
        String[] valuesString = stringValue.split(",");

        for (int i = 0; i < valuesString.length; i++) {
            values.add(Long.valueOf(valuesString[i]));
        }

        return values;
    }

    public Map<Integer, Map<Long, Map<String, List<Long>>>> parseBitmapInVirtualLayer(List<RowKeyItem> bitmapTableRowKeyList) throws IOException {
        //  partitionID subspaceID;
        Map<Integer, Map<Long, Map<String, List<Long>>>> virtualLayerDataMap = new HashMap<>();

        List<Get> basicGetList = new ArrayList<>();
        for (RowKeyItem key : bitmapTableRowKeyList) {
            Get basicGet = new Get(Bytes.toBytes(key.getStringRowKey()));
            basicGet.addFamily(Bytes.toBytes("cf"));
            basicGetList.add(basicGet);


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


        long start = System.currentTimeMillis();
        Result[] results = hBaseDriver.batchGet(VirtualLayerGeoMesa.VIRTUAL_LAYER_INFO_TABLE, basicGetList);
        long stop = System.currentTimeMillis();
        System.out.println("Batch get bitmap takes " + (stop - start));
        if (results != null && results.length > 0) {
            // count of one cell in the subspace
            for (Result result : results) {
                List<Cell> cellList = result.listCells();
                if (cellList != null && cellList.size() > 0) {
                    for (Cell cell : cellList) {
                        // partition id + subspace id
                        byte[] rowKey = result.getRow();
                        // virtual cell id
                        long virtualCellID = Bytes.toLong(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                        // count
                        byte[] bitmapValue = CellUtil.cloneValue(cell);
                        int count = Bytes.toInt(bitmapValue);


                        Map<String, String> parsedRowKey = RowKeyHelper.fromIndexStringRowKey(Bytes.toString(rowKey));  // TODO to byte
                        virtualLayerDataMap.get(Integer.valueOf(parsedRowKey.get("partitionID"))).get(Long.valueOf(parsedRowKey.get("subspaceID"))).get("basic").add(virtualCellID);
                        if (count > VirtualLayerConfiguration.BASIC_BITMAP_UP_BOUND) {
                            virtualLayerDataMap.get(Integer.valueOf(parsedRowKey.get("partitionID"))).get(Long.valueOf(parsedRowKey.get("subspaceID"))).get("extra").add(virtualCellID);

                        }
                    }
                }
            }
        }


        return virtualLayerDataMap;

    }

}
