package com.rogerguo.cymo.hbase;


import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.curve.CurveMeta;
import com.rogerguo.cymo.curve.CurveUtil;
import com.rogerguo.cymo.virtual.entity.CellLocation;
import com.rogerguo.cymo.virtual.entity.NormalizedLocation;
import com.rogerguo.cymo.virtual.entity.SubspaceLocation;
import com.rogerguo.cymo.virtual.helper.CurveTransformationHelper;
import com.rogerguo.cymo.virtual.helper.NormalizedDimensionHelper;
import com.rogerguo.cymo.virtual.helper.VirtualSpaceTransformationHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description
 *  *  * The 3-d whole space is partitioned into several temporal partitions,
 *  *  * each temporal partition is divided into several subspace according to the spatial information,
 *  *  * for each subspace, it contains multiple 3d cells sorted by the specified partition strategy
 *  *
 *  *         row key scheme = [TIME_PREFIX] + TEMPORAL_PARTITION_UNIT id + ZOrder(SPATIAL_PARTITION_LATITUDE_UNIT id, SPATIAL_PARTITION_LONGITUDE_GRANULARITY id) + PartitionStrategy(virtual cell id (TEMPORAL_VIRTUAL id, SPATIAL_VIRTUAL_LONGITUDE id, SPATIAL_VIRTUAL_LATITUDE id))
 *  *         TIME_PREFIX is optional
 *  *         All data in the same virtual cell will be stored as a group (share the same row key but different column name)
 *  *
 *  *         timestamp in key just uses epoch hour.
 *  *
 *  *         if you want to add a new curve, write the curve implementation and add the related info to CurveType and CurveFactory
 *  *
 *  *
 *  *         Partition id -> epoch TimePeriod
 *  *         Subsapce id -> Partition strategy configuration
 *  *         Cell id -> Virtual layer configuration
 * @Author GUO Yang
 * @Date 2019-12-18 5:34 PM
 */
public class RowKeyHelper {

    public static boolean isNormalized = VirtualLayerConfiguration.IS_NORMALIZED;


    public static RowKeyItem generateDataTableRowKey(double longitude, double latitude, long time) {

        // 1. normalized
        NormalizedLocation normalizedLocation = null;
        if (isNormalized) {
            normalizedLocation = new NormalizedLocation((int)longitude, (int)latitude, (int)time);
        } else {
            int normalizedLon = NormalizedDimensionHelper.normalizedLon(longitude);
            int normalizedLat = NormalizedDimensionHelper.normalizedLat(latitude);
            int normalizedTime = NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, time);
            normalizedLocation = new NormalizedLocation(normalizedLon, normalizedLat, normalizedTime);
        }

        // 2. transform to virtual space
        CellLocation cell = VirtualSpaceTransformationHelper.toCellLocation(normalizedLocation);

        CellLocation firstCellLocationOfThisSubspace = VirtualSpaceTransformationHelper.getFirstCellLocationOfThisSubspace(cell);
        // 3. curve encoding
        long virtualCellID = CurveTransformationHelper.generate3D(cell.getCurveMeta(), cell.getCellLongitude() - firstCellLocationOfThisSubspace.getCellLongitude(), cell.getCellLatitude() - firstCellLocationOfThisSubspace.getCellLatitude(), cell.getCellTime() - firstCellLocationOfThisSubspace.getCellTime());
        long virtualSpaceID = CurveTransformationHelper.generate2D(new CurveMeta(VirtualLayerConfiguration.DEFAULT_STRATEGY), cell.getSubspaceLongitude(), cell.getSubspaceLatitude());
        int partitionID = cell.getPartitionID();

        // 4. generate key
        return new RowKeyItem(concatDataTableByteRowKey(partitionID, virtualSpaceID, virtualCellID), concatDataTableStringRowKey(partitionID, virtualSpaceID, virtualCellID), time, partitionID, virtualSpaceID, virtualCellID, (byte)0, cell.toString());

    }

    public static long getRowKeyTimeCount = 0L;
    public static RowKeyItem generateDataTableRowKeyForGeoMesa(double longitude, double latitude, long time) {

        // 1. normalized
        NormalizedLocation normalizedLocation = null;
        if (isNormalized) {
            normalizedLocation = new NormalizedLocation((int)longitude, (int)latitude, (int)time);
        } else {
            int normalizedLon = NormalizedDimensionHelper.normalizedLon(longitude);
            int normalizedLat = NormalizedDimensionHelper.normalizedLat(latitude);
            int normalizedTime = NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, time);
            normalizedLocation = new NormalizedLocation(normalizedLon, normalizedLat, normalizedTime);
        }

        // 2. transform to virtual space
        CellLocation cell = VirtualSpaceTransformationHelper.toCellLocation(normalizedLocation);

        //CellLocation firstCellLocationOfThisSubspace = VirtualSpaceTransformationHelper.getFirstCellLocationOfThisSubspace(cell);

        NormalizedLocation firstCellNormalizedLocation = VirtualSpaceTransformationHelper.fromSubspaceLocation(cell.getPartitionID(), cell.getSubspaceLongitude(), cell.getSubspaceLatitude());
        int firstCellLocationOfThisSubspaceLongitude = firstCellNormalizedLocation.getX() / VirtualLayerConfiguration.SPATIAL_VIRTUAL_LONGITUDE_GRANULARITY;
        int firstCellLocationOfThisSubspaceLatitude = firstCellNormalizedLocation.getY() / VirtualLayerConfiguration.SPATIAL_VIRTUAL_LATITUDE_GRANULARITY;
        int firstCellLocationOfThisSubspaceTime = firstCellNormalizedLocation.getT();


        // 3. curve encoding
        //long virtualCellID = CurveTransformationHelper.generate3D(cell.getCurveMeta(), cell.getCellLongitude() - firstCellLocationOfThisSubspace.getCellLongitude(), cell.getCellLatitude() - firstCellLocationOfThisSubspace.getCellLatitude(), cell.getCellTime() - firstCellLocationOfThisSubspace.getCellTime());
        long virtualCellID = CurveTransformationHelper.generate3D(cell.getCurveMeta(), cell.getCellLongitude() - firstCellLocationOfThisSubspaceLongitude, cell.getCellLatitude() - firstCellLocationOfThisSubspaceLatitude, cell.getCellTime() - firstCellLocationOfThisSubspaceTime);

        long virtualSpaceID = CurveTransformationHelper.generate2D(new CurveMeta(VirtualLayerConfiguration.DEFAULT_STRATEGY), cell.getSubspaceLongitude(), cell.getSubspaceLatitude());
        int partitionID = cell.getPartitionID();



        // 4. generate key
        return new RowKeyItem(concatDataTableByteRowKey(partitionID, virtualSpaceID, virtualCellID), null, time, partitionID, virtualSpaceID, virtualCellID, (byte)0, cell.toString());

    }

    public static RowKeyItem generateIndexTableRowKey(double longitude, double latitude, long time) {
        // 1. normalized
        NormalizedLocation normalizedLocation = null;
        if (isNormalized) {
            normalizedLocation = new NormalizedLocation((int)longitude, (int)latitude, (int)time);
        } else {
            int normalizedLon = NormalizedDimensionHelper.normalizedLon(longitude);
            int normalizedLat = NormalizedDimensionHelper.normalizedLat(latitude);
            int normalizedTime = NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, time);
            normalizedLocation = new NormalizedLocation(normalizedLon, normalizedLat, normalizedTime);
        }

        // 2. transform to virtual space
        CellLocation cell = VirtualSpaceTransformationHelper.toCellLocation(normalizedLocation);

        CellLocation firstCellLocationOfThisSubspace = VirtualSpaceTransformationHelper.getFirstCellLocationOfThisSubspace(cell);
        // 3. curve encoding
        long virtualCellID = CurveTransformationHelper.generate3D(cell.getCurveMeta(), cell.getCellLongitude() - firstCellLocationOfThisSubspace.getCellLongitude(), cell.getCellLatitude() - firstCellLocationOfThisSubspace.getCellLatitude(), cell.getCellTime() - firstCellLocationOfThisSubspace.getCellTime());
        long virtualSpaceID = CurveTransformationHelper.generate2D(new CurveMeta(VirtualLayerConfiguration.DEFAULT_STRATEGY), cell.getSubspaceLongitude(), cell.getSubspaceLatitude());
        int partitionID = cell.getPartitionID();

        // 4. generate key
        return new RowKeyItem(concatIndexTableByteRowKey(partitionID, virtualSpaceID), concatIndexTableStringRowKey(partitionID, virtualSpaceID), virtualCellID, partitionID, virtualSpaceID, virtualCellID, (byte)1, cell.toString());
    }

    /**
     * virtual bitmap table: row key = partition id + subspace id, qualifier name = epoch time unit / "time" value = bitmap (byte[])
     * @param longitude
     * @param latitude
     * @param time
     * @return
     */
    public static RowKeyItem generateBitmapTableRowKey(double longitude, double latitude, long time) {
        // 1. normalized
        NormalizedLocation normalizedLocation = null;
        if (isNormalized) {
            normalizedLocation = new NormalizedLocation((int)longitude, (int)latitude, (int)time);
        } else {
            int normalizedLon = NormalizedDimensionHelper.normalizedLon(longitude);
            int normalizedLat = NormalizedDimensionHelper.normalizedLat(latitude);
            int normalizedTime = NormalizedDimensionHelper.normalizedTime(VirtualLayerConfiguration.TEMPORAL_VIRTUAL_GRANULARITY, time);
            normalizedLocation = new NormalizedLocation(normalizedLon, normalizedLat, normalizedTime);
        }

        // 2. transform to virtual space
        CellLocation cell = VirtualSpaceTransformationHelper.toCellLocation(normalizedLocation);

        CellLocation firstCellLocationOfThisSubspace = VirtualSpaceTransformationHelper.getFirstCellLocationOfThisSubspace(cell);
        // 3. curve encoding
        long timeOffset = cell.getCellTime() - firstCellLocationOfThisSubspace.getCellTime();
        long virtualSpaceID = CurveTransformationHelper.generate2D(VirtualLayerConfiguration.DEFAULT_STRATEGY, cell.getSubspaceLongitude(), cell.getSubspaceLatitude());
        int partitionID = cell.getPartitionID();

        // 4. generate key
        return new RowKeyItem(concatIndexTableByteRowKey(partitionID, virtualSpaceID), concatIndexTableStringRowKey(partitionID, virtualSpaceID), timeOffset, (byte)2, cell);

    }

    public static List<RowKeyItem> generateBitmapTableRowKeyList(NormalizedLocation leftDownLocation, NormalizedLocation rightUpLocation) {
        // left down subspace
        SubspaceLocation startSubspaceLocation = VirtualSpaceTransformationHelper.toSubspaceLocation(leftDownLocation);
        // right up subspace
        SubspaceLocation stopSubspaceLocation = VirtualSpaceTransformationHelper.toSubspaceLocation(rightUpLocation);

        // 3. find the subspace list in the query region and generate index row key list
        List<RowKeyItem> indexRowKeyList = new ArrayList<>();
        for (int i = startSubspaceLocation.getPartitionID(); i <= stopSubspaceLocation.getPartitionID(); i++) {
            for (int j = startSubspaceLocation.getSubspaceLongitude(); j <= stopSubspaceLocation.getSubspaceLongitude(); j++) {
                for (int k = startSubspaceLocation.getSubspaceLatitude(); k <= stopSubspaceLocation.getSubspaceLatitude(); k++) {
                    SubspaceLocation subspaceLocation = VirtualSpaceTransformationHelper.toSubspaceLocation(VirtualSpaceTransformationHelper.fromSubspaceLocation(i, j, k));
                    RowKeyItem rowKeyItem = RowKeyHelper.generateBitmapTableRowKey(subspaceLocation);
                    indexRowKeyList.add(rowKeyItem);

                }
            }
        }
        return indexRowKeyList;
    }


    /**
     * curve meta table: row key = partition id + subspace id,  value = curve meta
     *
     * @return
     */
    public static RowKeyItem generateCurveMetaTableRowKey(int partitionID, long subspaceID) {

        // 4. generate key
        return new RowKeyItem(concatIndexTableByteRowKey(partitionID, subspaceID), concatIndexTableStringRowKey(partitionID, subspaceID));

    }

    /**
     * rowkey -> partitionID + subspaceID
     * @param rowKey
     * @return
     */
    public static Map<String, String> fromIndexStringRowKey(String rowKey) {

        String[] items = rowKey.split(",");
        Map<String, String> resultMap = new HashMap<>();
        resultMap.put("partitionID", items[0]);
        resultMap.put("subspaceID", items[1]);
        return resultMap;
    }


    public static RowKeyItem generateBitmapTableRowKey(SubspaceLocation subspaceLocation) {

        // 1. curve encoding
        long virtualSpaceID = CurveTransformationHelper.generate2D(VirtualLayerConfiguration.DEFAULT_STRATEGY, subspaceLocation.getSubspaceLongitude(), subspaceLocation.getSubspaceLatitude());
        int partitionID = subspaceLocation.getPartitionID();

        // 2. generate key
        return new RowKeyItem(concatIndexTableByteRowKey(partitionID, virtualSpaceID), concatIndexTableStringRowKey(partitionID, virtualSpaceID), -1, partitionID, virtualSpaceID, (byte) 2, subspaceLocation);
    }

    public static String concatDataTableStringRowKey(int partitionID, long subspaceID, long virtualCellID) {
        String formatPartitionID = String.format("%010d", partitionID);
        String formatSubspaceID = String.format("%010d", subspaceID);
        String formatCellID = String.format("%016d", virtualCellID);

        return formatPartitionID + "," + formatSubspaceID + "," + formatCellID;
    }

    public static String concatIndexTableStringRowKey(int partitionID, long subspaceID) {
        String formatPartitionID = String.format("%010d", partitionID);
        String formatSubspaceID = String.format("%010d", subspaceID);

        return formatPartitionID + "," + formatSubspaceID;
    }

    public static byte[] concatDataTableByteRowKey(int partitionId, long subspaceId, long virtualCellId) {

        // partitionId 3 bytes, subspaceId 5 bytes, virtualCellId 8 bytes
        byte[] bytes = new byte[16];
        byte[] partitionBytes = CurveUtil.toBytes(partitionId);
        byte[] subspaceBytes = CurveUtil.toBytes(subspaceId);
        byte[] virtualCellBytes = CurveUtil.toBytes(virtualCellId);

        System.arraycopy(partitionBytes, 1, bytes, 0, 3);
        System.arraycopy(subspaceBytes, 3, bytes, 3, 5);
        System.arraycopy(virtualCellBytes, 0, bytes, 8, 8);

        return bytes;
    }

    public static byte[] concatIndexTableByteRowKey(int partitionId, long subspaceId) {

        // partitionId 3 bytes, subspaceId 5 bytes
        byte[] bytes = new byte[8];
        byte[] partitionBytes = CurveUtil.toBytes(partitionId);
        byte[] subspaceBytes = CurveUtil.toBytes(subspaceId);

        System.arraycopy(partitionBytes, 1, bytes, 0, 3);
        System.arraycopy(subspaceBytes, 3, bytes, 3, 5);

        return bytes;
    }



    /**
     * rowkey -> partitionID + subspaceID
     * @param rowKey
     * @return
     */
    public static Map<String, Object> fromIndexByteRowKey(byte[] rowKey) {

        byte[] partitionIDBytes = new byte[]{0, 0, 0, 0};
        byte[] subspaceIDBytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 0};

        System.arraycopy(rowKey, 0, partitionIDBytes, 1, 3);
        System.arraycopy(rowKey, 3, subspaceIDBytes, 3, 5);

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("partitionID", CurveUtil.bytesToInt(partitionIDBytes));
        resultMap.put("subspaceID", CurveUtil.bytesToLong(subspaceIDBytes));
        return resultMap;
    }


    /**
     * rowkey -> partitionID + subspaceID + cellID
     * @param rowKey
     * @return
     */
    public static Map<String, Object> fromDataByteRowKey(byte[] rowKey) {

        byte[] partitionIDBytes = new byte[]{0, 0, 0, 0};
        byte[] subspaceIDBytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 0};
        byte[] cellIDBytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 0};

        System.arraycopy(rowKey, 0, partitionIDBytes, 1, 3);
        System.arraycopy(rowKey, 3, subspaceIDBytes, 3, 5);
        System.arraycopy(rowKey, 8, cellIDBytes, 0, 8);

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("partitionID", CurveUtil.bytesToInt(partitionIDBytes));
        resultMap.put("subspaceID", CurveUtil.bytesToLong(subspaceIDBytes));
        resultMap.put("cellID", CurveUtil.bytesToLong(cellIDBytes));
        return resultMap;
    }

}
