package com.rogerguo.cymo.virtual.helper;

import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.curve.CurveMeta;
import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.curve.ZCurve;
import com.rogerguo.cymo.hbase.HBaseDriver;
import com.rogerguo.cymo.hbase.RowKeyHelper;
import com.rogerguo.cymo.hbase.RowKeyItem;
import com.rogerguo.cymo.virtual.entity.NormalizedLocation;
import com.rogerguo.cymo.virtual.entity.PartitionLocation;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @Description
 * Store the layout scheme of each subspace in HBase
 * Row key = partitionID + subspace ID, value = curve type
 * Custom curve type
 *
 * @Author GUO Yang
 * @Date 2019-12-06 9:34 PM
 */
public class PartitionCurveStrategyHelper {

    private static Logger logger = LoggerFactory.getLogger(PartitionCurveStrategyHelper.class);

    private static HBaseDriver hBaseDriver = new HBaseDriver("127.0.0.1");

    private static ZCurve zCurve = new ZCurve(); // used to encode subspace id

    public static final String CURVE_META_TABLE = VirtualLayerConfiguration.CURVE_META_TABLE;

    public final static String COLUMN_FAMILY_NAME = "cf";

    // TODO #cache# multiple thread

    public static Map<String, CurveMeta> curveMetaMapCache = new HashMap<>();


    static {
        try {
            ResultScanner scanner = hBaseDriver.scan(CURVE_META_TABLE);
            Result result = scanner.next();
            while (result != null) {
                byte[] bytesRowKey = result.getRow();
                Map<String, Object> keyMap = RowKeyHelper.fromIndexByteRowKey(bytesRowKey);
                String stringRowKey = RowKeyHelper.concatIndexTableStringRowKey((int) keyMap.get("partitionID"), (long) keyMap.get("subspaceID"));
                if (result.getRow() != null) {

                    List<Cell> cellList = result.listCells();
                    if (cellList != null && cellList.size() != 0) {
                        Cell cell = cellList.get(0);
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        String[] valueItems = value.split(",");
                        CurveType curveType = CurveType.valueOf(valueItems[0]);
                        if (curveType == CurveType.CUSTOM_CURVE_XYTTXY || curveType == CurveType.CUSTOM_CURVE_XYTXYT) {
                            CurveMeta curveMeta = new CurveMeta(curveType, Integer.valueOf(valueItems[1]), Integer.valueOf(valueItems[2]), Integer.valueOf(valueItems[3]), Integer.valueOf(valueItems[4]), Integer.valueOf(valueItems[5]), Integer.valueOf(valueItems[6]));
                            curveMetaMapCache.put(stringRowKey, curveMeta);

                        } else if (curveType == CurveType.CUSTOM_CURVE_XYT || curveType == CurveType.CUSTOM_CURVE_TXY) {
                            CurveMeta curveMeta = new CurveMeta(curveType, Integer.valueOf(valueItems[1]), Integer.valueOf(valueItems[2]), Integer.valueOf(valueItems[3]), Integer.valueOf(valueItems[4]), Integer.valueOf(valueItems[5]), Integer.valueOf(valueItems[6]));
                            curveMetaMapCache.put(stringRowKey, curveMeta);

                        } else {
                            CurveMeta curveMeta = new CurveMeta(curveType);
                            curveMetaMapCache.put(stringRowKey, curveMeta);

                        }
                    }
                } else {
                    curveMetaMapCache.put(stringRowKey, null);
                }
                result = scanner.next();
            }
            System.out.println("finish meta table cache");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static CurveType getCurveTypeByNormalizedLocation(NormalizedLocation normalizedLocation) {
        PartitionLocation partitionLocation = new PartitionLocation(normalizedLocation);
        if (partitionLocation.getNormalizedPartitionLength() == VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH) {
            return VirtualLayerConfiguration.PARTITION_A_DEFAULT_STRATEGY;
        } else if (partitionLocation.getNormalizedPartitionLength() == VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH) {
            return VirtualLayerConfiguration.PARTITION_B_DEFAULT_STRATEGY;
        } else {
            return VirtualLayerConfiguration.DEFAULT_STRATEGY;
        }
    }

    static int count = 0;
    public static CurveMeta getCurveMetaByNormalizedLocation(NormalizedLocation normalizedLocation) {

        PartitionLocation partitionLocation = new PartitionLocation(normalizedLocation);

        if (VirtualLayerConfiguration.IS_DYNAMIC_CURVE) {

            int subspaceLongitude = normalizedLocation.getX() / VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY;
            int subspaceLatitude = normalizedLocation.getY() / VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY;
            long subspaceID = zCurve.getCurveValue(subspaceLongitude, subspaceLatitude);
            //RowKeyItem rowKeyItem = RowKeyHelper.generateCurveMetaTableRowKey(partitionLocation.getPartitionID(), subspaceID);
            String stringRowKey = RowKeyHelper.concatIndexTableStringRowKey(partitionLocation.getPartitionID(), subspaceID);

            if (curveMetaMapCache.containsKey(stringRowKey) && curveMetaMapCache.get(stringRowKey) != null) {
                return curveMetaMapCache.get(stringRowKey);
            } else if (curveMetaMapCache.containsKey(stringRowKey) && curveMetaMapCache.get(stringRowKey) == null) {
                // jump to default config
            }
            else {
                count++;
                System.out.println("from hbase get scheme: " + count);
                try {
                    RowKeyItem rowKeyItem = RowKeyHelper.generateCurveMetaTableRowKey(partitionLocation.getPartitionID(), subspaceID);
                    Result result = hBaseDriver.get(CURVE_META_TABLE, rowKeyItem.getBytesRowKey());
                    if (result.getRow() != null) {

                        List<Cell> cellList = result.listCells();
                        if (cellList != null && cellList.size() != 0) {
                            Cell cell = cellList.get(0);
                            String value = Bytes.toString(CellUtil.cloneValue(cell));
                            String[] valueItems = value.split(",");
                            CurveType curveType = CurveType.valueOf(valueItems[0]);
                            if (curveType == CurveType.CUSTOM_CURVE_XYTTXY || curveType == CurveType.CUSTOM_CURVE_XYTXYT) {
                                CurveMeta curveMeta = new CurveMeta(curveType, Integer.valueOf(valueItems[1]), Integer.valueOf(valueItems[2]), Integer.valueOf(valueItems[3]), Integer.valueOf(valueItems[4]), Integer.valueOf(valueItems[5]), Integer.valueOf(valueItems[6]));
                                curveMetaMapCache.put(rowKeyItem.getStringRowKey(), curveMeta);
                                return curveMeta;
                            } else if (curveType == CurveType.CUSTOM_CURVE_XYT || curveType == CurveType.CUSTOM_CURVE_TXY) {
                                CurveMeta curveMeta = new CurveMeta(curveType, Integer.valueOf(valueItems[1]), Integer.valueOf(valueItems[2]), Integer.valueOf(valueItems[3]), Integer.valueOf(valueItems[4]), Integer.valueOf(valueItems[5]), Integer.valueOf(valueItems[6]));
                                curveMetaMapCache.put(rowKeyItem.getStringRowKey(), curveMeta);
                                return curveMeta;
                            } else {
                                CurveMeta curveMeta = new CurveMeta(curveType);
                                curveMetaMapCache.put(rowKeyItem.getStringRowKey(), curveMeta);
                                return curveMeta;
                            }
                        }
                    } else {
                        curveMetaMapCache.put(rowKeyItem.getStringRowKey(), null);
                    }
                } catch (IOException e) {
                    logger.info("get curve meta fail");
                    e.printStackTrace();
                }
            }


        }

        if (partitionLocation.getNormalizedPartitionLength() == VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH
        && partitionLocation.getPartitionID() % 2 == 0) {
            return new CurveMeta(VirtualLayerConfiguration.PARTITION_A_DEFAULT_STRATEGY);
        } else if (partitionLocation.getNormalizedPartitionLength() == VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH
        && partitionLocation.getPartitionID() % 2 == 1) {
            return new CurveMeta(VirtualLayerConfiguration.PARTITION_B_DEFAULT_STRATEGY);
        } else {
            return new CurveMeta(VirtualLayerConfiguration.DEFAULT_STRATEGY);
        }
    }


    public static void insertCurveMetaForSubspace(CurveMeta curveMeta, NormalizedLocation normalizedLocation) {

        PartitionLocation partitionLocation = new PartitionLocation(normalizedLocation);
        int subspaceLongitude = normalizedLocation.getX() / VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY;
        int subspaceLatitude = normalizedLocation.getY() / VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY;
        long subspaceID = zCurve.getCurveValue(subspaceLongitude, subspaceLatitude);
        RowKeyItem rowKeyItem = RowKeyHelper.generateCurveMetaTableRowKey(partitionLocation.getPartitionID(), subspaceID);

        Put put = new Put(rowKeyItem.getBytesRowKey());
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(""),Bytes.toBytes(curveMeta.toString()));
        try {
            hBaseDriver.put(CURVE_META_TABLE, put);
        } catch (IOException e) {
            logger.error("insert curve meta table fail");
            e.printStackTrace();

        }

    }

}
