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
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

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

    private static final String CURVE_META_TABLE = "virtual_space_metadata_table";

    public final static String COLUMN_FAMILY_NAME = "cf";

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

    public static CurveMeta getCurveMetaByNormalizedLocation(NormalizedLocation normalizedLocation) {

        PartitionLocation partitionLocation = new PartitionLocation(normalizedLocation);

        if (VirtualLayerConfiguration.IS_DYNAMIC_CURVE) {
            int subspaceLongitude = normalizedLocation.getX() / VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY;
            int subspaceLatitude = normalizedLocation.getY() / VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY;
            long subspaceID = zCurve.getCurveValue(subspaceLongitude, subspaceLatitude);
            RowKeyItem rowKeyItem = RowKeyHelper.generateCurveMetaTableRowKey(partitionLocation.getPartitionID(), subspaceID);

            try {
                Result result = hBaseDriver.get(CURVE_META_TABLE, rowKeyItem.getBytesRowKey());
                if (result.getRow() != null) {

                    List<Cell> cellList = result.listCells();
                    if (cellList != null && cellList.size() != 0) {
                        Cell cell = cellList.get(0);
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        String[] valueItems = value.split(",");
                        CurveType curveType = CurveType.valueOf(valueItems[0]);
                        if (curveType == CurveType.CUSTOM_CURVE_XYTTXY || curveType == CurveType.CUSTOM_CURVE_XYTXYT) {
                            return new CurveMeta(curveType, Integer.valueOf(valueItems[1]), Integer.valueOf(valueItems[2]), Integer.valueOf(valueItems[3]), Integer.valueOf(valueItems[4]), Integer.valueOf(valueItems[5]), Integer.valueOf(valueItems[6]));
                        } else {
                            return new CurveMeta(curveType);
                        }
                    }
                }
            } catch (IOException e) {
                logger.info("get curve meta fail");
                e.printStackTrace();
            }
        }

        if (partitionLocation.getNormalizedPartitionLength() == VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH) {
            return new CurveMeta(VirtualLayerConfiguration.PARTITION_A_DEFAULT_STRATEGY);
        } else if (partitionLocation.getNormalizedPartitionLength() == VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH) {
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
