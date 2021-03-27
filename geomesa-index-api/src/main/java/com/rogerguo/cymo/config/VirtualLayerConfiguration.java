package com.rogerguo.cymo.config;

import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.virtual.normalization.TimePeriod;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description uniform space
 *
 * temporalPartitionLength = n * temporalVirtualGranularity
 * spatialLongitudePartitionLength = n * spatialVirtualLongitudeGranularity
 * spatialLatitudePartitionLength = n * spatialLatitudePartitionLength
 *
 * at this stage, the temporal partition pattern only like this: ABABAB...AB...
 * @Date 6/4/20 8:35 PM
 * @Created by rogerguo
 */
public class VirtualLayerConfiguration {

    public static Map<String, String> configMap;

    private static Map<String, TimePeriod> timeGranularityMap;

    private static Map<String, CurveType> curveTypeMap;

    static {
        timeGranularityMap = new HashMap<>();
        timeGranularityMap.put("hour", TimePeriod.HOUR);
        timeGranularityMap.put("day", TimePeriod.DAY);

        curveTypeMap = new HashMap<>();
        curveTypeMap.put("CURVE_T1X3Y3", CurveType.CURVE_T1X3Y3);
        curveTypeMap.put("CURVE_T1X7Y7", CurveType.CURVE_T1X7Y7);
        curveTypeMap.put("Z_CURVE", CurveType.Z_CURVE);
        curveTypeMap.put("Z_CURVE_TXY",CurveType.Z_CURVE_TXY);

        configMap = new HashMap<>();
        try {
            BufferedReader in = new BufferedReader(new FileReader("E:\\Projects\\idea\\geomesa-cymo\\geomesa-index-api\\src\\main\\resources\\cymo\\conf\\test_dynamic.conf"));
            String str;
            while ((str = in.readLine()) != null) {
                String[] confPair = str.split("=");
                configMap.put(confPair[0], confPair[1]);
                System.out.println(str);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* Parameters of virtual layer*/
    public final static TimePeriod TEMPORAL_VIRTUAL_GRANULARITY = timeGranularityMap.get(configMap.get("TEMPORAL_VIRTUAL_GRANULARITY"));

    public final static int SPATIAL_VIRTUAL_LATITUDE_GRANULARITY = Integer.valueOf(configMap.get("SPATIAL_VIRTUAL_LATITUDE_GRANULARITY"));  // uniform space, one unit is 0.000085

    public final static int SPATIAL_VIRTUAL_LONGITUDE_GRANULARITY = Integer.valueOf(configMap.get("SPATIAL_VIRTUAL_LONGITUDE_GRANULARITY"));  // uniform space, one unit is 0.00017

    /* Parameters of partition */
    public final static int TEMPORAL_PARTITION_OFFSET = Integer.valueOf(configMap.get("TEMPORAL_PARTITION_OFFSET"));

    public final static int TEMPORAL_PARTITION_A_LENGTH = Integer.valueOf(configMap.get("TEMPORAL_PARTITION_A_LENGTH")); // TimePeriod * 120

    public final static CurveType PARTITION_A_DEFAULT_STRATEGY = curveTypeMap.get(configMap.get("PARTITION_A_DEFAULT_STRATEGY"));

    public final static int TEMPORAL_PARTITION_B_LENGTH = Integer.valueOf(configMap.get("TEMPORAL_PARTITION_B_LENGTH"));

    public final static CurveType PARTITION_B_DEFAULT_STRATEGY = curveTypeMap.get(configMap.get("PARTITION_B_DEFAULT_STRATEGY"));

    public final static CurveType DEFAULT_STRATEGY = curveTypeMap.get(configMap.get("DEFAULT_STRATEGY"));  // used to generate 2D curve (subspace id)

    public final static int PARTITION_LONGITUDE_GRANULARITY = SPATIAL_VIRTUAL_LONGITUDE_GRANULARITY * Integer.valueOf(configMap.get("PARTITION_LONGITUDE_GRANULARITY"));

    public final static int PARTITION_LATITUDE_GRANULARITY = SPATIAL_VIRTUAL_LATITUDE_GRANULARITY * Integer.valueOf(configMap.get("PARTITION_LATITUDE_GRANULARITY"));


    /* aggregation parameter */

    public final static int DIRECT_AGGREGATE_THRESHOLD = Integer.valueOf(configMap.get("DIRECT_AGGREGATE_THRESHOLD"));  // the param of direct aggregation: 0 means does not aggregate adjacent sub scans

    public final static int AGGREGATE_SUB_SCAN_NUM_THRESHOLD = Integer.valueOf(configMap.get("AGGREGATE_SUB_SCAN_NUM_THRESHOLD"));  // 0 means use direct aggregation

    public final static int LOGICAL_SEEK_COST = Integer.valueOf(configMap.get("LOGICAL_SEEK_COST"));  // the param of adaptive aggregation: 0 means aggregate all sub scans

    public final static int LOGICAL_READ_ONE_BLOCK_COST = Integer.valueOf(configMap.get("LOGICAL_READ_ONE_BLOCK_COST"));  // the param of adaptive aggregation: seek << read, no aggregation

    public final static int SPLIT_THRESHOLD = Integer.valueOf(configMap.get("SPLIT_THRESHOLD"));

    /* bitmap parameter */
    public final static int BASIC_BITMAP_UP_BOUND = Integer.valueOf(configMap.get("BASIC_BITMAP_UP_BOUND"));

    public final static int EXTRA_BITMAP_WEIGHT = Integer.valueOf(configMap.get("EXTRA_BITMAP_WEIGHT"));

    public final static boolean IS_NORMALIZED = Boolean.valueOf(configMap.get("IS_NORMALIZED"));

    /* curve mate parameter */
    public final static boolean IS_DYNAMIC_CURVE = Boolean.valueOf(configMap.get("IS_DYNAMIC_CURVE"));

    public final static boolean IS_WITH_META = Boolean.valueOf(configMap.get("IS_WITH_META"));

    public static final String VIRTUAL_LAYER_INFO_TABLE = configMap.get("VIRTUAL_LAYER_INFO_TABLE");

    public static final String CURVE_META_TABLE = configMap.get("CURVE_META_TABLE");

    /* Parameters of virtual layer*//*
    public final static TimePeriod TEMPORAL_VIRTUAL_GRANULARITY = TimePeriod.HOUR;

    public final static int SPATIAL_VIRTUAL_LATITUDE_GRANULARITY = 16;  // uniform space, one unit is 0.000085

    public final static int SPATIAL_VIRTUAL_LONGITUDE_GRANULARITY = 8;  // uniform space, one unit is 0.00017

    *//* Parameters of partition *//*
    public final static int TEMPORAL_PARTITION_OFFSET = 0;

    public final static int TEMPORAL_PARTITION_A_LENGTH = 168; // TimePeriod * 120

    public final static CurveType PARTITION_A_DEFAULT_STRATEGY = CurveType.CURVE_T1X3Y3;

    public final static int TEMPORAL_PARTITION_B_LENGTH = 168;

    public final static CurveType PARTITION_B_DEFAULT_STRATEGY = CurveType.CURVE_T1X7Y7;

    public final static CurveType DEFAULT_STRATEGY = CurveType.Z_CURVE;  // used to generate 2D curve (subspace id)

    public final static int PARTITION_LONGITUDE_GRANULARITY = SPATIAL_VIRTUAL_LONGITUDE_GRANULARITY * 32;

    public final static int PARTITION_LATITUDE_GRANULARITY = SPATIAL_VIRTUAL_LATITUDE_GRANULARITY * 32;


    *//* aggregation parameter *//*

    public final static int DIRECT_AGGREGATE_THRESHOLD = 128;  // the param of direct aggregation: 0 means does not aggregate adjacent sub scans

    public final static int AGGREGATE_SUB_SCAN_NUM_THRESHOLD = 0;  // 0 means use direct aggregation

    public final static int LOGICAL_SEEK_COST = 4;  // the param of adaptive aggregation: 0 means aggregate all sub scans

    public final static int LOGICAL_READ_ONE_BLOCK_COST = 1;  // the param of adaptive aggregation: seek << read, no aggregation

    public final static int SPLIT_THRESHOLD = 128;

    *//* bitmap parameter *//*
    public final static int BASIC_BITMAP_UP_BOUND = 4;

    public final static int EXTRA_BITMAP_WEIGHT = 32;

    public final static boolean IS_NORMALIZED = false;

    *//* curve mate parameter *//*
    public final static boolean IS_DYNAMIC_CURVE = true;

    public final static boolean IS_WITH_META = true;

    public static final String VIRTUAL_LAYER_INFO_TABLE = "geomesa_virtual_layer_info_table_nyc_test_new_from_datatable";

    public static final String CURVE_META_TABLE = "geomesa_virtual_space_metadata_table_nyc_test_new_from_datatable";*/

    /*public static final String VIRTUAL_LAYER_INFO_TABLE = "geomesa_virtual_layer_info_table_nyc_exp2_workload1";

    public static final String CURVE_META_TABLE = "geomesa_virtual_space_metadata_table_nyc_exp2_workload1";*/

    /*public static final String VIRTUAL_LAYER_INFO_TABLE = "geomesa_virtual_layer_info_table_nyc_z_txy";

    public static final String CURVE_META_TABLE = "geomesa_virtual_space_metadata_table_nyc_z_txy";*/



    /*public static final String VIRTUAL_LAYER_INFO_TABLE = "geomesa_virtual_layer_info_table_z_txy_168_168_8_8";

    public static final String CURVE_META_TABLE = "geomesa_virtual_space_metadata_table_z_txy_168_168_8_8";*/

}
