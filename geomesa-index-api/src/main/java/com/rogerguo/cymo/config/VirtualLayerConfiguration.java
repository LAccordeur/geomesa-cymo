package com.rogerguo.cymo.config;

import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.virtual.normalization.TimePeriod;

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
    /* Parameters of virtual layer*/
    public final static TimePeriod TEMPORAL_VIRTUAL_GRANULARITY = TimePeriod.HOUR;

    public final static int SPATIAL_VIRTUAL_LATITUDE_GRANULARITY = 16;  // uniform space

    public final static int SPATIAL_VIRTUAL_LONGITUDE_GRANULARITY = 16;  // uniform space

    /* Parameters of partition */
    public final static int TEMPORAL_PARTITION_OFFSET = 0;

    public final static int TEMPORAL_PARTITION_A_LENGTH = 168; // TimePeriod * 120

    public final static CurveType PARTITION_A_DEFAULT_STRATEGY = CurveType.Z_CURVE;

    public final static int TEMPORAL_PARTITION_B_LENGTH = 168;

    public final static CurveType PARTITION_B_DEFAULT_STRATEGY = CurveType.Z_CURVE;

    public final static CurveType DEFAULT_STRATEGY = CurveType.Z_CURVE;  // used to generate 2D curve (subspace id)

    public final static int PARTITION_LONGITUDE_GRANULARITY = SPATIAL_VIRTUAL_LONGITUDE_GRANULARITY * 32;

    public final static int PARTITION_LATITUDE_GRANULARITY = SPATIAL_VIRTUAL_LATITUDE_GRANULARITY * 32;


    /* aggregation parameter */

    public final static int DIRECT_AGGREGATE_THRESHOLD = 128;  // the param of direct aggregation: 0 means does not aggregate adjacent sub scans

    public final static int AGGREGATE_SUB_SCAN_NUM_THRESHOLD = 0;  // 0 means use direct aggregation

    public final static int LOGICAL_SEEK_COST = 4;  // the param of adaptive aggregation: 0 means aggregate all sub scans

    public final static int LOGICAL_READ_ONE_BLOCK_COST = 1;  // the param of adaptive aggregation: seek << read, no aggregation

    public final static int SPLIT_THRESHOLD = 128;

    /* bitmap parameter */
    public final static int BASIC_BITMAP_UP_BOUND = 4;

    public final static int EXTRA_BITMAP_WEIGHT = 32;

    public final static boolean IS_NORMALIZED = false;

    /* curve mate parameter */
    public final static boolean IS_DYNAMIC_CURVE = false;

}
