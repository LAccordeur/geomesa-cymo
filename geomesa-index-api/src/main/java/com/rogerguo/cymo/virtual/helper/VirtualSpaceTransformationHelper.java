package com.rogerguo.cymo.virtual.helper;



import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.virtual.entity.CellLocation;
import com.rogerguo.cymo.virtual.entity.NormalizedLocation;
import com.rogerguo.cymo.virtual.entity.PartitionLocation;
import com.rogerguo.cymo.virtual.entity.SubspaceLocation;

import java.util.Map;


/**
 * @Description Transform a normalized location to the virtual space or transform an virtual space location to normalized space
 *
 *     normalized -> virtual transformation -> curve transformation -> key transformation
 *
 * @Author GUO Yang
 * @Date 2019-12-18 3:49 PM
 */
public class VirtualSpaceTransformationHelper {

    public static PartitionLocation toPartitionLocation(NormalizedLocation normalizedLocation) {
        return new PartitionLocation(normalizedLocation);
    }


    public static SubspaceLocation toSubspaceLocation(NormalizedLocation normalizedLocation) {
        return new SubspaceLocation(normalizedLocation);
    }

    public static CellLocation toCellLocation(NormalizedLocation normalizedLocation) {
        return new CellLocation(normalizedLocation);
    }

    public static NormalizedLocation fromSubspaceLocation(int partitionID, int subspaceLongitude, int subspaceLatitude) {

        int numberOfPartitionA;
        int numberOfPartitionB;
        if (partitionID % 2 == 0) {
            numberOfPartitionA = partitionID / 2;
            numberOfPartitionB = partitionID / 2;
        } else {
            numberOfPartitionA = partitionID / 2 + 1;
            numberOfPartitionB = partitionID / 2;
        }

        int t = VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH * numberOfPartitionA + VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH * numberOfPartitionB - VirtualLayerConfiguration.TEMPORAL_PARTITION_OFFSET;
        int x = subspaceLongitude * VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY;
        int y = subspaceLatitude * VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY;
        return new NormalizedLocation(x, y, t);
    }

    public static NormalizedLocation fromPartitionIDAndSubspaceID(int partitionID, long subspaceID) {

        int numberOfPartitionA;
        int numberOfPartitionB;
        if (partitionID % 2 == 0) {
            numberOfPartitionA = partitionID / 2;
            numberOfPartitionB = partitionID / 2;
        } else {
            numberOfPartitionA = partitionID / 2 + 1;
            numberOfPartitionB = partitionID / 2;
        }

        int t = VirtualLayerConfiguration.TEMPORAL_PARTITION_A_LENGTH * numberOfPartitionA + VirtualLayerConfiguration.TEMPORAL_PARTITION_B_LENGTH * numberOfPartitionB - VirtualLayerConfiguration.TEMPORAL_PARTITION_OFFSET;
        Map<String, Integer> resultMap = CurveTransformationHelper.decode2D(CurveType.Z_CURVE, subspaceID);
        int x = resultMap.get("x") * VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY;
        int y = resultMap.get("y") * VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY;
        return new NormalizedLocation(x, y, t);
    }

    public static long toSubspaceID(int subspaceLongitude, int subspaceLatitude) {
        return CurveTransformationHelper.generate2D(CurveType.Z_CURVE, subspaceLongitude, subspaceLatitude);
    }


    public static CellLocation getFirstCellLocationOfThisSubspace(SubspaceLocation subspaceLocation) {
        NormalizedLocation normalizedLocation = fromSubspaceLocation(subspaceLocation.getPartitionID(), subspaceLocation.getSubspaceLongitude(), subspaceLocation.getSubspaceLatitude());
        CellLocation cellLocation = toCellLocation(normalizedLocation);
        return cellLocation;
    }

    public static CellLocation getLastCellLocationOfThisSubspace(SubspaceLocation subspaceLocation) {
        NormalizedLocation normalizedLocation = fromSubspaceLocation(subspaceLocation.getPartitionID(), subspaceLocation.getSubspaceLongitude(), subspaceLocation.getSubspaceLatitude());
        int x = normalizedLocation.getX() + VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY - 1;
        int y = normalizedLocation.getY() + VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY - 1;
        int t = normalizedLocation.getT() + subspaceLocation.getNormalizedPartitionLength() - 1;
        NormalizedLocation normalizedLocationInLastCell = new NormalizedLocation(x, y, t);
        return toCellLocation(normalizedLocationInLastCell);
    }

    public static int getSubspaceLongitudeCellLength() {
        return VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY / VirtualLayerConfiguration.SPATIAL_VIRTUAL_LONGITUDE_GRANULARITY;
    }

    public static int getSubspaceLatitudeCellLength() {
        return VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY / VirtualLayerConfiguration.SPATIAL_VIRTUAL_LATITUDE_GRANULARITY;
    }

}
