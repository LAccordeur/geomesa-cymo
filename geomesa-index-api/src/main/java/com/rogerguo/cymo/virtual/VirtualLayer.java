package com.rogerguo.cymo.virtual;

import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.entity.SpatialRange;
import com.rogerguo.cymo.entity.TimeRange;
import com.rogerguo.cymo.hbase.RowKeyHelper;
import com.rogerguo.cymo.hbase.RowKeyItem;
import com.rogerguo.cymo.virtual.entity.*;
import com.rogerguo.cymo.virtual.helper.CurveTransformationHelper;
import com.rogerguo.cymo.virtual.helper.NormalizedDimensionHelper;
import com.rogerguo.cymo.virtual.helper.VirtualSpaceTransformationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Description
 * @Date 6/5/20 4:07 PM
 * @Created by rogerguo
 */
public class VirtualLayer {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public List<SubScanRangePair> getRanges(SpatialRange longitudeRange, SpatialRange latitudeRange, TimeRange timeRange) {
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

        // TODO get bitmap for each subspace

        // for each subspace, generate optimized scan range
        List<SubScanRangePair> ranges = new ArrayList<>();
        for (RowKeyItem subspaceInfo : subspaceInfoList) {
            ScanOptimizer scanOptimizer = optimizeScanRange(normalizedLongitudeRange, normalizedLatitudeRange, normalizedTimeRange, subspaceInfo, null, null);
            List<SubScanItem> subScanItems = scanOptimizer.getSubScanItemList();
            int partitionID = scanOptimizer.getPartitionID();
            long subspaceID = scanOptimizer.getSubspaceID();
            for (SubScanItem subScanItem : subScanItems) {
                SubScanRange scanRange = subScanItem.getSubScanRange();
                SubScanRangePair rangePair = new SubScanRangePair(partitionID, subspaceID, scanRange.getLowBound(), scanRange.getHighBound());
                ranges.add(rangePair);
            }
        }

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

    private ScanOptimizer optimizeScanRange(NormalizedRange longitudeRange, NormalizedRange latitudeRange, NormalizedRange timeRange, RowKeyItem subspaceInfo, String basicBitmap, String extraBitmap) {
        int partitionID = subspaceInfo.getPartitionID();
        long subspaceID = subspaceInfo.getSubspaceID();
        BoundingBox queryRangeBoundBox = new BoundingBox(longitudeRange, latitudeRange, timeRange);

        if (basicBitmap == null || extraBitmap == null) {
            SubspaceLocation subspaceLocation = VirtualSpaceTransformationHelper.toSubspaceLocation(VirtualSpaceTransformationHelper.fromPartitionIDAndSubspaceID(partitionID, subspaceID));

            CellLocation beginCellOfThisSubspace = VirtualSpaceTransformationHelper.getFirstCellLocationOfThisSubspace(subspaceLocation);
            BoundingBox validQueryRange = computeValidQueryRangeInThisSubspace(subspaceLocation, queryRangeBoundBox);
            validQueryRange.computeOffsetRangeInThisBoundingBox(beginCellOfThisSubspace);

            //logger.info(String.format("[Virtual Layer] valid query range: %s", validQueryRange));

            double fillRateOfValidRange = validQueryRange.computeFillRateOfValidRange();
            logger.info(String.format("[Virtual Layer] Fill Rate: %f in (Partition ID: %d, Subspace ID: %d, CurveMeta: %s)", fillRateOfValidRange, partitionID, subspaceID, subspaceLocation.getCurveMeta().toString()));

            if (fillRateOfValidRange <= 0.5) {
                return optimizeByAggregationWithoutMeta(subspaceLocation, validQueryRange);
            } else if (fillRateOfValidRange < 0.99) {
                return optimizeBySplitWithoutMeta(subspaceLocation, validQueryRange);
            } else {
                ScanOptimizer scanOptimizer = new ScanOptimizer(subspaceLocation);
                return scanOptimizer;
            }

        } else {
            return null;
        }
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
        for (int n = 0; n < subspaceLocation.getNormalizedPartitionLength(); n++) {

            for (int i = 0; i < VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength(); i++) {
                for (int j = 0; j < VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength(); j++) {

                    Long id = CurveTransformationHelper.generate3D(subspaceLocation.getCurveMeta(), i, j, n);
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

    private BoundingBox computeValidQueryRangeInThisSubspace(SubspaceLocation subspaceLocation, BoundingBox queryRangeBoundBox) {
        CellLocation beginCellOfThisSubspace = VirtualSpaceTransformationHelper.getFirstCellLocationOfThisSubspace(subspaceLocation);
        BoundingBox subspaceBoundBox = new BoundingBox(beginCellOfThisSubspace, VirtualSpaceTransformationHelper.getLastCellLocationOfThisSubspace(subspaceLocation));
        BoundingBox validQueryRange = BoundingBox.computeIntersection(subspaceBoundBox, queryRangeBoundBox);
        return validQueryRange;
    }

}
