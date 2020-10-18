package com.rogerguo.cymo.virtual;


import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.curve.CurveMeta;
import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.virtual.entity.SubScanItem;
import com.rogerguo.cymo.virtual.entity.SubScanRange;
import com.rogerguo.cymo.virtual.entity.SubspaceLocation;
import com.rogerguo.cymo.virtual.helper.CurveTransformationHelper;
import com.rogerguo.cymo.virtual.helper.VirtualSpaceTransformationHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @Description
 * @Author GUO Yang
 * @Date 2019-12-18 7:11 PM
 */
public class ScanOptimizerUsingOffset {

    private SubspaceLocation subspaceLocation;

    private List<SubScanItem> subScanItemList;

    private int partitionID;

    private long subspaceID;

    // for aggregate
    private List<Long> sortedCellIDListInTheSubspace;

    private List<Long> sortedCellIDListInQueryRange;

    private List<Long> extraSortedCellIDListInTheSubspace;

    //private BitmapComposition bitmapComposition;

    // for split
    private SubScanRange initLargeScanRangeInQueryRange;

    private List<Long> sortedCellIDListOutTheSubspace;

    private List<Long> extraSortedCellIDListOutTheSubspace;


    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public static int executionCount = 0;

    private boolean isFull = false; // true if full or almost full



    public ScanOptimizerUsingOffset() {}

    // only for full
    public ScanOptimizerUsingOffset(SubspaceLocation subspaceLocation) {
        this.subspaceLocation = subspaceLocation;
        this.partitionID = subspaceLocation.getPartitionID();
        this.subspaceID = VirtualSpaceTransformationHelper.toSubspaceID(subspaceLocation.getSubspaceLongitude(), subspaceLocation.getSubspaceLatitude());
        this.subScanItemList = new ArrayList<>();

        SubScanItem subScanItem = new SubScanItem();
        subScanItem.setDistanceToNextSubScan(0);
        subScanItem.setBlockNumber(-1);

        long highBound = computeHighBoundOfThisSubSpace();

        subScanItem.setSubScanRange(new SubScanRange(0, highBound));
        this.subScanItemList.add(subScanItem);
        logger.info("[Virtual Layer] full valid sub space: " + this.subScanItemList);

    }

    // only used for aggregation
    public ScanOptimizerUsingOffset(SubspaceLocation subspaceLocation, List<Long> sortedCellIDListInTheSubspace, List<Long> extraSortedCellIDListInTheSubspace, List<Long> sortedCellIDListInQueryRange) {
        this.subspaceLocation = subspaceLocation;
        this.partitionID = subspaceLocation.getPartitionID();
        this.subspaceID = VirtualSpaceTransformationHelper.toSubspaceID(subspaceLocation.getSubspaceLongitude(), subspaceLocation.getSubspaceLatitude());
        this.subScanItemList = new ArrayList<>();
        this.sortedCellIDListInTheSubspace = sortedCellIDListInTheSubspace;
        this.extraSortedCellIDListInTheSubspace = extraSortedCellIDListInTheSubspace;
        this.sortedCellIDListInQueryRange = sortedCellIDListInQueryRange;
        //this.bitmapComposition = bitmapComposition;

        if (sortedCellIDListInQueryRange.size() == 0) {
            return;
        }

        //this.aggregate();
    }

    // only used for split
    public ScanOptimizerUsingOffset(SubspaceLocation subspaceLocation, SubScanRange initLargeScanRangeInQueryRange, List<Long> sortedCellIDListOutTheSubspace, List<Long> extraSortedCellIDListOutTheSubspace) {
        this.subspaceLocation = subspaceLocation;
        this.partitionID = subspaceLocation.getPartitionID();
        this.subspaceID = VirtualSpaceTransformationHelper.toSubspaceID(subspaceLocation.getSubspaceLongitude(), subspaceLocation.getSubspaceLatitude());
        this.subScanItemList = new ArrayList<>();
        this.initLargeScanRangeInQueryRange = initLargeScanRangeInQueryRange;
        this.sortedCellIDListOutTheSubspace = sortedCellIDListOutTheSubspace;
        this.extraSortedCellIDListOutTheSubspace = extraSortedCellIDListOutTheSubspace;
    }

    // only for test (no aggregation)
    public ScanOptimizerUsingOffset(SubspaceLocation subspaceLocation, List<Long> sortedCellIDListInQueryRange) {
        this.subspaceLocation = subspaceLocation;
        this.partitionID = subspaceLocation.getPartitionID();
        this.subspaceID = VirtualSpaceTransformationHelper.toSubspaceID(subspaceLocation.getSubspaceLongitude(), subspaceLocation.getSubspaceLatitude());
        this.subScanItemList = new ArrayList<>();
        this.subScanItemList = new ArrayList<>();
        for (long cellID : sortedCellIDListInQueryRange) {
            SubScanRange subScanRange = new SubScanRange(cellID, cellID);
            SubScanItem subScanItem = new SubScanItem(subScanRange, 0, 0);
            subScanItemList.add(subScanItem);
        }
    }

    public void split() {
        logger.info("[Virtual Layer] do split");

        List<SubScanItem> outRangeSubScanItemList = generateInitSubScanItems(this.sortedCellIDListOutTheSubspace);
        logger.info(String.format("[Virtual Layer] size of sub scan that is out of range: %d", outRangeSubScanItemList.size()));
        for (SubScanItem item : outRangeSubScanItemList) {
            int blockNumber = 0;

            SubScanRange itemRange = item.getSubScanRange();
            int startIndex = Collections.binarySearch(sortedCellIDListOutTheSubspace, itemRange.getLowBound());
            int stopIndex = Collections.binarySearch(sortedCellIDListOutTheSubspace, itemRange.getHighBound());
            List<Long> cellIDSubList = sortedCellIDListOutTheSubspace.subList(startIndex, stopIndex);

            // extra bitmap
            List<Long> extraCelIDSubList = new ArrayList<>();
            if (extraSortedCellIDListOutTheSubspace != null && extraSortedCellIDListOutTheSubspace.size() > 0) {
                int extraBeginIndex = Collections.binarySearch(extraSortedCellIDListOutTheSubspace, itemRange.getLowBound());
                extraBeginIndex = extraBeginIndex >= 0 ? extraBeginIndex : -(extraBeginIndex + 1);
                int extraEndIndex = Collections.binarySearch(extraSortedCellIDListOutTheSubspace, itemRange.getHighBound());
                extraEndIndex = extraEndIndex >= 0 ? extraEndIndex + 1 : -(extraEndIndex + 1);
                extraCelIDSubList = extraSortedCellIDListOutTheSubspace.subList(extraBeginIndex, extraEndIndex);
            }

            blockNumber = blockNumber + cellIDSubList.size() + extraCelIDSubList.size() * VirtualLayerConfiguration.EXTRA_BITMAP_WEIGHT;
            item.setBlockNumber(blockNumber);
        }

        int threshold = VirtualLayerConfiguration.SPLIT_THRESHOLD;
        int count = 0;
        long previousStartID = initLargeScanRangeInQueryRange.getLowBound();
        for (int i = 0; i < outRangeSubScanItemList.size(); i++) {
            SubScanItem outRangeItem = outRangeSubScanItemList.get(i);
            if (outRangeItem.getBlockNumber() > threshold || count >= threshold) {
                SubScanRange subScanRange = new SubScanRange(previousStartID, outRangeItem.getSubScanRange().getLowBound() - 1);
                this.subScanItemList.add(new SubScanItem(subScanRange));
                count = 0;
                previousStartID = outRangeItem.getSubScanRange().getHighBound() + 1;
            } else {
                count = count + outRangeItem.getBlockNumber();
            }
        }
        this.subScanItemList.add(new SubScanItem(new SubScanRange(previousStartID, initLargeScanRangeInQueryRange.getHighBound())));
        logger.info(String.format("[Virtual Layer] size of final sub scan: %d", this.subScanItemList.size()));
        ScanOptimizer.executionCount++;
        logger.info(String.format("[Virtual Layer] Scan optimizer run %d times", executionCount));
    }

    public void aggregate() {
        logger.info("[Virtual Layer] do aggregation");

        long startTime = System.currentTimeMillis();
        List<SubScanItem> initSubScanItemList = generateInitSubScanForAggregation(this.sortedCellIDListInQueryRange);
        long stopTime = System.currentTimeMillis();
        logger.info("[Virtual Layer] init subscan list size: " + initSubScanItemList.size() + " consume " + (stopTime - startTime) / 1000.0);
        List<SubScanItem> subScanItemResultList = null;
        startTime = System.currentTimeMillis();
        if (initSubScanItemList.size() > VirtualLayerConfiguration.AGGREGATE_SUB_SCAN_NUM_THRESHOLD) {
            subScanItemResultList = directAggregateInitSubScan(initSubScanItemList);
            logger.info("[Virtual Layer] direct aggregation");
        } else {
            //subScanItemResultList = aggregateInitSubScan(initSubScanItemList);
            subScanItemResultList = aggregateInitSubScanByEstimatedFunction(initSubScanItemList);
            logger.info("[Virtual Layer] optimized aggregation");
        }
        stopTime = System.currentTimeMillis();
        this.subScanItemList = subScanItemResultList;
        logger.info("[Virtual Layer] aggregated subscan list size: " + subScanItemResultList.size() + " consume " + (stopTime - startTime) / 1000.0);

        ScanOptimizer.executionCount++;
        logger.info(String.format("[Virtual Layer] Scan optimizer run %d times", executionCount));
    }

    /**
     * for large sub scan item list
     * @return
     */
    public List<SubScanItem> directAggregateInitSubScan(List<SubScanItem> initSubScanItemList) {

        List<SubScanItem> aggregatedSubScanList = new ArrayList<>();
        boolean isInPreviousSubScan = false;
        for (int i = 0; i < initSubScanItemList.size(); i++) {
            if ((i+1) < initSubScanItemList.size()) {
                SubScanItem currentSubScan = initSubScanItemList.get(i);
                SubScanItem nextSubScan = initSubScanItemList.get(i+1);
                if (currentSubScan.getDistanceToNextSubScan() < VirtualLayerConfiguration.DIRECT_AGGREGATE_THRESHOLD) {
                    /*if (aggregatedSubScanList.size() == 0) {
                        aggregatedSubScanList.add(new SubScanItem(new SubScanRange(currentSubScan.getSubScanRange().getLowBound(), currentSubScan.getSubScanRange().getHighBound()), currentSubScan.getBlockNumber()));
                    }*/
                    if (isInPreviousSubScan) {
                        SubScanItem aggregatedSubScan = aggregatedSubScanList.get(aggregatedSubScanList.size() - 1);
                        SubScanRange aggregatedSubScanRange = aggregatedSubScan.getSubScanRange();
                        aggregatedSubScanRange.setHighBound(nextSubScan.getSubScanRange().getHighBound());
                        aggregatedSubScan.setBlockNumber(aggregatedSubScan.getBlockNumber() + currentSubScan.getDistanceToNextSubScan() + nextSubScan.getBlockNumber());
                        isInPreviousSubScan = true;
                    } else {
                        int blockNumber = currentSubScan.getBlockNumber() + currentSubScan.getDistanceToNextSubScan() + nextSubScan.getBlockNumber();
                        SubScanItem aggregatedSubScan = new SubScanItem(new SubScanRange(currentSubScan.getSubScanRange().getLowBound(), nextSubScan.getSubScanRange().getHighBound()), blockNumber);
                        aggregatedSubScanList.add(aggregatedSubScan);
                        isInPreviousSubScan = true;
                    }
                } else {
                    SubScanItem subScanItem;
                    if (!isInPreviousSubScan) {
                        subScanItem = new SubScanItem(new SubScanRange(currentSubScan.getSubScanRange().getLowBound(), currentSubScan.getSubScanRange().getHighBound()), currentSubScan.getBlockNumber());
                        isInPreviousSubScan = false;
                    } else {
                        subScanItem = new SubScanItem(new SubScanRange(nextSubScan.getSubScanRange().getLowBound(), nextSubScan.getSubScanRange().getHighBound()), nextSubScan.getBlockNumber());
                        isInPreviousSubScan = true;
                    }
                    aggregatedSubScanList.add(subScanItem);

                }
            }
        }

        // check the last one
        if (aggregatedSubScanList.size() == 0 || aggregatedSubScanList.get(aggregatedSubScanList.size()-1).getSubScanRange().getHighBound() < initSubScanItemList.get(initSubScanItemList.size()-1).getSubScanRange().getHighBound()) {
            if (!isInPreviousSubScan) {
                aggregatedSubScanList.add(initSubScanItemList.get(initSubScanItemList.size() - 1));
            } else {
                SubScanItem currentSubScanItem = initSubScanItemList.get(initSubScanItemList.size() - 1);
                SubScanItem aggregatedSubScan = aggregatedSubScanList.get(aggregatedSubScanList.size() - 1);
                SubScanRange aggregatedSubScanRange = aggregatedSubScan.getSubScanRange();
                aggregatedSubScanRange.setHighBound(currentSubScanItem.getSubScanRange().getHighBound());
                aggregatedSubScan.setBlockNumber(aggregatedSubScan.getBlockNumber() + currentSubScanItem.getBlockNumber());

            }
        }

        return aggregatedSubScanList;
    }

    /**
     * for small sub scan item list
     * @param initSubScanItemList
     * @return
     */
    public List<SubScanItem> aggregateInitSubScan(List<SubScanItem> initSubScanItemList) {
        List<SubScanItem> resultSubScanItemList = new ArrayList<>();
        List<Integer> costList = new ArrayList<>();
        for (int i = 0; i < initSubScanItemList.size(); i++) {
            costList.add(Integer.MAX_VALUE);
        }

        List<Integer> positionList = new ArrayList<>();
        for (int i = 0; i < initSubScanItemList.size(); i++) {
            positionList.add(Integer.MAX_VALUE);
        }


        for (int i = 0; i < initSubScanItemList.size(); i++) {
            for (int j = 0; j <= i; j++) {
                int k = i - j;
                int cost = 0;
                if (k > 0) {
                    cost = estimate(initSubScanItemList, k, i) + costList.get(k - 1);
                } else {
                    cost = estimate(initSubScanItemList, k, i);
                }
                if (cost < costList.get(i)) {
                    costList.set(i, cost);
                    positionList.set(i, k);
                }
            }
        }

        int m = initSubScanItemList.size() - 1, position;
        while (m >= 0) {
            position = positionList.get(m);
            long lowBound = initSubScanItemList.get(position).getSubScanRange().getLowBound();
            long highBound = initSubScanItemList.get(m).getSubScanRange().getHighBound();
            resultSubScanItemList.add(new SubScanItem(new SubScanRange(lowBound, highBound)));
            m = position - 1;
        }

        Collections.reverse(resultSubScanItemList);


        List<Long> basicVirtualLayerIDList = this.sortedCellIDListInTheSubspace;
        // compute the "block" number on the virtual layer
        for (SubScanItem item : resultSubScanItemList) {
            SubScanRange itemRange = item.getSubScanRange();
            int blockNumber = 0;

            int beginIndex = basicVirtualLayerIDList.indexOf(itemRange.getLowBound());
            int endIndex = basicVirtualLayerIDList.indexOf(itemRange.getHighBound());
            List<Long> cellIDSubList = basicVirtualLayerIDList.subList(beginIndex, endIndex+1);

            /*for (long cellID : cellIDSubList) {
                blockNumber = blockNumber + getCountFromBitMap(subspaceLocation.getCurveType(), cellID, bitmapComposition.getBasicBitSetList(), bitmapComposition.getExtraBitSetList());;
            }*/

            List<Long> extraCelIDSubList = new ArrayList<>();
            if (extraSortedCellIDListInTheSubspace != null && extraSortedCellIDListInTheSubspace.size() > 0) {
                int extraBeginIndex = Collections.binarySearch(extraSortedCellIDListInTheSubspace, itemRange.getLowBound());
                extraBeginIndex = extraBeginIndex >= 0 ? extraBeginIndex : -(extraBeginIndex + 1);
                int extraEndIndex = Collections.binarySearch(extraSortedCellIDListInTheSubspace, itemRange.getHighBound());
                extraEndIndex = extraEndIndex >= 0 ? extraEndIndex + 1 : -(extraEndIndex + 1);
                extraCelIDSubList = extraSortedCellIDListInTheSubspace.subList(extraBeginIndex, extraEndIndex);
            }

            blockNumber = blockNumber + cellIDSubList.size() * 1 + extraCelIDSubList.size() * 1000;
            item.setBlockNumber(blockNumber);

            item.setBlockNumber(blockNumber);
        }

        return resultSubScanItemList;

    }

    public int estimate(List<SubScanItem> subScanItemList, int start, int stop) {
        //int seekCost = 16;
        //readOneBlockCost = 2;
        int seekCost = VirtualLayerConfiguration.LOGICAL_SEEK_COST;
        int readOneBlockCost = VirtualLayerConfiguration.LOGICAL_READ_ONE_BLOCK_COST;

        if (start == stop) {
            SubScanItem item = subScanItemList.get(start);
            return (int) (seekCost + readOneBlockCost * item.getBlockNumber());
        } else {
            int cost = 0;

            int totalBlockNumber = 0;
            for (int i = start; i <= stop; i++) {
                SubScanItem item = subScanItemList.get(i);
                if (i != stop) {
                    totalBlockNumber = totalBlockNumber + item.getBlockNumber() + item.getDistanceToNextSubScan();
                } else {
                    totalBlockNumber = totalBlockNumber + item.getBlockNumber();
                }
            }
            cost = (int) (seekCost + readOneBlockCost * totalBlockNumber);

            return cost;
        }

    }


    public List<SubScanItem> directAggregateInitSubScaForAdaptive(List<SubScanItem> initSubScanItemList, int threshold) {

        List<SubScanItem> aggregatedSubScanList = new ArrayList<>();
        boolean isInPreviousSubScan = false;
        for (int i = 0; i < initSubScanItemList.size(); i++) {
            if ((i+1) < initSubScanItemList.size()) {
                SubScanItem currentSubScan = initSubScanItemList.get(i);
                SubScanItem nextSubScan = initSubScanItemList.get(i+1);
                if (currentSubScan.getDistanceToNextSubScan() <= threshold) {
                    /*if (aggregatedSubScanList.size() == 0) {
                        aggregatedSubScanList.add(new SubScanItem(new SubScanRange(currentSubScan.getSubScanRange().getLowBound(), currentSubScan.getSubScanRange().getHighBound()), currentSubScan.getBlockNumber()));
                    }*/
                    if (isInPreviousSubScan) {
                        SubScanItem aggregatedSubScan = aggregatedSubScanList.get(aggregatedSubScanList.size() - 1);
                        SubScanRange aggregatedSubScanRange = aggregatedSubScan.getSubScanRange();
                        aggregatedSubScanRange.setHighBound(nextSubScan.getSubScanRange().getHighBound());
                        aggregatedSubScan.setBlockNumber(aggregatedSubScan.getBlockNumber() + currentSubScan.getDistanceToNextSubScan() + nextSubScan.getBlockNumber());
                        isInPreviousSubScan = true;
                    } else {
                        int blockNumber = currentSubScan.getBlockNumber() + currentSubScan.getDistanceToNextSubScan() + nextSubScan.getBlockNumber();
                        SubScanItem aggregatedSubScan = new SubScanItem(new SubScanRange(currentSubScan.getSubScanRange().getLowBound(), nextSubScan.getSubScanRange().getHighBound()), blockNumber);
                        aggregatedSubScanList.add(aggregatedSubScan);
                        isInPreviousSubScan = true;
                    }
                } else {
                    SubScanItem subScanItem;
                    if (!isInPreviousSubScan) {
                        subScanItem = new SubScanItem(new SubScanRange(currentSubScan.getSubScanRange().getLowBound(), currentSubScan.getSubScanRange().getHighBound()), currentSubScan.getBlockNumber());
                        isInPreviousSubScan = false;
                    } else {
                        subScanItem = new SubScanItem(new SubScanRange(nextSubScan.getSubScanRange().getLowBound(), nextSubScan.getSubScanRange().getHighBound()), nextSubScan.getBlockNumber());
                        isInPreviousSubScan = true;
                    }
                    aggregatedSubScanList.add(subScanItem);

                }
            }
        }

        // check the last one
        if (aggregatedSubScanList.size() == 0 || aggregatedSubScanList.get(aggregatedSubScanList.size()-1).getSubScanRange().getHighBound() < initSubScanItemList.get(initSubScanItemList.size()-1).getSubScanRange().getHighBound()) {
            if (!isInPreviousSubScan) {
                aggregatedSubScanList.add(initSubScanItemList.get(initSubScanItemList.size() - 1));
            } else {
                SubScanItem currentSubScanItem = initSubScanItemList.get(initSubScanItemList.size() - 1);
                SubScanItem aggregatedSubScan = aggregatedSubScanList.get(aggregatedSubScanList.size() - 1);
                SubScanRange aggregatedSubScanRange = aggregatedSubScan.getSubScanRange();
                aggregatedSubScanRange.setHighBound(currentSubScanItem.getSubScanRange().getHighBound());
                aggregatedSubScan.setBlockNumber(aggregatedSubScan.getBlockNumber() + currentSubScanItem.getBlockNumber());

            }
        }

        return aggregatedSubScanList;
    }

    public List<SubScanItem> aggregateInitSubScanByEstimatedFunction(List<SubScanItem> originalInitSubScanItemList) {

        List<SubScanItem> initSubScanItemList = directAggregateInitSubScaForAdaptive(originalInitSubScanItemList, 16);

        List<SubScanItem> resultSubScanItemList = new ArrayList<>();
        List<Double> costList = new ArrayList<>();
        for (double i = 0; i < initSubScanItemList.size(); i++) {
            costList.add(Double.MAX_VALUE);
        }

        List<Integer> positionList = new ArrayList<>();
        for (int i = 0; i < initSubScanItemList.size(); i++) {
            positionList.add(Integer.MAX_VALUE);
        }

        List<OffsetBlockPair> offsetList = transferSubScan2BlockOffset(initSubScanItemList);
        for (int i = 0; i < offsetList.size(); i++) {
            for (int j = 0; j <= i; j++) {
                int k = i - j;
                double cost = 0;
                if (k > 0) {
                    cost = estimateFromBlockOffset(offsetList, k, i) + costList.get(k - 1);
                } else {
                    cost = estimateFromBlockOffset(offsetList, k, i);
                }
                if (cost < costList.get(i)) {
                    costList.set(i, cost);
                    positionList.set(i, k);
                }
            }
        }

        int m = initSubScanItemList.size() - 1, position;
        while (m >= 0) {
            position = positionList.get(m);
            long lowBound = initSubScanItemList.get(position).getSubScanRange().getLowBound();
            long highBound = initSubScanItemList.get(m).getSubScanRange().getHighBound();
            resultSubScanItemList.add(new SubScanItem(new SubScanRange(lowBound, highBound)));
            m = position - 1;
        }

        Collections.reverse(resultSubScanItemList);


        List<Long> basicVirtualLayerIDList = this.sortedCellIDListInTheSubspace;
        // compute the "block" number on the virtual layer
        for (SubScanItem item : resultSubScanItemList) {
            SubScanRange itemRange = item.getSubScanRange();
            int blockNumber = 0;

            int beginIndex = basicVirtualLayerIDList.indexOf(itemRange.getLowBound());
            int endIndex = basicVirtualLayerIDList.indexOf(itemRange.getHighBound());
            List<Long> cellIDSubList = basicVirtualLayerIDList.subList(beginIndex, endIndex+1);

            /*for (long cellID : cellIDSubList) {
                blockNumber = blockNumber + getCountFromBitMap(subspaceLocation.getCurveType(), cellID, bitmapComposition.getBasicBitSetList(), bitmapComposition.getExtraBitSetList());;
            }*/

            List<Long> extraCelIDSubList = new ArrayList<>();
            if (extraSortedCellIDListInTheSubspace != null && extraSortedCellIDListInTheSubspace.size() > 0) {
                int extraBeginIndex = Collections.binarySearch(extraSortedCellIDListInTheSubspace, itemRange.getLowBound());
                extraBeginIndex = extraBeginIndex >= 0 ? extraBeginIndex : -(extraBeginIndex + 1);
                int extraEndIndex = Collections.binarySearch(extraSortedCellIDListInTheSubspace, itemRange.getHighBound());
                extraEndIndex = extraEndIndex >= 0 ? extraEndIndex + 1 : -(extraEndIndex + 1);
                extraCelIDSubList = extraSortedCellIDListInTheSubspace.subList(extraBeginIndex, extraEndIndex);
            }

            blockNumber = blockNumber + cellIDSubList.size() * 1 + extraCelIDSubList.size() * 256;
            item.setBlockNumber(blockNumber);

            item.setBlockNumber(blockNumber);
        }

        return resultSubScanItemList;

    }


    public double estimateFromBlockOffset(List<OffsetBlockPair> offsetList, int start, int stop) {
        int startOffset = offsetList.get(start).getOffset();
        int stopOffset = offsetList.get(stop).getOffset();
        int stopBlockNumber = offsetList.get(stop).getBlockNumber();
        return estimatedFunction(stopOffset + stopBlockNumber - startOffset);

    }

    public static double estimatedFunction(int recordOffset) {

        if (recordOffset < 64) {
            return 0.000001 * 10;
        }

        return 0.000001 * (recordOffset + 10);
    }

    class OffsetBlockPair {
        private int offset;

        private int blockNumber;

        public OffsetBlockPair(int offset, int blockNumber) {
            this.offset = offset;
            this.blockNumber = blockNumber;
        }

        public int getOffset() {
            return offset;
        }

        public void setOffset(int offset) {
            this.offset = offset;
        }

        public int getBlockNumber() {
            return blockNumber;
        }

        public void setBlockNumber(int blockNumber) {
            this.blockNumber = blockNumber;
        }
    }

    public List<OffsetBlockPair> transferSubScan2BlockOffset(List<SubScanItem> subScanItemList) {
        List<OffsetBlockPair> offsetList = new ArrayList<>();


        for (int i = 0; i < subScanItemList.size(); i++) {

            if (i == 0) {
                offsetList.add(new OffsetBlockPair(0, subScanItemList.get(i).getBlockNumber()));
            } else {
                int offset = offsetList.get(i - 1).getOffset() + subScanItemList.get(i -1).getBlockNumber() + subScanItemList.get(i - 1).getDistanceToNextSubScan();
                offsetList.add(new OffsetBlockPair(offset, subScanItemList.get(i).getBlockNumber()));
            }
        }

        return offsetList;
    }

    /**
     *  1, 2, 4, 5, 6, 9, 10 -> (1, 2), (4, 5, 6), (9, 10)
     * @param cellIDList
     * @return
     */
    public List<SubScanItem> generateInitSubScanForAggregation(List<Long> cellIDList) {
        // aggregate the cell id
        List<SubScanItem> subScanItemList = generateInitSubScanItems(cellIDList);

        List<Long> basicVirtualLayerIDList = this.sortedCellIDListInTheSubspace;
        // compute the "block" number on the virtual layer
        for (int n = 0; n < subScanItemList.size(); n++) {
            SubScanItem item = subScanItemList.get(n);
            SubScanRange itemRange = item.getSubScanRange();
            int blockNumber = 0;
            int distanceToNextSubscan = 0;


            int beginIndex = Collections.binarySearch(basicVirtualLayerIDList, itemRange.getLowBound());
            int endIndex = Collections.binarySearch(basicVirtualLayerIDList, itemRange.getHighBound());
            List<Long> cellIDSubList = basicVirtualLayerIDList.subList(beginIndex, endIndex+1);

            /*for (long cellID : cellIDSubList) {
                blockNumber = blockNumber + getCountFromBitMap(subspaceLocation.getCurveType(), cellID, bitmapComposition.getBasicBitSetList(), bitmapComposition.getExtraBitSetList());
            }*/

            // extra bitmap
            List<Long> extraCelIDSubList = new ArrayList<>();
            if (extraSortedCellIDListInTheSubspace != null && extraSortedCellIDListInTheSubspace.size() > 0) {
                int extraBeginIndex = Collections.binarySearch(extraSortedCellIDListInTheSubspace, itemRange.getLowBound());
                extraBeginIndex = extraBeginIndex >= 0 ? extraBeginIndex : -(extraBeginIndex + 1);
                int extraEndIndex = Collections.binarySearch(extraSortedCellIDListInTheSubspace, itemRange.getHighBound());
                extraEndIndex = extraEndIndex >= 0 ? extraEndIndex + 1 : -(extraEndIndex + 1);
                extraCelIDSubList = extraSortedCellIDListInTheSubspace.subList(extraBeginIndex, extraEndIndex);
            }

            blockNumber = blockNumber + cellIDSubList.size() * 1 + extraCelIDSubList.size() * VirtualLayerConfiguration.EXTRA_BITMAP_WEIGHT;
            item.setBlockNumber(blockNumber);

            if ((n+1) < subScanItemList.size()) {
                SubScanItem nextItem = subScanItemList.get(n+1);
                SubScanRange nextItemRange = nextItem.getSubScanRange();
                int nextBeginIndex = Collections.binarySearch(basicVirtualLayerIDList, itemRange.getHighBound());
                int nextEndIndex = Collections.binarySearch(basicVirtualLayerIDList, nextItemRange.getLowBound());
                List<Long> toNextSubScanIDSubList = basicVirtualLayerIDList.subList(nextBeginIndex+1, nextEndIndex);


                List<Long> extraNextCellIDList = new ArrayList<>();
                if (extraSortedCellIDListInTheSubspace != null && extraSortedCellIDListInTheSubspace.size() > 0) {
                    int extraNextBeginIndex = Collections.binarySearch(extraSortedCellIDListInTheSubspace, itemRange.getHighBound());
                    extraNextBeginIndex = extraNextBeginIndex >= 0 ? extraNextBeginIndex + 1 : -(extraNextBeginIndex + 1);
                    int extraNextEndIndex = Collections.binarySearch(extraSortedCellIDListInTheSubspace, nextItemRange.getLowBound());
                    extraNextEndIndex = extraNextEndIndex >= 0 ? extraNextEndIndex : -(extraNextEndIndex + 1);

                    extraNextCellIDList = extraSortedCellIDListInTheSubspace.subList(extraNextBeginIndex, extraNextEndIndex);
                }
                /*for (long nextID : toNextSubScanIDSubList) {
                    distanceToNextSubscan = distanceToNextSubscan + getCountFromBitMap(subspaceLocation.getCurveType(), nextID, bitmapComposition.getBasicBitSetList(), bitmapComposition.getExtraBitSetList());
                }*/
                distanceToNextSubscan = distanceToNextSubscan + toNextSubScanIDSubList.size() * 1 + extraNextCellIDList.size() * VirtualLayerConfiguration.EXTRA_BITMAP_WEIGHT;
                item.setDistanceToNextSubScan(distanceToNextSubscan);
            }
        }

        return subScanItemList;
    }


    private List<SubScanItem> generateInitSubScanItems(List<Long> cellIDList) {
        List<SubScanItem> subScanItemList = new ArrayList<>();
        long currentCellID, nextCellID;
        boolean newSubScanFlag = true;
        int i;
        for (i = 0; i < cellIDList.size(); i++) {
            if (newSubScanFlag) {
                SubScanRange subScanRange = new SubScanRange();
                SubScanItem subScanItem = new SubScanItem();
                subScanItem.setSubScanRange(subScanRange);
                subScanItemList.add(subScanItem);

                currentCellID = cellIDList.get(i);
                subScanRange.setLowBound(currentCellID);
                if (i != cellIDList.size() - 1) {
                    nextCellID = cellIDList.get(i + 1);
                    if (currentCellID + 1 == nextCellID) {
                        newSubScanFlag = false;
                        continue;
                    } else {
                        subScanRange.setHighBound(currentCellID);
                        newSubScanFlag = true;
                    }
                }
            } else {
                currentCellID = cellIDList.get(i);
                if (i != cellIDList.size() - 1) {
                    nextCellID = cellIDList.get(i+1);
                    if (currentCellID + 1 == nextCellID) {
                        newSubScanFlag = false;
                        continue;
                    } else {
                        SubScanItem lastItem = subScanItemList.get(subScanItemList.size()-1);
                        lastItem.getSubScanRange().setHighBound(currentCellID);
                        newSubScanFlag = true;
                    }
                }
            }

            // handle the last cell id
            if (i == cellIDList.size() - 1) {
                SubScanItem lastItem = subScanItemList.get(subScanItemList.size()-1);
                currentCellID = cellIDList.get(i);

                if (cellIDList.size() == 1) {
                    lastItem.getSubScanRange().setHighBound(currentCellID);
                    lastItem.setDistanceToNextSubScan(0);
                } else if (cellIDList.get(i-1) + 1 == currentCellID) {
                    // situation ..., 12 13
                    lastItem.getSubScanRange().setHighBound(currentCellID);
                } else {
                    // situation ..., 12 15
                    SubScanRange subScanRange = lastItem.getSubScanRange();

                    subScanRange.setLowBound(currentCellID);
                    subScanRange.setHighBound(currentCellID);
                    lastItem.setDistanceToNextSubScan(0);
                }
            }
        }
        return subScanItemList;
    }


/*
    private static int getCountFromBitMap(CurveType curveType, long cellID, List<BitSet> basicBitSetList, List<BitSet> extraBitSetList) {
        Map<String, Integer> result = CurveTransformationHelper.decode3D(curveType, cellID);
        int bitmapIndex = BitmapHelper.coordinate2D2BitmapIndex(result.get("x"), result.get("y"));
        if (basicBitSetList.get(result.get("z")).get(bitmapIndex)) {
            if (extraBitSetList.get(result.get("z")).get(bitmapIndex)) {
                return 1000;
            }
            return 1;
        }
        return 0;
    }
*/

    /**
     *
     * @return
     */
    private long computeHighBoundOfThisSubSpace() {
        CurveMeta curveMeta = subspaceLocation.getCurveMeta();
        int longitudeCellOffset = VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength()-1;
        int latitudeCellOffset = VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength()-1;
        int timeCellOffset = subspaceLocation.getNormalizedPartitionLength()-1;
        long value1 = CurveTransformationHelper.generate3D(curveMeta, longitudeCellOffset, latitudeCellOffset, timeCellOffset);
        long value2 = CurveTransformationHelper.generate3D(curveMeta, longitudeCellOffset, latitudeCellOffset, 0);
        long value3 = CurveTransformationHelper.generate3D(curveMeta, longitudeCellOffset, 0, timeCellOffset);
        long value4 = CurveTransformationHelper.generate3D(curveMeta, 0, latitudeCellOffset, timeCellOffset);
        long value5 = CurveTransformationHelper.generate3D(curveMeta, 0, 0, timeCellOffset);
        long value6 = CurveTransformationHelper.generate3D(curveMeta, 0, latitudeCellOffset, 0);
        long value7 = CurveTransformationHelper.generate3D(curveMeta, longitudeCellOffset, 0, 0);
        long value8 = CurveTransformationHelper.generate3D(curveMeta, 0, 0, 0);
        long maxValue = Long.MIN_VALUE;
        if (value1 > maxValue) {
            maxValue = value1;
        }
        if (value2 > maxValue) {
            maxValue = value2;
        }
        if (value3 > maxValue) {
            maxValue = value3;
        }
        if (value4 > maxValue) {
            maxValue = value4;
        }
        if (value5 > maxValue) {
            maxValue = value5;
        }
        if (value6 > maxValue) {
            maxValue = value6;
        }
        if (value7 > maxValue) {
            maxValue = value7;
        }
        if (value8 > maxValue) {
            maxValue = value8;
        }
        return maxValue;
    }

    public List<SubScanItem> getSubScanItemList() {
        return subScanItemList;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public long getSubspaceID() {
        return subspaceID;
    }

    public boolean isFull() {
        return isFull;
    }
}
