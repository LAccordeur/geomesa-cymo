package com.rogerguo.client.cymo;

import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.curve.CurveFactory;
import com.rogerguo.cymo.curve.CurveMeta;
import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.curve.SpaceFillingCurve;

import java.util.*;

/**
 * @Description
 * @Date 2020/3/17 14:31
 * @Created by X1 Carbon
 */
public class EvaluateIndexingScheme {


    /*public static void main(String[] args) {
        //System.out.println(Math.ceil((Math.log(8) / Math.log(2))));
        List<QueryPattern> queryPatternList = new ArrayList<>();
        QueryPattern queryPattern1 = new QueryPattern(72, 72, 1, 0.9, 0.5*0.5);
        queryPatternList.add(queryPattern1);
        QueryPattern queryPattern2 = new QueryPattern(7, 7, 1, 0.1, 0.5*0.5);
        queryPatternList.add(queryPattern2);
        QueryPattern queryPattern3 = new QueryPattern(16, 16, 512, 0.2, 0.2);
        //queryPatternList.add(queryPattern3);

        System.out.println(evaluateIndexing(queryPatternList));
    }*/

    private static Map<CurveType, List<Long>> curveIdListMap = new HashMap<>();

    public static CurveMeta evaluateIndexing(List<QueryPattern> queryPatternList) {

        Map<Double, CurveMeta> costLayoutMap = new HashMap<>();
        double costMin = 999999999999999999.0;
        for (QueryPattern pattern : queryPatternList) {
            int bitLengthX = (int) Math.ceil((Math.log(pattern.getLongitudeWidth()) / Math.log(2)));
            int bitLengthY = (int) Math.ceil((Math.log(pattern.getLatitudeWidth()) / Math.log(2)));
            int bitLengthT = (int) Math.ceil((Math.log(pattern.getTimeWidth() == 1 ? 1+0.1 : pattern.getTimeWidth()) / Math.log(2)));

            if (bitLengthX < bitLengthT && bitLengthY < bitLengthT) {
                CurveMeta layoutGeneratorXYT = new CurveMeta(CurveType.CUSTOM_CURVE_XYT, bitLengthX, bitLengthY, bitLengthT, 0, 0, 0);
                double costXYT = estimateOneSetOfQueryPatterns(queryPatternList, layoutGeneratorXYT);
                if (costXYT <= costMin) {
                    costMin = costXYT;
                }
                costLayoutMap.put(costXYT, layoutGeneratorXYT);
                System.out.println(String.format("cost: %f, layout: %s", costXYT, layoutGeneratorXYT));
            } else {

                CurveMeta layoutGenerator = new CurveMeta(CurveType.CUSTOM_CURVE_TXY, bitLengthX, bitLengthY, bitLengthT, 0, 0, 0);
                double cost = estimateOneSetOfQueryPatterns(queryPatternList, layoutGenerator);
                if (cost <= costMin) {
                    costMin = cost;
                }
                costLayoutMap.put(cost, layoutGenerator);
                System.out.println(String.format("cost: %f, layout: %s", cost, layoutGenerator));
            }
        }


        CurveMeta layoutGeneratorZCurve = new CurveMeta(CurveType.Z_CURVE);
        double zcurveCost = estimateOneSetOfQueryPatterns(queryPatternList, layoutGeneratorZCurve);
        if (zcurveCost <= costMin) {
            costMin = zcurveCost;
        }
        costLayoutMap.put(zcurveCost, layoutGeneratorZCurve);
        System.out.println(String.format("cost: %f, layout: %s", zcurveCost, layoutGeneratorZCurve));

        CurveMeta layoutGeneratorZCurveTXY = new CurveMeta(CurveType.Z_CURVE_TXY);
        double zcurveTXYCost = estimateOneSetOfQueryPatterns(queryPatternList, layoutGeneratorZCurveTXY);
        if (zcurveTXYCost <= costMin) {
            costMin = zcurveTXYCost;
        }
        costLayoutMap.put(zcurveTXYCost, layoutGeneratorZCurveTXY);
        System.out.println(String.format("cost: %f, layout: %s", zcurveTXYCost, layoutGeneratorZCurveTXY));

        /*LayoutGenerator layoutGeneratorHilbert = new LayoutGenerator(LayoutType.HILBERT);
        double hilbertCost = estimateOneSetOfQueryPatterns(queryPatternList, layoutGeneratorHilbert);
        if (hilbertCost <= costMin) {
            costMin = hilbertCost;
        }
        costLayoutMap.put(hilbertCost, layoutGeneratorHilbert);
        System.out.println(String.format("cost: %f, layout: %s", hilbertCost, layoutGeneratorHilbert));*/


        return costLayoutMap.get(costMin);
    }


    private static double estimateOneSetOfQueryPatterns(List<QueryPattern> queryPatternList, CurveMeta layoutGenerator) {
        double cost = 0;

        for (QueryPattern pattern : queryPatternList) {
            //System.out.println(estimateOneQueryPattern(pattern, layoutGenerator));
            //System.out.println(pattern.getWeight());
            cost = cost + estimateOneQueryPattern(pattern, layoutGenerator) * pattern.getWeight();
        }

        return cost;
    }

    private static double estimateOneQueryPattern(QueryPattern pattern, CurveMeta layoutGenerator) {

        int xLength = VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY;
        int yLength = VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY;
        int tLength = 168;


        List<Long> idListInSubspace = new ArrayList<>();

        if (curveIdListMap.containsKey(layoutGenerator.getCurveType())) {
            idListInSubspace = curveIdListMap.get(layoutGenerator.getCurveType());
        } else {
            for (int i = 0; i < xLength; i++) {
                for (int j = 0; j < yLength; j++) {
                    for (int k = 0; k < tLength; k++) {
                        SpaceFillingCurve curve = CurveFactory.createCurve(layoutGenerator);
                        long value = curve.getCurveValue(i, j, k);
                        idListInSubspace.add(value);
                    }
                }
            }
            Collections.sort(idListInSubspace);
            curveIdListMap.put(layoutGenerator.getCurveType(), idListInSubspace);
        }


        List<Double> costList = new ArrayList<>();
        // execute 32 random queries of this pattern
        SpaceFillingCurve curve = CurveFactory.createCurve(layoutGenerator);
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 32; i++) {
            double cost = 0;
            List<Long> idList = new ArrayList<>();
            /*int xStart = random.nextInt((int)Math.pow(2.0, 6.0));
            int yStart = random.nextInt((int)Math.pow(2.0, 6.0));
            int tStart = random.nextInt((int)Math.pow(2.0, 8.0));*/
            int xStart = random.nextInt(xLength - pattern.getLongitudeWidth());
            int yStart = random.nextInt(yLength - pattern.getLatitudeWidth());
            int tStart = random.nextInt(tLength - pattern.getTimeWidth());
/*            int xStart = 0;
            int yStart = 0;
            int tStart = 0;*/

            int xStop = xStart + pattern.getLongitudeWidth() - 1;
            int yStop = yStart + pattern.getLatitudeWidth() - 1;
            int tStop = tStart + pattern.getTimeWidth() - 1;



            for (int x = xStart; x <= xStop; x++) {
                for (int y = yStart; y <= yStop; y++) {
                    for (int t = tStart; t <= tStop; t++) {
                        long value = curve.getCurveValue(x, y, t);
                        idList.add(value);
                    }
                }
            }
            cost = computeCost(idList, idListInSubspace);
            costList.add(cost);
        }


        return average(costList);
    }

    private static double average(List<Double> values) {
        double result = 0;
        for (double value : values) {
            result = result + value;
        }
        return result / values.size();
    }

    public static double computeCost(List<Long> cellIDList, List<Long> cellIdListInsubspace) {
        List<QuerySegment> querySegmentList = aggregateQuerySegments(cellIDList, cellIdListInsubspace);
        List<Long> intervalLengthList = new ArrayList<>();
        for (QuerySegment segment : querySegmentList) {
            if (segment.getDistanceToNextSegment() != 0) {
                intervalLengthList.add(segment.getDistanceToNextSegment());
            }
        }
        long intervalLengthSum = sum(intervalLengthList);
        double cost = 0;
        for (int i = 0; i < intervalLengthList.size(); i++) {
            long intervalLength = intervalLengthList.get(i);
            if (intervalLength != 0) {
                cost = cost - (1.0 * intervalLength / intervalLengthSum) * (Math.log(1.0 * intervalLength / intervalLengthSum));
            }
        }

        double globalCost = 0;
        for (int i = 0; i < intervalLengthList.size(); i++) {
            globalCost += intervalLengthList.get(i);
        }

        cost = cost * globalCost;

        return cost;
    }

    private static long sum(List<Long> values) {
        long sum = 0;
        for (Long value : values) {
            sum += value;
        }
        return sum;
    }

    public static List<QuerySegment> aggregateQuerySegments(List<Long> cellIDList, List<Long> cellIDListInSubspace) {
        List<QuerySegment> initSegments = generateInitQuerySegments(cellIDList);

        for (int i = 0; i < initSegments.size() - 1; i++) {
            QuerySegment currentSegment = initSegments.get(i);
            QuerySegment nextSegment = initSegments.get(i + 1);
            long currentHighBound = currentSegment.getHighBound();
            long nextLowBound = nextSegment.getLowBound();

            int beginIndex = Collections.binarySearch(cellIDListInSubspace, currentHighBound);
            int stopIndex = Collections.binarySearch(cellIDListInSubspace, nextLowBound);

            int distance = cellIDListInSubspace.subList(beginIndex, stopIndex).size();
            if (distance <= 1) {
                currentSegment.setDistanceToNextSegment(0);
            } else {
                currentSegment.setDistanceToNextSegment(distance - 1);
            }
        }

        return initSegments;
    }

    public static List<QuerySegment> generateInitQuerySegments(List<Long> cellIDList) {
        Collections.sort(cellIDList);
        List<QuerySegment> querySegmentList = new ArrayList<>();
        long currentCellID, nextCellID;
        boolean newQuerySegmentFlag = true;
        int i;
        for (i = 0; i < cellIDList.size(); i++) {
            if (newQuerySegmentFlag) {
                QuerySegment querySegment = new QuerySegment();
                querySegmentList.add(querySegment);

                currentCellID = cellIDList.get(i);
                querySegment.setLowBound(currentCellID);
                if (i != cellIDList.size() - 1) {
                    nextCellID = cellIDList.get(i + 1);
                    if (currentCellID + 1 == nextCellID) {
                        newQuerySegmentFlag = false;
                        continue;
                    } else {
                        querySegment.setHighBound(currentCellID);
                        querySegment.setDistanceToNextSegment(nextCellID - currentCellID - 1);
                        newQuerySegmentFlag = true;
                    }
                }
            } else {
                currentCellID = cellIDList.get(i);
                if (i != cellIDList.size() - 1) {
                    nextCellID = cellIDList.get(i+1);
                    if (currentCellID + 1 == nextCellID) {
                        newQuerySegmentFlag = false;
                        continue;
                    } else {
                        QuerySegment lastItem = querySegmentList.get(querySegmentList.size()-1);
                        lastItem.setHighBound(currentCellID);
                        lastItem.setDistanceToNextSegment(nextCellID - currentCellID - 1);
                        newQuerySegmentFlag = true;
                    }
                }
            }

            // handle the last cell id
            if (i == cellIDList.size() - 1) {
                QuerySegment lastItem = querySegmentList.get(querySegmentList.size()-1);
                currentCellID = cellIDList.get(i);

                if (cellIDList.size() == 1) {
                    lastItem.setHighBound(currentCellID);
                    lastItem.setDistanceToNextSegment(0);
                } else if (cellIDList.get(i-1) + 1 == currentCellID) {
                    // situation ..., 12 13
                    lastItem.setHighBound(currentCellID);
                    lastItem.setDistanceToNextSegment(0);
                } else {
                    // situation ..., 12 15
                    lastItem.setLowBound(currentCellID);
                    lastItem.setHighBound(currentCellID);
                    lastItem.setDistanceToNextSegment(0);

                }
            }
        }
        return querySegmentList;
    }

}
