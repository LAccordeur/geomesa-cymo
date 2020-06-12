package com.rogerguo.cymo.virtual.entity;


import com.rogerguo.cymo.curve.CurveMeta;
import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.virtual.helper.CurveTransformationHelper;
import com.rogerguo.cymo.virtual.helper.VirtualSpaceTransformationHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Author GUO Yang
 * @Date 2019-12-19 11:12 AM
 */
public class BoundingBox {

    private CellLocation firstCellLocation; // left down (include)

    private CellLocation lastCellLocation;  // right up (include)

    private int timeStart;  // used to imply cell coordinate offset of this bounding box in its corresponding subspace

    private int timeStop;

    private int longitudeStart;

    private int longitudeStop;

    private int latitudeStart;

    private int latitudeStop;

    public int getTimeStart() {
        return timeStart;
    }

    public void setTimeStart(int timeStart) {
        this.timeStart = timeStart;
    }

    public int getTimeStop() {
        return timeStop;
    }

    public void setTimeStop(int timeStop) {
        this.timeStop = timeStop;
    }

    public int getLongitudeStart() {
        return longitudeStart;
    }

    public void setLongitudeStart(int longitudeStart) {
        this.longitudeStart = longitudeStart;
    }

    public int getLongitudeStop() {
        return longitudeStop;
    }

    public void setLongitudeStop(int longitudeStop) {
        this.longitudeStop = longitudeStop;
    }

    public int getLatitudeStart() {
        return latitudeStart;
    }

    public void setLatitudeStart(int latitudeStart) {
        this.latitudeStart = latitudeStart;
    }

    public int getLatitudeStop() {
        return latitudeStop;
    }

    public void setLatitudeStop(int latitudeStop) {
        this.latitudeStop = latitudeStop;
    }

    public void printRangeInSubspace() {
        System.out.println(String.format("longitude range: [%d, %d], latitude range: [%d, %d], time range: [%d, %d]", longitudeStart, longitudeStop, latitudeStart, latitudeStop, timeStart, timeStop));
    }

    public BoundingBox(CellLocation firstCellLocation, CellLocation lastCellLocation) {
        this.firstCellLocation = firstCellLocation;
        this.lastCellLocation = lastCellLocation;
    }

    public BoundingBox(NormalizedRange longitudeRange, NormalizedRange latitudeRange, NormalizedRange timeRange) {
        NormalizedLocation firstNormalizedLocation = new NormalizedLocation(longitudeRange.getLowBound(), latitudeRange.getLowBound(), timeRange.getLowBound());
        NormalizedLocation lastNormalizedLocation = new NormalizedLocation(longitudeRange.getHighBound(), latitudeRange.getHighBound(), timeRange.getHighBound());
        this.firstCellLocation = VirtualSpaceTransformationHelper.toCellLocation(firstNormalizedLocation);
        this.lastCellLocation = VirtualSpaceTransformationHelper.toCellLocation(lastNormalizedLocation);
    }

    public void computeOffsetRangeInThisBoundingBox(CellLocation beginCellOfThisSubspace) {

        timeStart = firstCellLocation.getCellTime() - beginCellOfThisSubspace.getCellTime();
        timeStop = lastCellLocation.getCellTime() - beginCellOfThisSubspace.getCellTime();
        longitudeStart = firstCellLocation.getCellLongitude() - beginCellOfThisSubspace.getCellLongitude();
        longitudeStop = lastCellLocation.getCellLongitude() - beginCellOfThisSubspace.getCellLongitude();
        latitudeStart = firstCellLocation.getCellLatitude() - beginCellOfThisSubspace.getCellLatitude();
        latitudeStop = lastCellLocation.getCellLatitude() - beginCellOfThisSubspace.getCellLatitude();
    }

    public double computeFillRateOfValidRange() {
        int validRangeVolume = (longitudeStop - longitudeStart + 1) * (latitudeStop - latitudeStart + 1) * (timeStop - timeStart);
        int totalVolume = VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength() * VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength() * this.firstCellLocation.getNormalizedPartitionLength();
        return 1.0 * validRangeVolume / totalVolume;
    }

    public SubScanRange computeInitScanRangeInValidRange() {
        CurveMeta curveMeta = firstCellLocation.getCurveMeta();
        CurveType curveType = firstCellLocation.getCurveMeta().getCurveType();

        if (curveType == CurveType.HILBERT_CURVE) {
            List<Long> hilbertValueList = new ArrayList<>();
            for (int i = longitudeStart; i <= longitudeStop; i++) {
                for (int j = latitudeStart; j <= latitudeStop; j++) {
                    long value = CurveTransformationHelper.generate3D(curveMeta, i, j, timeStart);
                    hilbertValueList.add(value);
                }
            }
            for (int i = longitudeStart; i <= longitudeStop; i++) {
                for (int j = latitudeStart; j <= latitudeStop; j++) {
                    long value = CurveTransformationHelper.generate3D(curveMeta, i, j, timeStop);
                    hilbertValueList.add(value);
                }
            }
            for (int i = longitudeStart; i <= longitudeStop; i++) {
                for (int j = timeStart; j <= timeStop; j++) {
                    long value = CurveTransformationHelper.generate3D(curveMeta, i, latitudeStart, j);
                    hilbertValueList.add(value);
                }
            }
            for (int i = longitudeStart; i <= longitudeStop; i++) {
                for (int j = timeStart; j <= timeStop; j++) {
                    long value = CurveTransformationHelper.generate3D(curveMeta, i, latitudeStop, j);
                    hilbertValueList.add(value);
                }
            }
            for (int i = latitudeStart; i <= latitudeStop; i++) {
                for (int j = timeStart; j <= timeStop; j++) {
                    long value = CurveTransformationHelper.generate3D(curveMeta, longitudeStart, i, j);
                    hilbertValueList.add(value);
                }
            }
            for (int i = latitudeStart; i <= latitudeStop; i++) {
                for (int j = timeStart; j <= timeStop; j++) {
                    long value = CurveTransformationHelper.generate3D(curveMeta, longitudeStop, i, j);
                    hilbertValueList.add(value);
                }
            }

            Long maxValue = Long.MIN_VALUE;
            Long minValue = Long.MAX_VALUE;
            for (long hilbertValue : hilbertValueList) {
                if (hilbertValue > maxValue) {
                    maxValue = hilbertValue;
                }
                if (hilbertValue < minValue) {
                    minValue = hilbertValue;
                }
            }
            return new SubScanRange(minValue, maxValue);
        }

        long value1 = CurveTransformationHelper.generate3D(curveMeta, longitudeStart, latitudeStart, timeStart);
        long value2 = CurveTransformationHelper.generate3D(curveMeta, longitudeStart, latitudeStart, timeStop);
        long value3 = CurveTransformationHelper.generate3D(curveMeta, longitudeStart, latitudeStop, timeStart);
        long value4 = CurveTransformationHelper.generate3D(curveMeta, longitudeStart, latitudeStop, timeStop);
        long value5 = CurveTransformationHelper.generate3D(curveMeta, longitudeStop, latitudeStart, timeStart);
        long value6 = CurveTransformationHelper.generate3D(curveMeta, longitudeStop, latitudeStart, timeStop);
        long value7 = CurveTransformationHelper.generate3D(curveMeta, longitudeStop, latitudeStop, timeStart);
        long value8 = CurveTransformationHelper.generate3D(curveMeta, longitudeStop, latitudeStop, timeStop);
        long maxValue = Long.MIN_VALUE;
        long minValue = Long.MAX_VALUE;
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

        if (value1 < minValue) {
            minValue = value1;
        }
        if (value2 < minValue) {
            minValue = value2;
        }
        if (value3 < minValue) {
            minValue = value3;
        }
        if (value4 < minValue) {
            minValue = value4;
        }
        if (value5 < minValue) {
            minValue = value5;
        }
        if (value6 < minValue) {
            minValue = value6;
        }
        if (value7 < minValue) {
            minValue = value7;
        }
        if (value8 < minValue) {
            minValue = value8;
        }

        return new SubScanRange(minValue, maxValue);
    }

    public boolean isInBoundingBox(int longitudeOffset, int latitudeOffset, int timeOffset) {
        if (longitudeOffset >= longitudeStart && longitudeOffset <= longitudeStop
                && latitudeOffset >= latitudeStart && latitudeOffset <= latitudeStop
                && timeOffset >= timeStart && timeOffset <= timeStop) {
            return true;
        }
        return false;
    }

    public static BoundingBox computeIntersection(BoundingBox boundingBox1, BoundingBox boundingBox2) {
        CellLocation cellLocationFirst1 = boundingBox1.firstCellLocation;
        CellLocation cellLocationFirst2 = boundingBox2.firstCellLocation;

        CellLocation cellLocationLast1 = boundingBox1.lastCellLocation;
        CellLocation cellLocationLast2 = boundingBox2.lastCellLocation;

        int firstT = Math.max(cellLocationFirst1.getOriginalCellTime(), cellLocationFirst2.getOriginalCellTime());
        int firstX = Math.max(cellLocationFirst1.getOriginalCellLongitude(), cellLocationFirst2.getOriginalCellLongitude());
        int firstY = Math.max(cellLocationFirst1.getOriginalCellLatitude(), cellLocationFirst2.getOriginalCellLatitude());
        CellLocation firstCellLocation = VirtualSpaceTransformationHelper.toCellLocation(new NormalizedLocation(firstX, firstY, firstT));

        int lastT = Math.min(cellLocationLast1.getOriginalCellTime(), cellLocationLast2.getOriginalCellTime());
        int lastX = Math.min(cellLocationLast1.getOriginalCellLongitude(), cellLocationLast2.getOriginalCellLongitude());
        int lastY = Math.min(cellLocationLast1.getOriginalCellLatitude(), cellLocationLast2.getOriginalCellLatitude());
        CellLocation lastCellLocation = VirtualSpaceTransformationHelper.toCellLocation(new NormalizedLocation(lastX, lastY, lastT));

        if (firstT <= lastT && firstX <= lastX && firstY <= lastY) {
            return new BoundingBox(firstCellLocation, lastCellLocation);
        }

        return null;
    }

    public boolean isFull() {
        if (lastCellLocation.getCellTime() - firstCellLocation.getCellTime() == firstCellLocation.getNormalizedPartitionLength() - 1
                && lastCellLocation.getCellLongitude() - firstCellLocation.getCellLongitude() == VirtualSpaceTransformationHelper.getSubspaceLongitudeCellLength() - 1
                && lastCellLocation.getCellLatitude() - firstCellLocation.getCellLatitude() == VirtualSpaceTransformationHelper.getSubspaceLatitudeCellLength()- 1) {
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return "BoundingBox{" +
                "firstCellLocation=" + firstCellLocation +
                ", lastCellLocation=" + lastCellLocation +
                ", timeStart=" + timeStart +
                ", timeStop=" + timeStop +
                ", longitudeStart=" + longitudeStart +
                ", longitudeStop=" + longitudeStop +
                ", latitudeStart=" + latitudeStart +
                ", latitudeStop=" + latitudeStop +
                '}';
    }

    public CellLocation getFirstCellLocation() {
        return firstCellLocation;
    }

    public void setFirstCellLocation(CellLocation firstCellLocation) {
        this.firstCellLocation = firstCellLocation;
    }

    public CellLocation getLastCellLocation() {
        return lastCellLocation;
    }

    public void setLastCellLocation(CellLocation lastCellLocation) {
        this.lastCellLocation = lastCellLocation;
    }
}
