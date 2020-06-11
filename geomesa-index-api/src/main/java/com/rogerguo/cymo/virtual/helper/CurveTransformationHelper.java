package com.rogerguo.cymo.virtual.helper;


import com.rogerguo.cymo.curve.CurveFactory;
import com.rogerguo.cymo.curve.CurveMeta;
import com.rogerguo.cymo.curve.CurveType;
import com.rogerguo.cymo.curve.SpaceFillingCurve;

import java.util.Map;

/**
 * @Description
 * @Author GUO Yang
 * @Date 2019-12-18 5:19 PM
 */
public class CurveTransformationHelper {

    public static Map<String, Integer> decode2D(CurveMeta curveMeta, long curveValue) {
        SpaceFillingCurve curve = CurveFactory.createCurve(curveMeta);
        return curve.from2DCurveValue(curveValue);
    }

    /**
     *
     * @param curveMeta
     * @param longitude has been normalized
     * @param latitude has been normalized
     * @return
     */
    public static long generate2D(CurveMeta curveMeta, int longitude, int latitude) {
        SpaceFillingCurve curve = CurveFactory.createCurve(curveMeta);
        return curve.getCurveValue(longitude, latitude);
    }

    public static Map<String, Integer> decode3D(CurveMeta curveMeta, long curveValue) {
        SpaceFillingCurve curve = CurveFactory.createCurve(curveMeta);
        return curve.from3DCurveValue(curveValue);
    }

    /**
     *
     *
     * @param curveMeta
     * @param longitude  has been normalized
     * @param latitude  has been normalized
     * @param timestamp has been normalized
     * @return
     */
    public static long generate3D(CurveMeta curveMeta, int longitude, int latitude, int timestamp) {
        SpaceFillingCurve curve = CurveFactory.createCurve(curveMeta);
        return curve.getCurveValue(longitude,
                latitude,
                timestamp);
    }

    @Deprecated
    public static Map<String, Integer> decode2D(CurveType curveType, long curveValue) {
        SpaceFillingCurve curve = CurveFactory.createCurve(curveType);
        return curve.from2DCurveValue(curveValue);
    }

    /**
     *
     * @param curveType
     * @param longitude has been normalized
     * @param latitude has been normalized
     * @return
     */
    @Deprecated
    public static long generate2D(CurveType curveType, int longitude, int latitude) {
        SpaceFillingCurve curve = CurveFactory.createCurve(curveType);
        return curve.getCurveValue(longitude, latitude);
    }

    @Deprecated
    public static Map<String, Integer> decode3D(CurveType curveType, long curveValue) {
        SpaceFillingCurve curve = CurveFactory.createCurve(curveType);
        return curve.from3DCurveValue(curveValue);
    }

    /**
     *
     *
     * @param curveType
     * @param longitude  has been normalized
     * @param latitude  has been normalized
     * @param timestamp has been normalized
     * @return
     */
    @Deprecated
    public static long generate3D(CurveType curveType, int longitude, int latitude, int timestamp) {
        SpaceFillingCurve curve = CurveFactory.createCurve(curveType);
        return curve.getCurveValue(longitude,
                latitude,
                timestamp);
    }



}
