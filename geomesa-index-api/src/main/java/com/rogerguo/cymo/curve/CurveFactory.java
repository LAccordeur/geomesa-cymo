package com.rogerguo.cymo.curve;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Date 6/4/20 8:52 PM
 * @Created by rogerguo
 */
public class CurveFactory {

    private static Map<String, SpaceFillingCurve> cachedCurves = new HashMap<>();
    static {
        cachedCurves.put(CurveType.Z_CURVE.toString(), new ZCurve());
        cachedCurves.put(CurveType.Z_CURVE_TXY.toString(), new ZCurveTXY());
        cachedCurves.put(CurveType.CURVE_X3Y3T5.toString(), new CurveX3Y3T5());
    }

    @Deprecated
    public static SpaceFillingCurve createCurve(CurveType curveType) {

        switch (curveType) {
            case Z_CURVE: return new ZCurve();
            case Z_CURVE_TXY: return new ZCurveTXY();
            case HILBERT_CURVE: return new HilbertCurve();
            case HILBERT_CURVE_TXY: return new HilbertCurveTXY();
            default: return null;
        }

    }

    public static SpaceFillingCurve createCurve(CurveMeta curveMeta) {

        CurveType curveType = curveMeta.getCurveType();
        switch (curveType) {
            case Z_CURVE: if (cachedCurves.containsKey(CurveType.Z_CURVE.toString())) return cachedCurves.get(curveType.Z_CURVE.toString()); else return new ZCurve();
            case Z_CURVE_TXY: if (cachedCurves.containsKey(CurveType.Z_CURVE_TXY.toString())) return cachedCurves.get(curveType.Z_CURVE_TXY.toString()); else return new ZCurveTXY();
            case HILBERT_CURVE: return new HilbertCurve();
            case HILBERT_CURVE_TXY: return new HilbertCurveTXY();
            case CUSTOM_CURVE_XYTTXY: return new CustomCurveXYTTXY(curveMeta.getBitNumberX(), curveMeta.getBitNumberY(), curveMeta.getBitNumberT(), curveMeta.getBitNumberGroupX(), curveMeta.getBitNumberGroupY(), curveMeta.getBitNumberGroupT());
            case CUSTOM_CURVE_XYTXYT: return new CustomCurveXYTXYT(curveMeta.getBitNumberX(), curveMeta.getBitNumberY(), curveMeta.getBitNumberT(), curveMeta.getBitNumberGroupX(), curveMeta.getBitNumberGroupY(), curveMeta.getBitNumberGroupT());
            case CURVE_T1X7Y7: return new CurveT1X7Y7();
            case CURVE_X3Y3T8: return new CurveX3Y3T8();
            case CURVE_T1X3Y3: return new CurveT1X3Y3();
            case CURVE_X1Y1T8: return new CurveX1Y1T8();
            case CUSTOM_CURVE_TXY: return new CustomCurveTXY(curveMeta.getBitNumberX(), curveMeta.getBitNumberY(), curveMeta.getBitNumberT());
            case CUSTOM_CURVE_XYT: return new CustomCurveXYT(curveMeta.getBitNumberX(), curveMeta.getBitNumberY(), curveMeta.getBitNumberT());
            case CURVE_X1Y1T5: return new CurveX1Y1T5();
            case CURVE_X3Y3T5: if (cachedCurves.containsKey(CurveType.CURVE_X3Y3T5.toString())) return cachedCurves.get(curveType.CURVE_X3Y3T5.toString()); else return new ZCurve();
            case CURVE_T5X7Y7: return new CurveT5X7Y7();
            case CURVE_X7Y7T8: return new CurveX7Y7T8();
            case CURVE_T1X4Y4: return new CurveT1X4Y4();
            case CURVE_SPACE: return new SpacePreferredCurve();
            case CURVE_TIME: return new TimePreferredCurve();
            default: return null;
        }

    }

}

