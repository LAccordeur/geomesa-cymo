package com.rogerguo.cymo.curve;

/**
 * @Description
 * @Date 6/4/20 8:52 PM
 * @Created by rogerguo
 */
public class CurveFactory {

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
            case Z_CURVE: return new ZCurve();
            case Z_CURVE_TXY: return new ZCurveTXY();
            case HILBERT_CURVE: return new HilbertCurve();
            case HILBERT_CURVE_TXY: return new HilbertCurveTXY();
            case CUSTOM_CURVE_XYTTXY: return new CustomCurveXYTTXY(curveMeta.getBitNumberX(), curveMeta.getBitNumberY(), curveMeta.getBitNumberT(), curveMeta.getBitNumberGroupX(), curveMeta.getBitNumberGroupY(), curveMeta.getBitNumberGroupT());
            case CUSTOM_CURVE_XYTXYT: return new CustomCurveXYTXYT(curveMeta.getBitNumberX(), curveMeta.getBitNumberY(), curveMeta.getBitNumberT(), curveMeta.getBitNumberGroupX(), curveMeta.getBitNumberGroupY(), curveMeta.getBitNumberGroupT());

            default: return null;
        }

    }

}

