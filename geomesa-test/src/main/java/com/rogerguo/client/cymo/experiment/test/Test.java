package com.rogerguo.client.cymo.experiment.test;

import com.rogerguo.cymo.curve.*;

/**
 * @Description
 * @Date 2021/3/26 21:48
 * @Created by X1 Carbon
 */
public class Test {
    public static void main(String[] args) {
        CustomCurveXYT customCurveX3Y3T8 = new CustomCurveXYT(3, 3, 8);
        CustomCurveXYT customCurveX1Y1T8 = new CustomCurveXYT(1, 1, 8);
        CurveX3Y3T8 curveX3Y3T8 = new CurveX3Y3T8();
        CurveX1Y1T8 curveX1Y1T8 = new CurveX1Y1T8();
        System.out.println(customCurveX3Y3T8.getCurveValue(12,14,17));
        System.out.println(curveX3Y3T8.getCurveValue(12,14,17));
        System.out.println(customCurveX1Y1T8.getCurveValue(12,14,17));
        System.out.println(curveX1Y1T8.getCurveValue(12,14,17));

        CustomCurveTXY customCurveT1X3Y3 = new CustomCurveTXY(1, 3, 3);
        CustomCurveTXY customCurveT1X7Y7 = new CustomCurveTXY(1, 7, 7);
        CurveT1X3Y3 curveT1X3Y3 = new CurveT1X3Y3();
        CurveT1X7Y7 curveT1X7Y7 = new CurveT1X7Y7();
        System.out.println(customCurveT1X3Y3.getCurveValue(22, 21,12));
        System.out.println(curveT1X3Y3.getCurveValue(22, 21,12));
        System.out.println(customCurveT1X7Y7.getCurveValue(22, 21,12));
        System.out.println(curveT1X7Y7.getCurveValue(22, 21,12));

    }
}
