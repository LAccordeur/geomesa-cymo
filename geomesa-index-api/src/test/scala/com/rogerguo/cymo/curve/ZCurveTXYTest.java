package com.rogerguo.cymo.curve;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Description
 * @Date 2021/5/7 14:21
 * @Created by X1 Carbon
 */
public class ZCurveTXYTest {

    @Test
    public void getCurveValue() {

        ZCurve zCurveTXY = new ZCurve();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 100; j++) {
                for (int k = 0; k < 100; k++) {
                    zCurveTXY.getCurveValue(i,j,k);
                }
            }
        }
        long stop = System.currentTimeMillis();
        System.out.println("Time: " + (stop - start));

    }

    @Test
    public void getCurveValue2() {

        CurveX3Y3T5 curveX3Y3T5 = new CurveX3Y3T5();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 80; i++) {
            for (int j = 0; j < 10; j++) {
                for (int k = 0; k < 10; k++) {
                    curveX3Y3T5.getCurveValue(i,j,k);
                }
            }
        }
        long stop = System.currentTimeMillis();
        System.out.println("Time: " + (stop - start));

    }
}