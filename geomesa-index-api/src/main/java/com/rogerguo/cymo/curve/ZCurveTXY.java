package com.rogerguo.cymo.curve;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Date 6/4/20 8:58 PM
 * @Created by rogerguo
 */
public class ZCurveTXY implements SpaceFillingCurve {


    @Override
    public String getCurveValueString(int x, int y) {
        byte[] ret = new byte[8];
        int xh = makeGap(x);
        int xl = makeGap(x << 16);
        int yh = makeGap(y) >>> 1;
        int yl = makeGap(y << 16) >>> 1;

        int zh = xh | yh;
        int zl = xl | yl;

        byte[] rh = CurveUtil.toBytes(zh);
        byte[] rl = CurveUtil.toBytes(zl);
        System.arraycopy(rh, 0, ret, 0, 4);
        System.arraycopy(rl, 0, ret, 4, 4);
        return CurveUtil.bytesToBit(ret);
    }

    @Override
    public String getCurveValueString(int x, int y, int z) {
        //long numResult = split(x) | split(y) << 1 | split(z) << 2;
        long numResult = split(z) | split(y) << 1 | split(x) << 2;
        byte[] result = CurveUtil.toBytes(numResult);

        return CurveUtil.bytesToBit(result);
    }

    /**
     *  TODO 21 bit
     * @param x >0 x31x30...x0
     * @param y >0 y31y30...y0
     * @return
     */
    @Override
    public long getCurveValue(int x, int y) {
        byte[] ret = new byte[8];
        int xh = makeGap(x);
        int xl = makeGap(x << 16);
        int yh = makeGap(y) >>> 1;
        int yl = makeGap(y << 16) >>> 1;

        int zh = xh | yh;
        int zl = xl | yl;

        byte[] rh = CurveUtil.toBytes(zh);
        byte[] rl = CurveUtil.toBytes(zl);
        System.arraycopy(rh, 0, ret, 0, 4);
        System.arraycopy(rl, 0, ret, 4, 4);
        return CurveUtil.bytesToLong(ret);
    }

    @Override
    public Map<String, Integer> from2DCurveValue(long curveValue) {
        Map<String, Integer> resultMap = new HashMap<>();

        byte[] valueBytes = CurveUtil.toBytes(curveValue);

        int zh = Bytes.toInt(valueBytes, 0);
        int zl = Bytes.toInt(valueBytes, 4);

        int xh = elimGap(zh);
        int yh = elimGap(zh << 1);
        int xl = elimGap(zl) >>> 16;
        int yl = elimGap(zl << 1) >>> 16;

        int x = xh | xl;
        int y = yh | yl;
        resultMap.put("x", x);
        resultMap.put("y", y);

        return resultMap;
    }




    @Override
    public long getCurveValue(int x, int y, int z) {
        long numResult = split(y) | split(x) << 1 | split(z) << 2;

        return numResult;
    }


    public Map<String, Integer> from3DCurveValue(long curveValue) {
        Map<String, Integer> resultMap = new HashMap<>();
        int y = combine(curveValue);
        int x = combine(curveValue >> 1);
        int z = combine(curveValue >> 2);
        resultMap.put("x", x);
        resultMap.put("y", y);
        resultMap.put("z", z);

        return resultMap;
    }

    /** come from geomesa **/
    /** insert 00 between every bit in value. Only first 21 bits can be considered. */
    private static long MaxMask = 0x1fffffL;
    private static long split(long value) {
        long x = value & MaxMask;
        x = (x | x << 32) & 0x1f00000000ffffL;
        //System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
        x = (x | x << 16) & 0x1f0000ff0000ffL;
        //System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
        x = (x | x << 8)  & 0x100f00f00f00f00fL;
        //System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
        x = (x | x << 4)  & 0x10c30c30c30c30c3L;
        //System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
        return (x | x << 2)      & 0x1249249249249249L;
    }

    private static int combine(long curveValue) {
        long x = curveValue & 0x1249249249249249L;
        x = (x ^ (x >>  2)) & 0x10c30c30c30c30c3L;
        x = (x ^ (x >>  4)) & 0x100f00f00f00f00fL;
        x = (x ^ (x >>  8)) & 0x1f0000ff0000ffL;
        x = (x ^ (x >> 16)) & 0x1f00000000ffffL;
        x = (x ^ (x >> 32)) & MaxMask;
        return (int) x;
    }

    public static void main(String[] args) {
        ZCurveTXY zCurve = new ZCurveTXY();
        long curve = zCurve.getCurveValue(1256791, 132335, 2097151);
        Map location = zCurve.from3DCurveValue(curve);
        System.out.println(curve);
    }

    private static final int[] MASKS = new int[] { 0xFFFF0000, 0xFF00FF00,
            0xF0F0F0F0, 0xCCCCCCCC, 0xAAAAAAAA };
    private static int makeGap(int x) {
        int x0 = x & MASKS[0];
        int x1 = (x0 | (x0 >>> 8)) & MASKS[1];
        int x2 = (x1 | (x1 >>> 4)) & MASKS[2];
        int x3 = (x2 | (x2 >>> 2)) & MASKS[3];
        int x4 = (x3 | (x3 >>> 1)) & MASKS[4];
        return x4;
    }

    public static int elimGap(int x) {
        int x0 = x & MASKS[4];
        int x1 = (x0 | (x0 << 1)) & MASKS[3];
        int x2 = (x1 | (x1 << 2)) & MASKS[2];
        int x3 = (x2 | (x2 << 4)) & MASKS[1];
        int x4 = (x3 | (x3 << 8)) & MASKS[0];
        return x4;
    }

}

