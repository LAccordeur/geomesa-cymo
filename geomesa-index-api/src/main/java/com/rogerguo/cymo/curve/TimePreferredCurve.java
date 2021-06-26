package com.rogerguo.cymo.curve;

import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Date 6/4/20 8:52 PM
 * @Created by rogerguo
 */
public class TimePreferredCurve implements SpaceFillingCurve {


    @Override
    public String getCurveValueString(int x, int y) {
        return null;
    }

    @Override
    public String getCurveValueString(int x, int y, int z) {
        return null;
    }

    /**
     *
     * @param x >0 x31x30...x0
     * @param y >0 y31y30...y0
     * @return
     */
    @Override
    public long getCurveValue(int x, int y) {
        /*byte[] ret = new byte[8];
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

        return CurveUtil.bytesToLong(ret);*/
        return split(x) | split(y) << 1;
    }

    @Override
    public Map<String, Integer> from2DCurveValue(long curveValue) {

        return null;
    }




    @Override
    public long getCurveValue(int x, int y, int z) {

        // Bits of x and y will be encoded as ....y1x1y0x0
        long spatialPart = split(x) | split(y) << 1;
        int latBitNum = (int) Math.ceil(Math.log(VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY / VirtualLayerConfiguration.SPATIAL_VIRTUAL_LATITUDE_GRANULARITY) / Math.log(2D));
        int lonBitNum = (int) Math.ceil(Math.log(VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY / VirtualLayerConfiguration.SPATIAL_VIRTUAL_LONGITUDE_GRANULARITY) / Math.log(2D));
        int validSpatialBitNum = latBitNum + lonBitNum;
        long timePreferred = ((long) z << validSpatialBitNum) | spatialPart;

        return timePreferred;
    }


    public Map<String, Integer> from3DCurveValue(long curveValue) {

        return null;
    }

    private static long MaxMask = 0x1fffffL;
    /*private static long split(long value) {
        long x = value & MaxMask;
        x = (x | x << 32) & 0x1f00000000ffffL;
        System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
        x = (x | x << 16) & 0x1f0000ff0000ffL;
        System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
        x = (x | x << 8)  & 0x100f00f00f00f00fL;
        System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
        x = (x | x << 4)  & 0x10c30c30c30c30c3L;
        System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
        return (x | x << 2)      & 0x1249249249249249L;
    }*/

    /** insert 0 between every bit in value. Only first 21 bits can be considered. */
    public static long split(long value) {

        long x = value & MaxMask;
        x = (x ^ (x << 32)) & 0x00000000ffffffffL;
        x = (x ^ (x << 16)) & 0x0000ffff0000ffffL;
        x = (x ^ (x <<  8)) & 0x00ff00ff00ff00ffL; // 11111111000000001111111100000000..
        x = (x ^ (x <<  4)) & 0x0f0f0f0f0f0f0f0fL; // 1111000011110000
        x = (x ^ (x <<  2)) & 0x3333333333333333L; // 11001100..
        x = (x ^ (x <<  1)) & 0x5555555555555555L; // 1010...
        return x;
    }


    public static void main(String[] args) {
        /*TimePreferredCurve timePreferredCurve = new TimePreferredCurve();
        long curve = timePreferredCurve.getCurveValue(1, 3, 4);
        String stringFormat = CurveUtil.bytesToBit(CurveUtil.toBytes(curve));
        System.out.println(curve);
        System.out.println(stringFormat);*/

        TimePreferredCurve timePreferredCurve = new TimePreferredCurve();
        ZCurve zCurve = new ZCurve();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            for (int j = 0; j < 1000; j ++) {

                timePreferredCurve.getCurveValue(i, j);

            }
        }
        long stop = System.currentTimeMillis();
        System.out.println("time: " + (stop - start));
        long result = split(11);
        System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(result)));
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

