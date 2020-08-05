package com.rogerguo.cymo.curve;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Author GUO Yang
 * @Date 2020-01-19 6:27 PM
 */
public class CurveT1X3Y3 implements SpaceFillingCurve {

    private static int count = 0;

    private static int precision = 21;

    @Override
    public String getCurveValueString(int x, int y) {
        return null;
    }

    @Override
    public String getCurveValueString(int x, int y, int z) {
        return null;
    }

    @Override
    public long getCurveValue(int x, int y) {
        return 0;
    }


    @Override
    public long getCurveValue(int x, int y, int z) {
        /*String curveBinaryString = generateCurve(x, y, z, 4, 2, 4);
        System.out.println(curveBinaryString);
        //count++;
        //System.out.println(count);
        return Long.valueOf(curveBinaryString, 2);*/
        return generateCurveEfficiently(z, x, y, 1, 3, 3);
    }

    @Override
    public Map<String, Integer> from3DCurveValue(long curveValue) {
        return from3DCurveEfficiently(curveValue, 1, 3, 3);
    }

    @Override
    public Map<String, Integer> from2DCurveValue(long curveValue) {
        return null;
    }

    /** Only first 21 bits can be considered. */
    private static long MaxMask = 0x1fffffL;

    private static Map<Integer, Long> BIT_MASK_MAP = new HashMap<>();

    static {
        BIT_MASK_MAP.put(1, 0x1L);
        BIT_MASK_MAP.put(2, 0x3L);
        BIT_MASK_MAP.put(3, 0x7L);
        BIT_MASK_MAP.put(4, 0xfL);
        BIT_MASK_MAP.put(5, 0x1fL);
        BIT_MASK_MAP.put(6, 0x3fL);
        BIT_MASK_MAP.put(7, 0x7fL);
        BIT_MASK_MAP.put(8, 0xffL);
        BIT_MASK_MAP.put(9, 0x1ffL);
        BIT_MASK_MAP.put(10, 0x3ffL);
        BIT_MASK_MAP.put(11, 0x7ffL);
        BIT_MASK_MAP.put(12, 0xfffL);
        BIT_MASK_MAP.put(13, 0x1fffL);
        BIT_MASK_MAP.put(14, 0x3fffL);
        BIT_MASK_MAP.put(15, 0x7ffL);
        BIT_MASK_MAP.put(16, 0xffffL);
        BIT_MASK_MAP.put(17, 0x1ffffL);
        BIT_MASK_MAP.put(18, 0x3ffffL);
        BIT_MASK_MAP.put(19, 0x7ffffL);
        BIT_MASK_MAP.put(20, 0xfffffL);
        BIT_MASK_MAP.put(21, 0x1fffffL);
    }

    /**
     *
     * @param value
     * @param bitNumberX
     * @param bitNumberY
     * @param bitNumberT
     * @return
     */
    public static Map<String, Integer> from3DCurveEfficiently(long value, int bitNumberX, int bitNumberY, int bitNumberT) {
        int remainBitNumberX = precision - bitNumberX;
        int remainBitNumberY = precision - bitNumberY;
        int remainBitNumberT = precision - bitNumberT;

        long tempX = value;
        long tempY = value;
        long tempT = value;

        tempT = (value & BIT_MASK_MAP.get(bitNumberT)) | (((value >> (bitNumberX + bitNumberY + bitNumberT)) & BIT_MASK_MAP.get(remainBitNumberT)) << bitNumberT);
        tempY = ((value >> bitNumberT) & BIT_MASK_MAP.get(bitNumberY)) | (((value >> (bitNumberX + bitNumberY + precision)) & BIT_MASK_MAP.get(remainBitNumberY)) << bitNumberY);
        tempX = ((value >> (bitNumberT + bitNumberY)) & BIT_MASK_MAP.get(bitNumberX)) | (((value >> (bitNumberX + precision + precision)) & BIT_MASK_MAP.get(remainBitNumberX)) << bitNumberX);

        Map<String, Integer> resultMap = new HashMap<>();
        resultMap.put("z", (int)tempX);
        resultMap.put("x", (int)tempY);
        resultMap.put("y", (int)tempT);
        //System.out.println(value);
        return resultMap;
    }

    /**
     * xyt
     * @param x
     * @param y
     * @param t
     * @param bitNumberX
     * @param bitNumberY
     * @param bitNumberT
     * @return
     */
    public static long generateCurveEfficiently(int x, int y, int t, int bitNumberX, int bitNumberY, int bitNumberT) {
        int remainBitNumberX = precision - bitNumberX;
        int remainBitNumberY = precision - bitNumberY;
        int remainBitNumberT = precision - bitNumberT;

        long xBytes = x & MaxMask;
        long yBytes = y & MaxMask;
        long tBytes = t & MaxMask;

        long value = 0L;
        value = value | (tBytes & BIT_MASK_MAP.get(bitNumberT));
        value = value | ((yBytes & BIT_MASK_MAP.get(bitNumberY)) << bitNumberT);
        value = value | ((xBytes & BIT_MASK_MAP.get(bitNumberX)) << (bitNumberT + bitNumberY));
        value = value | ((tBytes & ~BIT_MASK_MAP.get(bitNumberT)) << (bitNumberX + bitNumberY));
        value = value | ((yBytes & ~BIT_MASK_MAP.get(bitNumberY)) << (bitNumberX + bitNumberT + remainBitNumberT));
        value = value | ((xBytes & ~BIT_MASK_MAP.get(bitNumberX)) << (bitNumberY + bitNumberT + remainBitNumberT + remainBitNumberY));

        //System.out.println(value);
        return value;
    }

    public static String generateCurve(int x, int y, int t, int bitNumberX, int bitNumberY, int bitNumberT) {
        String binaryStringX = int2BinaryString(x, precision);
        String binaryStringY = int2BinaryString(y, precision);
        String binaryStringT = int2BinaryString(t, precision);
        //System.out.println("x: " + binaryStringX);
        //System.out.println("y: " + binaryStringY);
        //System.out.println("t: " + binaryStringT);

        String lowX = binaryStringX.substring(binaryStringX.length() - bitNumberX);
        String lowY = binaryStringY.substring(binaryStringY.length() - bitNumberY);
        String lowT = binaryStringT.substring(binaryStringT.length() - bitNumberT);
        //System.out.println("low x: " + lowX);
        //System.out.println("low y: " + lowY);
        //System.out.println("low t: " + lowT);


        String highX = binaryStringX.substring(0, binaryStringX.length() - bitNumberX);
        String highY = binaryStringY.substring(0, binaryStringY.length() - bitNumberY);
        String highT = binaryStringT.substring(0, binaryStringT.length() - bitNumberT);
        //System.out.println("high x: " + highX);
        //System.out.println("high y: " + highY);
        //System.out.println("high t: " + highT);

        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append('0');
        stringBuffer.append(highX).append(highY).append(highT);
        stringBuffer.append(lowX).append(lowY).append(lowT);

        return stringBuffer.toString();
    }

    public static String int2BinaryString(int value, int precision) {
        String string = CurveUtil.bytesToBit(CurveUtil.toBytes(value));
        return string.substring(string.length() - precision);
    }
}
