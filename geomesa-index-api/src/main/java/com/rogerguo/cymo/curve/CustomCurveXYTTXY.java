package com.rogerguo.cymo.curve;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Date 6/4/20 9:04 PM
 * @Created by rogerguo
 */
public class CustomCurveXYTTXY implements SpaceFillingCurve {

    private int bitNumberX;

    private int bitNumberY;

    private int bitNumberT;

    private int bitNumberGroupX;

    private int bitNumberGroupY;

    private int bitNumberGroupT;

    public CustomCurveXYTTXY(int bitNumberX, int bitNumberY, int bitNumberT, int bitNumberGroupX, int bitNumberGroupY, int bitNumberGroupT) {
        this.bitNumberX = bitNumberX;
        this.bitNumberY = bitNumberY;
        this.bitNumberT = bitNumberT;
        this.bitNumberGroupX = bitNumberGroupX;
        this.bitNumberGroupY = bitNumberGroupY;
        this.bitNumberGroupT = bitNumberGroupT;
    }

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
    /**
     * xyttxy
     */
    public long getCurveValue(int x, int y, int z) {
        /*String curveBinaryString = generateCurve(x, y, z, 4, 2, 4);
        System.out.println(curveBinaryString);
        //count++;
        //System.out.println(count);
        return Long.valueOf(curveBinaryString, 2);*/
        return generateCurveEfficiently(x, y, z, bitNumberX, bitNumberY, bitNumberT, bitNumberGroupX, bitNumberGroupY, bitNumberGroupT);
    }

    @Override
    public Map<String, Integer> from3DCurveValue(long curveValue) {
        return from3DCurveEfficiently(curveValue, bitNumberX, bitNumberY, bitNumberT, bitNumberGroupX, bitNumberGroupY, bitNumberGroupT);
    }

    @Override
    public Map<String, Integer> from2DCurveValue(long curveValue) {
        return null;
    }

    /** Only first 21 bits can be considered. */
    private static long MaxMask = 0x1fffffL;

    private static Map<Integer, Long> BIT_MASK_MAP = new HashMap<>();

    static {
        BIT_MASK_MAP.put(0, 0x0L);
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
     * xyt
     * @param x
     * @param y
     * @param t
     * @param bitNumberX
     * @param bitNumberY
     * @param bitNumberT
     * @return
     */
    public static long generateCurveEfficiently(int x, int y, int t, int bitNumberX, int bitNumberY, int bitNumberT, int bitNumberGroupX, int bitNumberGroupY, int bitNumberGroupT) {


        int remainBitNumberX = precision - bitNumberX - bitNumberGroupX;
        int remainBitNumberY = precision - bitNumberY - bitNumberGroupY;
        int remainBitNumberT = precision - bitNumberT - bitNumberGroupT;
        int groupBitSize = bitNumberGroupT + bitNumberGroupX + bitNumberGroupY;

        long xBytes = x & MaxMask;
        long yBytes = y & MaxMask;
        long tBytes = t & MaxMask;

        long value = 0L;
        // group part
        /*value = value | (tBytes & BIT_MASK_MAP.get(bitNumberGroupT));
        value = value | ((yBytes & BIT_MASK_MAP.get(bitNumberGroupY)) << bitNumberGroupT);
        value = value | ((xBytes & BIT_MASK_MAP.get(bitNumberGroupX)) << (bitNumberGroupT + bitNumberGroupY));*/
        value = value | (yBytes & BIT_MASK_MAP.get(bitNumberGroupY));
        value = value | ((xBytes & BIT_MASK_MAP.get(bitNumberGroupX)) << bitNumberGroupY);
        value = value | ((tBytes & BIT_MASK_MAP.get(bitNumberGroupT)) << (bitNumberGroupX + bitNumberGroupY));


        // normal part
        value = value | (((tBytes & BIT_MASK_MAP.get(bitNumberT + bitNumberGroupT)) >> bitNumberGroupT) << (groupBitSize));
        value = value | (((yBytes & BIT_MASK_MAP.get(bitNumberY + bitNumberGroupY)) >> bitNumberGroupY) << (groupBitSize + bitNumberT));
        value = value | (((xBytes & BIT_MASK_MAP.get(bitNumberX + bitNumberGroupX)) >> bitNumberGroupX) << (groupBitSize + bitNumberT + bitNumberY));

        // remaining part
        value = value | (((tBytes & ~BIT_MASK_MAP.get(bitNumberT + bitNumberGroupT)) >> (bitNumberT + bitNumberGroupT)) << (groupBitSize + bitNumberX + bitNumberY + bitNumberT));
        value = value | (((yBytes & ~BIT_MASK_MAP.get(bitNumberY + bitNumberGroupY)) >> (bitNumberY + bitNumberGroupY)) << (groupBitSize + bitNumberX + bitNumberY + bitNumberT + remainBitNumberT));
        value = value | (((xBytes & ~BIT_MASK_MAP.get(bitNumberX + bitNumberGroupX)) >> (bitNumberX + bitNumberGroupX)) << (groupBitSize + bitNumberX + bitNumberY + bitNumberT + remainBitNumberT + remainBitNumberY));

        //System.out.println(value);
        return value;
    }

    /**
     *
     * @param value
     * @param bitNumberX
     * @param bitNumberY
     * @param bitNumberT
     * @return
     */
    public static Map<String, Integer> from3DCurveEfficiently(long value, int bitNumberX, int bitNumberY, int bitNumberT, int bitNumberGroupX, int bitNumberGroupY, int bitNumberGroupT) {
        int remainBitNumberX = precision - bitNumberX - bitNumberGroupX;
        int remainBitNumberY = precision - bitNumberY - bitNumberGroupY;
        int remainBitNumberT = precision - bitNumberT - bitNumberGroupT;
        int bitGroupSize = bitNumberGroupT + bitNumberGroupX + bitNumberGroupY;
        int totalBitSize = bitGroupSize + bitNumberT + bitNumberX + bitNumberY;

        long tempX = value;
        long tempY = value;
        long tempT = value;

        tempT = ((value >> (bitNumberGroupX + bitNumberGroupY)) & BIT_MASK_MAP.get(bitNumberGroupT)) | (((value >> (bitGroupSize)) & BIT_MASK_MAP.get(bitNumberT)) << bitNumberGroupT) | (((value >> (totalBitSize)) & BIT_MASK_MAP.get(remainBitNumberT)) << (bitNumberGroupT + bitNumberT));
        tempY = (value & BIT_MASK_MAP.get(bitNumberGroupY)) | (((value >> (bitGroupSize + bitNumberT)) & BIT_MASK_MAP.get(bitNumberY)) << bitNumberGroupY) | (((value >> (totalBitSize + remainBitNumberT)) & BIT_MASK_MAP.get(remainBitNumberY)) << (bitNumberGroupY + bitNumberY));
        tempX = ((value >> (bitNumberGroupY)) & BIT_MASK_MAP.get(bitNumberGroupX)) | (((value >> (bitGroupSize + bitNumberT + bitNumberY)) & BIT_MASK_MAP.get(bitNumberX)) << bitNumberGroupX) | (((value >> (totalBitSize + remainBitNumberT + remainBitNumberY)) & BIT_MASK_MAP.get(remainBitNumberX)) << (bitNumberGroupX + bitNumberX));

        Map<String, Integer> resultMap = new HashMap<>();
        resultMap.put("x", (int)tempX);
        resultMap.put("y", (int)tempY);
        resultMap.put("z", (int)tempT);
        //System.out.println(value);
        return resultMap;
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

