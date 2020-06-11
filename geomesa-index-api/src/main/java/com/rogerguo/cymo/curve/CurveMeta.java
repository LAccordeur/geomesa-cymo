package com.rogerguo.cymo.curve;

/**
 * @Description
 * @Date 6/4/20 8:50 PM
 * @Created by rogerguo
 */
public class CurveMeta {

    private CurveType curveType;

    /* for custom curve*/
    private int bitNumberX;

    private int bitNumberY;

    private int bitNumberT;

    private int bitNumberGroupX;

    private int bitNumberGroupY;

    private int bitNumberGroupT;

    @Override
    public String toString() {
        return curveType +
                "," + bitNumberX +
                "," + bitNumberY +
                "," + bitNumberT +
                "," + bitNumberGroupX +
                "," + bitNumberGroupY +
                "," + bitNumberGroupT
                ;
    }

    public CurveMeta(CurveType curveType) {
        this.curveType = curveType;
    }

    public CurveMeta(CurveType curveType, int bitNumberX, int bitNumberY, int bitNumberT, int bitNumberGroupX, int bitNumberGroupY, int bitNumberGroupT) {
        this.curveType = curveType;
        this.bitNumberX = bitNumberX;
        this.bitNumberY = bitNumberY;
        this.bitNumberT = bitNumberT;
        this.bitNumberGroupX = bitNumberGroupX;
        this.bitNumberGroupY = bitNumberGroupY;
        this.bitNumberGroupT = bitNumberGroupT;
    }

    public CurveType getCurveType() {
        return curveType;
    }

    public void setCurveType(CurveType curveType) {
        this.curveType = curveType;
    }

    public int getBitNumberX() {
        return bitNumberX;
    }

    public void setBitNumberX(int bitNumberX) {
        this.bitNumberX = bitNumberX;
    }

    public int getBitNumberY() {
        return bitNumberY;
    }

    public void setBitNumberY(int bitNumberY) {
        this.bitNumberY = bitNumberY;
    }

    public int getBitNumberT() {
        return bitNumberT;
    }

    public void setBitNumberT(int bitNumberT) {
        this.bitNumberT = bitNumberT;
    }

    public int getBitNumberGroupX() {
        return bitNumberGroupX;
    }

    public void setBitNumberGroupX(int bitNumberGroupX) {
        this.bitNumberGroupX = bitNumberGroupX;
    }

    public int getBitNumberGroupY() {
        return bitNumberGroupY;
    }

    public void setBitNumberGroupY(int bitNumberGroupY) {
        this.bitNumberGroupY = bitNumberGroupY;
    }

    public int getBitNumberGroupT() {
        return bitNumberGroupT;
    }

    public void setBitNumberGroupT(int bitNumberGroupT) {
        this.bitNumberGroupT = bitNumberGroupT;
    }
}
