package com.rogerguo.cymo.virtual.helper;


import com.rogerguo.cymo.virtual.normalization.BinnedTime;
import com.rogerguo.cymo.virtual.normalization.NormalizedDimension;
import com.rogerguo.cymo.virtual.normalization.TimePeriod;

/**
 * @Description The max precision of normalization (lat, lon, time) is 21 bits
 * @Date 2019/11/7 20:34
 * @Created by GUO Yang
 */
public class NormalizedDimensionHelper {


    private static NormalizedDimension dimensionLat = new NormalizedDimension(-90d, 90d, NormalizedDimension.precision);

    private static NormalizedDimension dimensionLon = new NormalizedDimension(-180d, 180d, NormalizedDimension.precision);

    private static BinnedTime dimensionTime = new BinnedTime();

    public static int normalizedLat(double lat) {

        return dimensionLat.normalize(lat);
    }

    public static int normalizedLon(double lon) {
        return dimensionLon.normalize(lon);
    }

    /**
     * return the epoch days/hours
     * @param periodType
     * @param timestamp
     * @return
     */
    public static int normalizedTime(TimePeriod periodType, long timestamp) {
        return dimensionTime.timeToBin(periodType, timestamp);
    }

    public static void main(String[] args) {
        System.out.println(normalizedLon(-37.0600867));
        System.out.println(normalizedLon(-37.0595031));
        System.out.println(normalizedLon(-37.05912132));
    }
}
