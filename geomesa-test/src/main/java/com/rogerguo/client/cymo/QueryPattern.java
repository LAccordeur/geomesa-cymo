package com.rogerguo.client.cymo;

import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.virtual.entity.SubspaceLocation;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Description
 * @Date 2020/3/18 15:09
 * @Created by X1 Carbon
 */
public class QueryPattern {

    private int longitudeWidth;

    private int latitudeWidth;

    private int timeWidth;

    private double frequency;

    private double weight;

    private List<RangeQuery> historicalRangeQueryList = new ArrayList<>();

    @Override
    public String toString() {
        return "QueryPattern{" +
                "longitudeWidth=" + longitudeWidth +
                ", latitudeWidth=" + latitudeWidth +
                ", timeWidth=" + timeWidth +
                ", frequency=" + frequency +
                ", weight=" + weight +
                '}';
    }

    public void generateRangeQueryList(int size, SubspaceLocation subspaceLocation) {
        Random random = new Random(System.currentTimeMillis());

        int longitudeLengthOfSubspace = VirtualLayerConfiguration.PARTITION_LONGITUDE_GRANULARITY / VirtualLayerConfiguration.SPATIAL_VIRTUAL_LONGITUDE_GRANULARITY;
        int latitudeLengthOfSubspace = VirtualLayerConfiguration.PARTITION_LATITUDE_GRANULARITY / VirtualLayerConfiguration.SPATIAL_VIRTUAL_LATITUDE_GRANULARITY;

        for (int i = 0; i < size; i++) {
            int xStart = random.nextInt(longitudeLengthOfSubspace - longitudeWidth);
            int yStart = random.nextInt(latitudeLengthOfSubspace - latitudeWidth);
            int tStart = random.nextInt(subspaceLocation.getNormalizedPartitionLength() - this.timeWidth);


            int xStop = xStart + this.getLongitudeWidth() - 1;
            int yStop = yStart + this.getLatitudeWidth() - 1;
            int tStop = tStart + this.getTimeWidth() - 1;
            RangeQuery rangeQuery = new RangeQuery(xStart, xStop, yStart, yStop, tStart, tStop);
            historicalRangeQueryList.add(rangeQuery);
        }

    }

    public List<RangeQuery> getHistoricalRangeQueryList() {
        return historicalRangeQueryList;
    }

    public void setHistoricalRangeQueryList(List<RangeQuery> historicalRangeQueryList) {
        this.historicalRangeQueryList = historicalRangeQueryList;
    }

    public QueryPattern(int longitudeWidth, int latitudeWidth, int timeWidth, double frequency, double weight) {
        this.longitudeWidth = longitudeWidth;
        this.latitudeWidth = latitudeWidth;
        this.timeWidth = timeWidth;
        this.frequency = frequency;
        this.weight = weight;
    }

    public QueryPattern(RangeQuery historicalRangeQuery, double frequency, double weight) {
        this.longitudeWidth = historicalRangeQuery.getxStop() - historicalRangeQuery.getxStart() + 1;
        this.latitudeWidth = historicalRangeQuery.getyStop() - historicalRangeQuery.getyStart() + 1;
        this.timeWidth = historicalRangeQuery.gettStop() - historicalRangeQuery.gettStart() + 1;
        this.weight = weight;
        this.frequency = frequency;
        this.historicalRangeQueryList.add(historicalRangeQuery);
    }

    public void addRangeQuery(RangeQuery rangeQuery) {
        this.historicalRangeQueryList.add(rangeQuery);
    }

    public int getLongitudeWidth() {
        return longitudeWidth;
    }

    public void setLongitudeWidth(int longitudeWidth) {
        this.longitudeWidth = longitudeWidth;
    }

    public int getLatitudeWidth() {
        return latitudeWidth;
    }

    public void setLatitudeWidth(int latitudeWidth) {
        this.latitudeWidth = latitudeWidth;
    }

    public int getTimeWidth() {
        return timeWidth;
    }

    public void setTimeWidth(int timeWidth) {
        this.timeWidth = timeWidth;
    }

    public double getFrequency() {
        return frequency;
    }

    public void setFrequency(double frequency) {
        this.frequency = frequency;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }
}
