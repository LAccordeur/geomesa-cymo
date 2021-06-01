package com.rogerguo.test;

import com.rogerguo.cymo.entity.SpatialRange;
import com.rogerguo.cymo.entity.TimeRange;
import com.rogerguo.cymo.hbase.RowKeyHelper;
import com.rogerguo.cymo.hbase.RowKeyItem;
import com.rogerguo.cymo.virtual.entity.NormalizedRange;
import com.rogerguo.cymo.virtual.helper.NormalizedDimensionHelper;
import com.rogerguo.cymo.virtual.normalization.TimePeriod;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * @Description
 * @Date 2020/6/18 16:36
 * @Created by X1 Carbon
 */
public class VerifyUtil {

    public static VerifyUtil util = new VerifyUtil();

    public static void main(String[] args) {
        VerifyUtil verifyUtil = new VerifyUtil();
        List<String> realResult = verifyUtil.computeCountInThisRange(
                new SpatialRange(-74.007756,-73.907756),
                new SpatialRange(40.723537,40.823537),
                new TimeRange(fromDateToTimestamp("2010-01-01 15:10:00"), fromDateToTimestamp("2010-01-01 16:09:59")));
        System.out.println(realResult.size());
        /*System.out.println(fromDateToTimestamp("2010-01-27 19:00:00"));
        System.out.println(NormalizedDimensionHelper.normalizedTime(TimePeriod.HOUR, fromDateToTimestamp("2010-01-01 00:00:00")));
        curveValue(-73.991000,40.750000, fromDateToTimestamp("2010-01-27 19:00:00"));*/

    }

    public static void verify(List<String> resultNeedVerify) {
        List<String> realResult = util.computeCountInThisRange(new SpatialRange(-73.960000, -73.910000),
                new SpatialRange(40.762000,40.767000),
                new TimeRange(fromDateToTimestamp("2010-01-02 15:05:00"), fromDateToTimestamp("2010-01-31 15:25:00")));
        System.out.println("-----verify-------");
        System.out.println("count in result need verify: " + resultNeedVerify.size());
        System.out.println("real count in the range: " + realResult.size());

        for (String string : resultNeedVerify) {
            if (realResult.contains(string)) {
                realResult.remove(string);
            }
        }
        System.out.println(realResult);
        System.out.println(realResult.size());


    }

    public static void verify(List<String> resultNeedVerify, SpatialRange longitudeRange, SpatialRange latitudeRange, TimeRange timeRange) {
        List<String> realResult = util.computeCountInThisRange(longitudeRange,
                latitudeRange,
                timeRange);
        System.out.println("-----verify-------");
        System.out.println("count in result need verify: " + resultNeedVerify.size());
        System.out.println("real count in the range: " + realResult.size());

        for (String string : resultNeedVerify) {
            if (realResult.contains(string)) {
                realResult.remove(string);
            }
        }
        System.out.println(realResult);
        System.out.println(realResult.size());


    }

    public List<String> computeCountInThisRange(SpatialRange longitudeRange, SpatialRange latitudeRange, TimeRange timeRange) {

        URL input = getClass().getClassLoader().getResource("dataset/trip_data_1_pickup.csv");
        //URL input = getClass().getClassLoader().getResource("G:\\DataSet\\FOIL2010\\format\\trip_data_1_pickup.csv");
        if (input == null) {
            throw new RuntimeException("Couldn't load resource trip_data_1.csv");
        }

        List<String> testResult = new ArrayList<>();
        int count = 0;
        // date parser corresponding to the CSV format
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);
        try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.DEFAULT)) {
            for (CSVRecord record : parser) {
                String seqID = record.get(0);
                String medallion = record.get(1);
                String tripTimeInSecs = record.get(3);
                String tripDistance = record.get(4);
                long time = Date.from(LocalDateTime.parse(record.get(2), dateFormat).toInstant(ZoneOffset.UTC)).getTime();
                double longitude = Double.parseDouble(record.get(5));
                double latitude = Double.parseDouble(record.get(6));


                if (SpatialRange.isInRange(longitude, longitudeRange) && SpatialRange.isInRange(latitude, latitudeRange) && TimeRange.isInRange(time, timeRange)) {
                    StringBuffer stringBuffer = new StringBuffer();
                    testResult.add(stringBuffer.append(seqID).append(longitude).append(latitude).append(time).toString());
                    count++;
                }


            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading taxi data:", e);
        }

        if (testResult.size() > 0) {
            System.out.println(testResult.get(0));
        } else {
            System.out.println(testResult.size());
        }
        return testResult;
    }

    public static long fromDateToTimestamp(String dateString) {
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);
        long time = Date.from(LocalDateTime.parse(dateString, dateFormat).toInstant(ZoneOffset.UTC)).getTime();
        return time;
    }

    public static RowKeyItem curveValue(double longitude, double latitude, long timestamp) {
        RowKeyItem value = RowKeyHelper.generateDataTableRowKey(longitude, latitude, timestamp);

        System.out.println(value);

        return value;
    }
}
