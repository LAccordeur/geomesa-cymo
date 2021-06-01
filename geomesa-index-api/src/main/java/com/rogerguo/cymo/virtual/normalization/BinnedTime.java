package com.rogerguo.cymo.virtual.normalization;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/**
 * @Description
 * @Date 6/4/20 8:44 PM
 * @Created by rogerguo
 */
public class BinnedTime {

    private int bin;

    private long offset;

    public BinnedTime(int bin, long offset) {
        this.bin = bin;
        this.offset = offset;
    }

    public BinnedTime() { }

    private ZonedDateTime epoch = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);

    // min value (inclusive)
    private ZonedDateTime zMinDate= epoch;

    private int maxBin = (int)Math.pow(2, NormalizedDimension.precision);

    // max precision is 21 bit
    // 2209-03-30T08:00Z
    private ZonedDateTime hoursMaxDate = epoch.plusHours(maxBin);

    // 7711-10-23T00:00Z
    private ZonedDateTime daysMaxDate = epoch.plusDays(maxBin);

    /**
     * Gets period index (e.g. days since the epoch)
     *
     * @param timePeriod interval type
     * @param timestamp
     * @return
     */
    public int timeToBin(TimePeriod timePeriod, long timestamp) {
        switch (timePeriod){
            case HOUR:  return toHour(timestamp);
            case DAY:   return toDay(timestamp);
            case MIN10: return toMin10(timestamp);
            case MIN30: return toMin30(timestamp);
            case MINUTE: return toMinute(timestamp);
            case MIN2: return toMin2(timestamp);
            case MIN5: return toMin5(timestamp);
            //case WEEK:  return toWeek(timestamp);
            //case MONTH: return toMonth(timestamp);
            //case YEAR:  return toYear(timestamp);
        }
        return -1;
    }

    public BinnedTime timeToBinnedTimeAndSecond(TimePeriod epochBinTimePeriod, int epochBin, long timestamp) {

        //  private def toDayAndMillis(date: ZonedDateTime): BinnedTime = {
        //    val days = toDay(date)
        //    val millisInDay = date.toInstant.toEpochMilli - Epoch.plus(days, ChronoUnit.DAYS).toInstant.toEpochMilli
        //    BinnedTime(days, millisInDay)
        //  }

        ZonedDateTime currentTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);

        switch (epochBinTimePeriod) {
            case HOUR: return new BinnedTime(epochBin, toSecondOffset(fromBinnedHour(epochBin), currentTime));
            case DAY:  return new BinnedTime(epochBin, toSecondOffset(fromBinnedDay(epochBin), currentTime));
        }

        return null;
    }

    public ZonedDateTime fromBinnedTimeAndSecond(TimePeriod timePeriod, BinnedTime binnedTime) {

        switch (timePeriod) {
            case HOUR: return epoch.plusHours(binnedTime.bin).plus(binnedTime.offset, ChronoUnit.SECONDS);
            case DAY:  return epoch.plusDays(binnedTime.bin).plus(binnedTime.offset, ChronoUnit.SECONDS);
        }
        return null;
    }

    private int toSecondOffset(ZonedDateTime epochTime, ZonedDateTime currentTime) {
        int secondOffset = (int) (currentTime.toEpochSecond() - epochTime.toEpochSecond());
        return secondOffset;
    }

    private int toMinute(long timestamp) {
        return toMinute(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC));
    }

    private int toMin2(long timestamp) {
        int epochMinute = toMinute(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC));
        return (int)(epochMinute / 2);
    }

    private int toMin5(long timestamp) {
        int epochMinute = toMinute(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC));
        return (int)(epochMinute / 5);
    }

    private int toMin10(long timestamp) {
        int epochMinute = toMinute(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC));
        return (int)(epochMinute / 10);
    }

    private int toMin30(long timestamp) {
        int epochMinute = toMinute(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC));
        return (int)(epochMinute / 30);
    }

    private int toHour(long timestamp) {
        return toHour(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC));
    }

    public ZonedDateTime fromBinnedHour(int hours) {
        //Epoch.plusDays(date.bin).plus(date.offset, ChronoUnit.MILLIS)
        if (hours > maxBin) {
            return hoursMaxDate;
        }

        return epoch.plusHours(hours);
    }

    private int toHour(ZonedDateTime date) {

        if (date.isBefore(zMinDate)) {
            return 0;
        }

        if (!hoursMaxDate.isAfter(date)) {
            return maxBin;
        }

        /*if (date.isBefore(zMinDate) || !hoursMaxDate.isAfter(date)) {
            return -1;
        }*/

        return (int)ChronoUnit.HOURS.between(epoch, date);
    }

    private int toMinute(ZonedDateTime date) {

        if (date.isBefore(zMinDate)) {
            return 0;
        }

        if (!hoursMaxDate.isAfter(date)) {
            return maxBin;
        }

        /*if (date.isBefore(zMinDate) || !hoursMaxDate.isAfter(date)) {
            return -1;
        }*/

        return (int)ChronoUnit.MINUTES.between(epoch, date);
    }

    private int toDay(long timestamp) {
        return toDay(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC));
    }

    public ZonedDateTime fromBinnedDay(int days) {
        if (days > maxBin) {
            return daysMaxDate;
        }
        return epoch.plusDays(days);
    }

    private int toDay(ZonedDateTime date) {
        //require(!date.isBefore(ZMinDate), s"Date exceeds minimum indexable value ($ZMinDate): $date")
        //require(DaysMaxDate.isAfter(date), s"Date exceeds maximum indexable value ($DaysMaxDate): $date")

        /*if (date.isBefore(zMinDate) || !daysMaxDate.isAfter(date)) {
            return -1;
        }*/

        if (date.isBefore(zMinDate)) {
            return 0;
        }

        if (!daysMaxDate.isAfter(date)) {
            return maxBin;
        }

        return (int)ChronoUnit.DAYS.between(epoch, date);
    }


    @Override
    public String toString() {
        return "BinnedTime{" +
                "bin=" + bin +
                ", offset=" + offset +
                '}';
    }

    public static void main(String[] args) {
        ZonedDateTime epoch = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
        ZonedDateTime result = epoch.plusHours((int)Math.pow(2, 21));
        System.out.println(result);

    }


}
