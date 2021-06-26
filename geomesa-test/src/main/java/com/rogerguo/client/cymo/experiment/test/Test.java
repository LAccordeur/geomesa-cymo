package com.rogerguo.client.cymo.experiment.test;

import com.rogerguo.cymo.curve.*;
import com.rogerguo.cymo.virtual.entity.CellLocation;
import com.rogerguo.cymo.virtual.entity.NormalizedLocation;
import com.rogerguo.cymo.virtual.helper.NormalizedDimensionHelper;
import com.rogerguo.cymo.virtual.normalization.TimePeriod;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

/**
 * @Description
 * @Date 2021/3/26 21:48
 * @Created by X1 Carbon
 */
public class Test {
    public static void main(String[] args) {

        ZCurve zCurve = new ZCurve();
        CellLocation cellLocation = new CellLocation(new NormalizedLocation(617795, 1523051, 1262304000));
        long result = zCurve.getCurveValue(1608, 1983);
        System.out.println(result);
    }


    public static long fromDateToTimestamp(String dateString) {
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);
        long time = Date.from(LocalDateTime.parse(dateString, dateFormat).toInstant(ZoneOffset.UTC)).getTime();
        return time;
    }

    static String getStubKey(
                             final String rsHostname,
                             int port,
                             boolean resolveHostnames) {

        // Sometimes, servers go down and they come back up with the same hostname but a different
        // IP address. Force a resolution of the rsHostname by trying to instantiate an
        // InetSocketAddress, and this way we will rightfully get a new stubKey.
        // Also, include the hostname in the key so as to take care of those cases where the
        // DNS name is different but IP address remains the same.
        String address = rsHostname;
        if (resolveHostnames) {
            InetAddress i =  new InetSocketAddress(rsHostname, port).getAddress();
            if (i != null) {
                address = i.getHostAddress() + "-" + rsHostname;
            }
        }
        return  address + ":" + port;
    }
}
