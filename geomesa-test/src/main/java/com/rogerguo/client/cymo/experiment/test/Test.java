package com.rogerguo.client.cymo.experiment.test;

import com.rogerguo.cymo.curve.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * @Description
 * @Date 2021/3/26 21:48
 * @Created by X1 Carbon
 */
public class Test {
    public static void main(String[] args) {

        String result = getStubKey("LAPTOP-ROGERGUO", 4897, true);
        System.out.println(result);
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
