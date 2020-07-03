package com.rogerguo.test;

import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import com.rogerguo.cymo.hbase.HBaseDriver;
import com.rogerguo.cymo.virtual.VirtualLayerGeoMesa;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Description
 * @Date 2020/6/18 16:02
 * @Created by X1 Carbon
 */
public class HBaseTest {

    public static void main(String[] args) throws IOException {
        /*HBaseDriver hBaseDriver = new HBaseDriver("127.0.0.1");
        hBaseDriver.scan(VirtualLayerGeoMesa.VIRTUAL_LAYER_INFO_TABLE, Bytes.toBytes("test"), Bytes.toBytes("test"));*/
        System.out.println(1L << 21);
    }

}
