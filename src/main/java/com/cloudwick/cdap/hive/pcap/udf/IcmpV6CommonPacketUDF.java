package com.cloudwick.cdap.hive.pcap.udf;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.pcap4j.packet.IcmpV6CommonPacket;

public class IcmpV6CommonPacketUDF extends UDF {
    private IcmpV6CommonPacket packet;

    // TODO - Format output
    public Text evaluate(BytesWritable bytes) {
        try {
            packet = IcmpV6CommonPacket.newPacket(bytes.getBytes(), 0, bytes.getLength());
            return new Text(packet.toString());
        } catch (Exception ex) {
            return null;
        }
    }

}
