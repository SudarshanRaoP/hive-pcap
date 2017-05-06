package com.cloudwick.cdap.hive.pcap.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.pcap4j.packet.EthernetPacket;

public class EthernetPacketUDF extends UDF {
    private EthernetPacket packet;

    // TODO - Format output
    public Text evaluate(BytesWritable bytes) {
        try {
            packet = EthernetPacket.newPacket(bytes.getBytes(), 16, bytes.getLength() - 16);
            return new Text(packet.toString());
        } catch (Exception ex) {
            return null;
        }
    }
}
