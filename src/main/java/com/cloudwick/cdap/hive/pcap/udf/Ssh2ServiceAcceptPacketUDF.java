package com.cloudwick.cdap.hive.pcap.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.pcap4j.packet.Ssh2ServiceAcceptPacket;

public class Ssh2ServiceAcceptPacketUDF extends UDF {
    private Ssh2ServiceAcceptPacket packet;

    // TODO - Format output
    public Text evaluate(BytesWritable bytes) {
        try {
            packet = Ssh2ServiceAcceptPacket.newPacket(bytes.getBytes(), 0, bytes.getLength());
            return new Text(packet.toString());
        } catch (Exception ex) {
            return null;
        }
    }
}
