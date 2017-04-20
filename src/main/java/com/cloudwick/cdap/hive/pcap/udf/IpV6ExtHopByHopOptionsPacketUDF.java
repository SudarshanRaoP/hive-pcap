package com.cloudwick.cdap.hive.pcap.udf;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.pcap4j.packet.IpV6ExtHopByHopOptionsPacket;

public class IpV6ExtHopByHopOptionsPacketUDF extends UDF {
        private IpV6ExtHopByHopOptionsPacket packet;

        // TODO - Format output
        public Text evaluate(BytesWritable bytes) {
            try {
                packet = IpV6ExtHopByHopOptionsPacket.newPacket(bytes.getBytes(),
                        0, bytes.getLength());
                return new Text(packet.toString());
            } catch (Exception ex) {
                return null;
            }
        }
}
