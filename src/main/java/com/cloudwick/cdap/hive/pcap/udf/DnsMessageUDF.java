package com.cloudwick.cdap.hive.pcap.udf;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.UdpPacket;
import org.pcap4j.packet.namednumber.UdpPort;
import org.xbill.DNS.Message;

public class DnsMessageUDF extends UDF {
    private Message message;
    private EthernetPacket eth;
    private IpV4Packet ipV4;
    private UdpPacket udp;

    // TODO - Format output
    public Text evaluate(BytesWritable bytes) {
        UdpPacket.UdpHeader udpHeader;
        try {
            if (bytes.getLength() > 0 ) {
                eth = EthernetPacket.newPacket(bytes.getBytes(), 16, bytes.getLength() - 16);
                ipV4 = eth.get(IpV4Packet.class);
                udp = eth.get(UdpPacket.class);
                if (ipV4 != null && udp != null) {
                    udpHeader = udp.getHeader();
                    if (udpHeader.getSrcPort() == UdpPort.DOMAIN ||
                            udpHeader.getDstPort() == UdpPort.DOMAIN) {
                        message = new Message(udp.getPayload().getRawData());
                    }
                }
            }
            return new Text(message.toString());
        } catch (Exception ex) {
            return null;
        }
    }

}
