package com.cloudwick.cdap.hive.pcap.udtf;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.UdpPacket;
import org.pcap4j.packet.namednumber.UdpPort;
import org.xbill.DNS.Message;
import org.xbill.DNS.Record;
import org.xbill.DNS.Section;

import java.util.ArrayList;
import java.util.List;

public class SpotDnsGenericUDTF extends GenericUDTF {

    private PrimitiveObjectInspector longOI;
    private PrimitiveObjectInspector binaryOI;
    private DateTimeFormatter format = DateTimeFormat.forPattern("MMM dd yyyy HH:mm:ss.SSS 'UTC'");
    private Logger logger = Logger.getLogger(SpotDnsGenericUDTF.class);

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<StructField> inFields;
        inFields = (List<StructField>) argOIs.getAllStructFieldRefs();
        if (inFields.size() != 2) {
            throw new UDFArgumentException("SpotDnsGenericUDTF() takes exactly two arguments");
        }
        /**
         * This UDTF expects three arguments : UNIX Timestamp as long and UDP DNS payload as BINARY
         */
        if (((PrimitiveObjectInspector) inFields.get(0)
                .getFieldObjectInspector())
                .getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG &&
                ((PrimitiveObjectInspector) inFields.get(2)
                        .getFieldObjectInspector())
                        .getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BINARY) {
            throw new UDFArgumentException("SpotDnsGenericUDTF() takes a long and a binary as parameters");
        }

        longOI = (PrimitiveObjectInspector) inFields.get(0).getFieldObjectInspector();
        binaryOI = (PrimitiveObjectInspector) inFields.get(1).getFieldObjectInspector();
        List<String> fieldNames = new ArrayList<String>(14);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(14);

        fieldNames.add("frame_time");
        fieldNames.add("unix_tstamp");
        fieldNames.add("frame_len");
        fieldNames.add("ip_dst");
        fieldNames.add("ip_src");
        fieldNames.add("dns_qry_name");
        fieldNames.add("dns_qry_type");
        fieldNames.add("dns_qry_class");
        fieldNames.add("dns_qry_rcode");
        fieldNames.add("dns_a");
        fieldNames.add("y");
        fieldNames.add("m");
        fieldNames.add("d");
        fieldNames.add("h");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    private Message getMessage(UdpPacket udpPacket) {
        Message message = null;
        UdpPacket.UdpHeader udpHeader;
        try {
            udpHeader = udpPacket.getHeader();
            if (udpHeader.getSrcPort() == UdpPort.DOMAIN ||
                    udpHeader.getDstPort() == UdpPort.DOMAIN) {
                message = new Message(udpPacket.getPayload().getRawData());
            }
        } catch (Exception ex) {
            logger.error("Error while creating message : " + ex.getMessage());
            message = null;
        }
        return message;
    }

    private Object[] getObject(long ts, int len, IpV4Packet.IpV4Header header, Message message) {
        Record question = message.getQuestion();
        // Date expects epoch in milliseconds
        DateTime date = new DateTime(ts);
        Record[] dnsA = message.getSectionArray(Section.ANSWER);
        ArrayList<String> sRecords = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        for (Record r : dnsA) {
            sRecords.add(r.toString());
        }
        String answers = StringUtils.join(sRecords, ",");
        sb.append("[");
        sb.append(answers);
        sb.append("]");

        return new Object[]{format.print(date), ts, len,
                header.getDstAddr().getHostAddress(),
                header.getSrcAddr().getHostAddress(), question.getName().toString(),
                question.getType(), String.valueOf(question.getDClass()), message.getRcode(),
                sb.toString(), date.getYear(),
                date.getMonthOfYear(),
                date.getDayOfMonth(),
                date.getHourOfDay()
        };
    }

    public void process(Object[] objects) throws HiveException {
        EthernetPacket eth;
        IpV4Packet ip4;
        UdpPacket udp;
        Message message;
        Object[] out;
        long ts = PrimitiveObjectInspectorUtils.getLong(objects[0], longOI) * 1000;
        byte[] data = PrimitiveObjectInspectorUtils.getBinary(objects[1], binaryOI).getBytes();
        int len = data.length;
        try {
            if (len > 0) {
                eth = EthernetPacket.newPacket(data, 16, data.length - 16);
                ip4 = eth.get(IpV4Packet.class);
                udp = eth.get(UdpPacket.class);
                if (ip4 != null && udp != null) {
                    message = getMessage(udp);
                    if (message != null) {
                        out = getObject(ts, len, ip4.getHeader(), message);
                        forward(out);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("Error encountered while processing packet : " + ex.getMessage());
        }

    }

    public void close() throws HiveException {
    }
}
