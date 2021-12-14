

package mqtt.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.CharsetUtil;
import mqtt.protocol.MqttMessage;
import mqtt.protocol.MqttTopic;
import mqtt.protocol.fixheader.MqttFixedHeader;
import mqtt.protocol.payload.*;
import mqtt.protocol.varheader.*;
import mqtt.util.ByteUtil;

import java.util.List;

/**
 *消息编码器， 将MqttMessage转成字节
 **/
public class MqttEncoder extends MessageToMessageEncoder<MqttMessage> {
    @Override
    protected void encode(ChannelHandlerContext ctx, MqttMessage msg, List<Object> out)  {
        out.add(doEncode(msg,ctx));
    }

    private static ByteBuf doEncode(MqttMessage msg,ChannelHandlerContext ctx){
        switch (msg.fixedHeader().messageType()){
            case CONNECT:return encodeConnect(msg,ctx);
            case CONNACK:return encodeConnectAck(msg,ctx);
            case PUBLISH:return encodePublish(msg,ctx);
            case PUBACK:return encodePuback(msg,ctx);
            case PUBREC:return encodePubrec(msg, ctx);
            case PUBREL:return encodePubrel(msg,ctx);
            case PUBCOMP:return encodePubcomp(msg,ctx);
            case SUBSCRIBE:return encodeSubscribe(msg,ctx);
            case SUBACK:return encodeSubAck(msg,ctx);
            case UNSUBSCRIBE:return encodeUnSubscribe(msg,ctx);
            case UNSUBACK:return encodeUnSubAck(msg,ctx);
            case PINGREQ:return encodePingreq(msg,ctx);
            case PINGRESP:return encodePingresp(msg,ctx);
            case DISCONNECT:return encodeDisconnect(msg,ctx);
            default:break;
        }
        return null;
    }

    /**
     * 编码 连接报文
     */
    private static ByteBuf encodeConnect(MqttMessage msg,ChannelHandlerContext ctx){
        MqttFixedHeader fixedHeader = msg.fixedHeader();
        MqttConnectVarHeader connectVarHeader = (MqttConnectVarHeader) msg.variableHeader();
        MqttConnectPayload payload = (MqttConnectPayload)msg.payload();

        //计算 payLoad的大小
        int payLoadSize = 0;
        // 添加 clientdId的大小
        payLoadSize+= 2 + ByteUtil.utf8Bytes(payload.getClientIdentifier());

        if(connectVarHeader.isWillFlag()){
            payLoadSize+= 2 + ByteUtil.utf8Bytes(payload.getWillTopic());
            payLoadSize+= 2 + (payload.getWillMessage() == null ? 0 :payload.getWillMessage().length);
        }
        if(connectVarHeader.hasUserName()){
            payLoadSize+= 2 + ByteUtil.utf8Bytes(payload.getUserName());
        }
        if(connectVarHeader.hasPassword()){
            payLoadSize+= 2 + (payload.getPassword() == null ? 0 : payload.getPassword().length);
        }

        //计算可变头部varheader的大小
        // 协议名称 连接标志 保活时间
        int varHeaderSize =ByteUtil.utf8Bytes(connectVarHeader.name()) + 2 + 4;
        // 变长部分大小
        int varPartSize = varHeaderSize + payLoadSize;
        // 计算 固定头部大小， 1字节固定报头 + 几个用来标识可变部分长度的字节
        int fiexedPartSize =  1 + ByteUtil.getLengthByteNum(varPartSize);
        //获取用来写数据的 Buffer
        ByteBuf buf = ctx.alloc().buffer(fiexedPartSize + varPartSize);

        //写FixedHeader
        // 首字节
        buf.writeByte(getFixedHeaderByte1(fixedHeader));
        // 写可变部分的长度
        writeVariableLengthInt(buf,varPartSize);

        // 写 可变头部
        //写protocol name
        byte[] name = connectVarHeader.name().getBytes(CharsetUtil.UTF_8);
        buf.writeShort(name.length);
        buf.writeBytes(name);
        // 写 protocol level
        buf.writeByte(connectVarHeader.version());
        // 写connectFlag
        buf.writeByte(getConnVariableHeaderFlag(connectVarHeader));
        //写保活时间
        buf.writeShort(connectVarHeader.keepAliveTimeSeconds());

        // 写payload
        // 写 client id
        ByteUtil.writeString(buf,payload.getClientIdentifier());
        if(connectVarHeader.isWillFlag()){
            ByteUtil.writeString(buf,payload.getWillTopic());
            buf.writeShort(payload.getWillMessage().length);
            buf.writeBytes(payload.getWillMessage());
        }
        if (connectVarHeader.hasUserName()) {
            ByteUtil.writeString(buf,payload.getUserName());
        }
        if (connectVarHeader.hasPassword()) {
            buf.writeShort(payload.getPassword().length);
            buf.writeBytes(payload.getPassword());
        }
        return buf;
    }
    /**
     * 编码 连接ack报文
     */
    private static ByteBuf encodeConnectAck(MqttMessage msg,ChannelHandlerContext ctx){
        MqttConnectAckVarHeader varHeader = (MqttConnectAckVarHeader)msg.variableHeader();
        return withoutPayload(msg.fixedHeader(),ctx,varHeader.isSessionPresent()? (byte)0x01 : (byte)0x00,varHeader.getReturnCode().byteValue());

    }
    /**
     * 编码 发布消息 报文
     */
    private static ByteBuf encodePublish(MqttMessage msg,ChannelHandlerContext ctx){
        MqttFixedHeader fixedHeader = msg.fixedHeader();
        MqttPublishVarHeader varHeader = (MqttPublishVarHeader) msg.variableHeader();
        MqttPublishPayload payload = (MqttPublishPayload)msg.payload();

        int topicNameBytes = ByteUtil.utf8Bytes(varHeader.getTopicName());
        //可变头部 大小
        int varHeaderSize = 2 + topicNameBytes + ((fixedHeader.qosLevel().value()> 0) ? 2 :0);
        // payLoad大小
        int payLoadSize = payload.getBytes() == null ? 0 : payload.getBytes().length;

        //可变部分大小
        int variablePart = varHeaderSize + payLoadSize;
        //固定头部大小
        int fixedHeaderSize = 1 + ByteUtil.getLengthByteNum(variablePart);
        ByteBuf buf = ctx.alloc().buffer(fixedHeaderSize + variablePart);
        buf.writeByte(getFixedHeaderByte1(fixedHeader));
        writeVariableLengthInt(buf, variablePart);
        ByteUtil.writeString(buf,varHeader.getTopicName());
        if (fixedHeader.qosLevel().value() > 0) {
            buf.writeShort(varHeader.getPacketId());
        }
        buf.writeBytes(payload.getBytes());
        return buf;
    }
    /**
     * 编码 发布ack报文
     */
    private static ByteBuf encodePuback(MqttMessage msg,ChannelHandlerContext ctx){
        MqttPublishAckVarHeader varHeader = (MqttPublishAckVarHeader) msg.variableHeader();
        return withoutPayload(msg.fixedHeader(),ctx,varHeader.getPacketId());
    }
    /**
     * 编码  发布收到
     */
    private static ByteBuf encodePubrec(MqttMessage msg,ChannelHandlerContext ctx){
        MqttPublishRecVarHeader varHeader = (MqttPublishRecVarHeader) msg.variableHeader();
        return withoutPayload(msg.fixedHeader(),ctx,varHeader.getPacketId());
    }
    /**
     * 编码 发布释放报文
     */
    private static ByteBuf encodePubrel(MqttMessage msg,ChannelHandlerContext ctx){
        MqttPublishRelVarHeader varHeader = (MqttPublishRelVarHeader) msg.variableHeader();
        return withoutPayload(msg.fixedHeader(),ctx,varHeader.getPacketId());
    }
    /**
     * 编码 发布完成
     */
    private static ByteBuf encodePubcomp(MqttMessage msg,ChannelHandlerContext ctx){
        MqttPublishCompVarHeader varHeader = (MqttPublishCompVarHeader) msg.variableHeader();
        return withoutPayload(msg.fixedHeader(),ctx,varHeader.getPacketId());
    }
    /**
     * 编码 订阅报文
     */
    private static ByteBuf encodeSubscribe(MqttMessage msg,ChannelHandlerContext ctx){
        MqttFixedHeader fixedHeader = msg.fixedHeader();
        MqttSubTopicVarHeader varHeader = (MqttSubTopicVarHeader)msg.variableHeader();
        MqttSubTopicPayload payload = (MqttSubTopicPayload)msg.payload();

        // 可变报头 固定两个字节
        int variableLen = 2;
        //payload 长度
        int payloadLen = 0;
        for(MqttTopic topic : payload.getTopics()){
            int topicBytes = ByteUtil.utf8Bytes(topic.getTopic());
            //包括 topic的长度以及 qos占据的一个字节，额外加3
            payloadLen+=topicBytes + 3;
        }
        //可变部分长度
        int variablePart = variableLen + payloadLen;
        //获取固定头部长度
        int fixedLen = 1 + ByteUtil.getLengthByteNum(variablePart);
        ByteBuf buf = ctx.alloc().buffer(fixedLen + variablePart);
        //写固定头部
        buf.writeByte(getFixedHeaderByte1(fixedHeader));
        writeVariableLengthInt(buf,variablePart);
        //写可变报头
        buf.writeShort(varHeader.getPacketId());
        //写payload
        for(MqttTopic topic : payload.getTopics()){
            ByteUtil.writeString(buf,topic.getTopic());
            buf.writeByte(topic.getQoS().value());
        }
        return buf;
    }
    /**
     * 编码 订阅 确认 报文
     */
    private static ByteBuf encodeSubAck(MqttMessage msg,ChannelHandlerContext ctx){
        MqttFixedHeader fixedHeader = msg.fixedHeader();
        MqttSubAckVarHeader varHeader = (MqttSubAckVarHeader)msg.variableHeader();
        MqttSubAckPayload payload = (MqttSubAckPayload)msg.payload();

        int varHeaderLen = 2;
        int payloadLen = payload.getResultCode() == null? 0 : payload.getResultCode().size();
        //可变部分
        int varPart = varHeaderLen + payloadLen;
        //固定头部大小
        int fixHeadLen = 1 +  ByteUtil.getLengthByteNum(varPart);
        ByteBuf buf = ctx.alloc().buffer(fixHeadLen + varPart);
        buf.writeByte(getFixedHeaderByte1(fixedHeader));
        writeVariableLengthInt(buf,varPart);
        // 写可变报文
        buf.writeShort(varHeader.getPacketId());
        //写paylaod
        for(Integer returnCode : payload.getResultCode()){
            buf.writeByte(returnCode);
        }
        return buf;
    }
    /**
     * 编码 取消订阅报文
     */
    private static ByteBuf encodeUnSubscribe(MqttMessage msg,ChannelHandlerContext ctx){
        MqttFixedHeader fixedHeader = msg.fixedHeader();
        MqttSubCancelVarHeader varHeader = (MqttSubCancelVarHeader)msg.variableHeader();
        MqttSubCancelPayload payload = (MqttSubCancelPayload)msg.payload();
        //可变报文头部长度
        int varHeaderLen = 2;
        // palyload长度
        int payloadLen = 0;
        for(String sub : payload.getSubscribes()){
            payloadLen+= 2 + ByteUtil.utf8Bytes(sub);
        }
        //可变部分长度
        int variablePart = varHeaderLen + payloadLen;
        //固定头部长度
        int fixeHeaderLen = 1 + ByteUtil.getLengthByteNum(variablePart);
        ByteBuf buf = ctx.alloc().buffer(fixeHeaderLen + payloadLen);
        buf.writeByte(getFixedHeaderByte1(fixedHeader));
        writeVariableLengthInt(buf,variablePart);
        buf.writeShort(varHeader.getPacketId());
        //写payload
        for(String sub : payload.getSubscribes()){
            ByteUtil.writeString(buf,sub);
        }
        return buf;
    }
    /**
     * 编码 取消订阅ack报文
     */
    private static ByteBuf encodeUnSubAck(MqttMessage msg,ChannelHandlerContext ctx){
        MqttFixedHeader fixedHeader = msg.fixedHeader();
        MqttSubCancelAckVarHeader varHeader = (MqttSubCancelAckVarHeader) msg.variableHeader();
        //固定四个字节
        ByteBuf  buf = ctx.alloc().buffer(4);
        //写头部头部
        buf.writeByte(getFixedHeaderByte1(fixedHeader));
        //剩余长度两个字节
        writeVariableLengthInt(buf,2);
        buf.writeShort(varHeader.getPacketId());
        return buf;
    }
    /**
     * 编码 心跳请求报文
     */
    private static ByteBuf encodePingreq(MqttMessage msg ,ChannelHandlerContext ctx){
        return onleyFixedHeader(msg,ctx);
    }
    /**
     * 编码 编码心跳响应报文
     */
    private static ByteBuf encodePingresp(MqttMessage msg,ChannelHandlerContext ctx){
        return onleyFixedHeader(msg,ctx);
    }
    /**
     * 编码 断开连接报文
     */
    private static ByteBuf encodeDisconnect(MqttMessage msg,ChannelHandlerContext ctx){
        return onleyFixedHeader(msg,ctx);
    }

    /**
     * 只有 固定报头部分
     */
    private static ByteBuf onleyFixedHeader(MqttMessage msg,ChannelHandlerContext ctx){
        MqttFixedHeader fixedHeader = msg.fixedHeader();
        ByteBuf buf = ctx.alloc().buffer(2);
        buf.writeByte(getFixedHeaderByte1(fixedHeader));
        buf.writeByte(0);
        return buf;
    }
    /***
     * 只有 固定报头 和 两个字节的 可变头部, 可变头部参数是一个short的报文id
     */
    private static ByteBuf withoutPayload(MqttFixedHeader fixedHeader,ChannelHandlerContext ctx,int packetId){
        ByteBuf buf = ctx.alloc().buffer(4);
        buf.writeByte(getFixedHeaderByte1(fixedHeader));
        //剩余长度两个字节
        writeVariableLengthInt(buf,2);
        buf.writeShort(packetId);
        return buf;
    }
    /**
     * 只有 固定报头 和 两个字节的 可变头部, 可变头部是一个 两个字节
     */
    private static ByteBuf withoutPayload(MqttFixedHeader fixedHeader,ChannelHandlerContext ctx,Byte b1,Byte b2){
        ByteBuf buf = ctx.alloc().buffer(4);
        buf.writeByte(getFixedHeaderByte1(fixedHeader));
        //剩余长度两个字节
        writeVariableLengthInt(buf,2);
        buf.writeByte(b1);
        buf.writeByte(b2);
        return buf;
    }


    /**
     * 获取报文固定头部的第一个字节
     */
    private static int getFixedHeaderByte1(MqttFixedHeader header) {
        int ret = 0;
        ret |= header.messageType().value() << 4;
        if (header.isDup()) {
            ret |= 0x08;
        }
        ret |= header.qosLevel().value() << 1;
        if (header.isRetain()) {
            ret |= 0x01;
        }
        return ret;
    }

    /**
     * 固定报文写入 可变部分的长度
     */
    private static void writeVariableLengthInt(ByteBuf buf, int num) {
        do {
            int digit = num % 128;
            num /= 128;
            if (num > 0) {
                digit |= 0x80;
            }
            buf.writeByte(digit);
        } while (num > 0);
    }

    /**
     * 获取 连接报文 可变报文头 的连接标志 connectFlag
     */
    private static int getConnVariableHeaderFlag(MqttConnectVarHeader variableHeader) {
        int flagByte = 0;
        if (variableHeader.hasUserName()) {
            flagByte |= 0x80;
        }
        if (variableHeader.hasPassword()) {
            flagByte |= 0x40;
        }
        if (variableHeader.isWillRetain()) {
            flagByte |= 0x20;
        }
        flagByte |= (variableHeader.willQos() & 0x03) << 3;
        if (variableHeader.isWillFlag()) {
            flagByte |= 0x04;
        }
        if (variableHeader.isCleanSession()) {
            flagByte |= 0x02;
        }
        return flagByte;
    }
}




