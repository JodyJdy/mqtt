

package mqtt.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.util.CharsetUtil;
import mqtt.enums.DecoderState;
import mqtt.enums.MqttConnectReturnCode;
import mqtt.enums.MqttMessageType;
import mqtt.enums.MqttQoS;
import mqtt.protocol.MqttMessage;
import mqtt.protocol.MqttTopic;
import mqtt.protocol.payload.*;
import mqtt.protocol.varheader.*;
import mqtt.protocol.fixheader.MqttFixedHeader;
import mqtt.util.ByteUtil;

import static mqtt.enums.DecoderState.*;
import static mqtt.util.ByteUtil.decodeToByteArray;
import static mqtt.util.ByteUtil.decodeToString;

import java.util.ArrayList;
import java.util.List;

/**
 * 消息解码器， 解码为 MqttMessage
 **/
public class MqttDecoder extends ReplayingDecoder<DecoderState> {
    /**
     * 固定报文头部
     */
    private MqttFixedHeader fixedHeader;
    /**
     * 变长报文头部
     */
    private VaraibleHeader varaibleHeader;
    /**
     * 报文还需要读取的字节长度
     */
    private int remainLength;

    public MqttDecoder() {
        super(READ_FIXED_HEADER);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state()) {
            case READ_FIXED_HEADER:
                this.fixedHeader = decodeFixedHeader(in);
                this.remainLength = fixedHeader.remainingLength();
                checkpoint(READ_VARIABLE_HEADER);
                //对于没有varheader,payload的报文直接结束掉
                if (!hasVarHeader(fixedHeader.messageType())) {
                    out.add(new MqttMessage(fixedHeader, null, null));
                    checkpoint(READ_FIXED_HEADER);
                }
                break;
            case READ_VARIABLE_HEADER:
                this.varaibleHeader = decodeVariableHeader(this.fixedHeader, in);
                if (this.varaibleHeader != null) {
                    this.remainLength -= this.varaibleHeader.getByteConsume();
                }
                checkpoint(READ_PAYLOAD);
                //对于没有 payload的报文，这里直接返回掉
                if (!hasPayload(fixedHeader.messageType())) {
                    out.add(new MqttMessage(fixedHeader, varaibleHeader, null));
                    checkpoint(READ_FIXED_HEADER);
                }
                break;
            case READ_PAYLOAD:
                //读取完payload得到一个完整的报文体
                Payload payload = decodePayload(fixedHeader.messageType(), in, remainLength, varaibleHeader);
                out.add(new MqttMessage(fixedHeader, varaibleHeader, payload));
                checkpoint(READ_FIXED_HEADER);
                break;
            default:
                throw new Exception();
        }
    }

    /**
     * 解码 固定头部
     */
    private static MqttFixedHeader decodeFixedHeader(ByteBuf buf) {
        // 首字节： MQTT控制报文的类型 & 用于指定控制报文类型的标志位
        short byte1 = buf.readUnsignedByte();
        // 高四位 7 6 5 4 代表报文类型
        MqttMessageType messageType = MqttMessageType.valueOf(byte1 >> 4);
        // 低四位 3 2 1 0 代表 dup qos高位,qos低位,retain的控制位
        boolean dupFlag = (byte1 & 0x08) != 0;
        //qos占两位
        int qos = (byte1 & 0x06) >> 1;
        boolean retain = (byte1 & 0x01) != 0;
        // 首字节后是报文剩余长度， 每读取大于0x80就要再读取一个字节
        short digit;
        int remainingLength = 0;
        int multiplier = 1;
        do {
            digit = buf.readUnsignedByte();
            remainingLength += (digit & 127) * multiplier;
            multiplier *= 128;
        } while ((digit & 128) != 0);

        return new MqttFixedHeader(messageType, dupFlag, MqttQoS.valueOf(qos), retain, remainingLength);
    }

    /**
     * 解码变长头部
     */
    private static VaraibleHeader decodeVariableHeader(MqttFixedHeader header, ByteBuf buf) {
        switch (header.messageType()) {
            case CONNECT:
                return decodeConnectionVariableHeader(buf);
            case CONNACK:
                return decodeMqttConnectAckVarheader(buf);
            case PUBLISH:
                return decodeMqttPublishVarHeader(buf, header);
            case PUBACK:
                return decodeMqttPublishAckVarHeader(buf);
            case PUBREC:
                return decodeMqttPublishRecVarHeader(buf);
            case PUBREL:
                return decodeMqttPublishRelVarHeader(buf);
            case PUBCOMP:
                return decodeMqttPublishCompVarHeader(buf);
            case SUBSCRIBE:
                return decodeMqttSubTopicVarHeader(buf);
            case SUBACK:
                return decodeMqttSubAckVarHeader(buf);
            case UNSUBSCRIBE:
                return decodeMqttSubCancelVarcheder(buf);
            case UNSUBACK:
                return decodeMqttSubCancelAckVarHeader(buf);
            default:
                break;
        }
        return null;
    }

    /**
     * 解码 PayLoad
     */
    private static Payload decodePayload(MqttMessageType mqttMessageType, ByteBuf buf, int remainLen, VaraibleHeader varaibleHeader) {
        switch (mqttMessageType) {
            case CONNECT:
                return decodeConnectPayload(buf, (MqttConnectVarHeader) varaibleHeader);
            case PUBLISH:
                return decodeMqttPublishPayload(buf, remainLen);
            case SUBSCRIBE:
                return decodeMqttSubTopicPayload(buf, remainLen);
            case SUBACK:
                return decodeMqttSubAckPayload(buf, remainLen);
            case UNSUBSCRIBE:
                return decodeMqttSubCancelPayload(buf, remainLen);
            default:
                return null;
        }
    }

    /**
     * 解码连接报文可变头部
     */
    private static MqttConnectVarHeader decodeConnectionVariableHeader(ByteBuf buf) {
        //读取的字节数
        int byteCosumed = 0;
        //读取协议名称的长度
        int procolNameLen = ByteUtil.decodeMsbLsb(buf);
        byteCosumed += 2;
        //获取协议名
        String protocolName = buf.toString(buf.readerIndex(), procolNameLen, CharsetUtil.UTF_8);
        buf.skipBytes(procolNameLen);
        byteCosumed += procolNameLen;
        //读取协议级别
        int protocolLevel = buf.readUnsignedByte();
        byteCosumed += 1;
        //读取连接标志
        int connectFlag = buf.readUnsignedByte();
        byteCosumed += 1;
        // 8位连接标志内容为: userNameFlag,passWordFlag,willRetain, willQos两位，wiiFlag,cleanSession,Reserverd
        boolean hasUserName = (connectFlag & 0x80) != 0;
        boolean hasPassWord = (connectFlag & 0x40) != 0;
        boolean willRetain = (connectFlag & 0x20) != 0;
        int willQos = (connectFlag & 0x18) >> 3;
        boolean willFlag = (connectFlag & 0x4) != 0;
        boolean cleanSession = (connectFlag & 0x02) != 0;
        //保活时间
        int keepAlive = ByteUtil.decodeMsbLsb(buf);
        byteCosumed += 2;

        MqttConnectVarHeader mqttConnectVarHeader = new MqttConnectVarHeader(protocolName, protocolLevel, hasUserName, hasPassWord
                , willRetain, willQos, willFlag, cleanSession, keepAlive);
        //设置 可变头部占用的字节数
        mqttConnectVarHeader.setByteConsume(byteCosumed);
        return mqttConnectVarHeader;

    }

    /**
     * 解码 连接报文 Payload
     */
    private static MqttConnectPayload decodeConnectPayload(ByteBuf buffer,
                                                           MqttConnectVarHeader mqttConnectVariableHeader) {
        int cosumeByte = 0;
        // 读取 client Identifier
        int clintIdLen = ByteUtil.decodeMsbLsb(buffer);
        String cliendId = decodeToString(buffer, clintIdLen);
        cosumeByte += clintIdLen + 2;
        String willTopic = null;
        byte[] willMessage = null;
        // 如果 willFlag为true, payload的下一个字段是will Topic, willMessage
        if (mqttConnectVariableHeader.isWillFlag()) {
            //获取 will Topic长度
            int willTopicLen = ByteUtil.decodeMsbLsb(buffer);
            willTopic = decodeToString(buffer, willTopicLen);
            cosumeByte += willTopicLen + 2;
            //获取willMessage长度
            int willMessageLen = ByteUtil.decodeMsbLsb(buffer);
            willMessage = decodeToByteArray(buffer, willMessageLen);
            cosumeByte += willMessageLen + 2;
        }
        String userName = null;
        byte[] passWord = null;
        //如果有用户名，读取用户名
        if (mqttConnectVariableHeader.hasUserName()) {
            int userNameLen = ByteUtil.decodeMsbLsb(buffer);
            userName = decodeToString(buffer, userNameLen);
            cosumeByte += userNameLen + 2;
        }
        //如果有密码，读取密码
        if (mqttConnectVariableHeader.hasPassword()) {
            int passwordLen = ByteUtil.decodeMsbLsb(buffer);
            passWord = decodeToByteArray(buffer, passwordLen);
            cosumeByte += passwordLen + 2;
        }

        MqttConnectPayload connectPayload = new MqttConnectPayload(cliendId, willTopic, willMessage, userName, passWord);
        connectPayload.setByteConsume(cosumeByte);
        return connectPayload;
    }

    /**
     * 解码连接Ack报文变长头部
     */
    private static MqttConnectAckVarHeader decodeMqttConnectAckVarheader(ByteBuf buffer) {
        // session Present 标志
        boolean sessionPresent = (buffer.readByte() & 0x01) != 0;
        // 连接返回码
        MqttConnectReturnCode returnCode = MqttConnectReturnCode.valueOf(buffer.readByte());
        MqttConnectAckVarHeader ackVarHeader = new MqttConnectAckVarHeader(sessionPresent, returnCode);
        ackVarHeader.setByteConsume(2);
        return ackVarHeader;
    }

    /**
     * 解码发布报文可变头部
     */
    private static MqttPublishVarHeader decodeMqttPublishVarHeader(ByteBuf buf, MqttFixedHeader fixedHeader) {
        int byteConsume = 0;
        // 获取 topicName
        int topicNameLen = ByteUtil.decodeMsbLsb(buf);
        String topicName = decodeToString(buf, topicNameLen);
        byteConsume += topicNameLen + 2;
        // qos等级是1,2，才有 报文标识符
        int messageId = -1;
        if (fixedHeader.qosLevel().value() > 0) {
            messageId = ByteUtil.decodeMsbLsb(buf);
            byteConsume += 2;
        }
        MqttPublishVarHeader publishVarHeader = new MqttPublishVarHeader(topicName, messageId);
        publishVarHeader.setByteConsume(byteConsume);
        return publishVarHeader;
    }

    /**
     * 解码发布报文 Payload
     */
    private static MqttPublishPayload decodeMqttPublishPayload(ByteBuf buf, int remainLength) {
        byte[] payload = decodeToByteArray(buf, remainLength);
        return new MqttPublishPayload(payload);
    }

    /**
     * 解码发布确认报文 变长头部
     */
    private static MqttPublishAckVarHeader decodeMqttPublishAckVarHeader(ByteBuf buf) {
        int messageId = ByteUtil.decodeMsbLsb(buf);
        MqttPublishAckVarHeader mqttPublishAckVarHeader = new MqttPublishAckVarHeader(messageId);
        mqttPublishAckVarHeader.setByteConsume(2);
        return mqttPublishAckVarHeader;
    }

    /**
     * 解码 发布收到报文 变长头部
     */
    private static MqttPublishRecVarHeader decodeMqttPublishRecVarHeader(ByteBuf buf) {
        int messageId = ByteUtil.decodeMsbLsb(buf);
        MqttPublishRecVarHeader mqttPublishRecVarHeader = new MqttPublishRecVarHeader(messageId);
        mqttPublishRecVarHeader.setByteConsume(2);
        return mqttPublishRecVarHeader;
    }

    /**
     * 解码 发布释放报文 变长头部
     */
    private static MqttPublishRelVarHeader decodeMqttPublishRelVarHeader(ByteBuf buf) {
        int messageId = ByteUtil.decodeMsbLsb(buf);
        MqttPublishRelVarHeader mqttPublishRelVarHeader = new MqttPublishRelVarHeader(messageId);
        mqttPublishRelVarHeader.setByteConsume(2);
        return mqttPublishRelVarHeader;
    }

    /**
     * 解码 发布完成报文 变长头部
     */
    private static MqttPublishCompVarHeader decodeMqttPublishCompVarHeader(ByteBuf buf) {
        int messageId = ByteUtil.decodeMsbLsb(buf);
        MqttPublishCompVarHeader mqttPublishCompVarHeader = new MqttPublishCompVarHeader(messageId);
        mqttPublishCompVarHeader.setByteConsume(2);
        return mqttPublishCompVarHeader;
    }

    /**
     * 解码 订阅主题 报文 变长头部
     */
    private static MqttSubTopicVarHeader decodeMqttSubTopicVarHeader(ByteBuf buf) {
        int messageId = ByteUtil.decodeMsbLsb(buf);
        MqttSubTopicVarHeader varHeader = new MqttSubTopicVarHeader(messageId);
        varHeader.setByteConsume(2);
        return varHeader;
    }

    /**
     * 解码订阅主题报文 payload
     */
    private static MqttSubTopicPayload decodeMqttSubTopicPayload(ByteBuf buf, int remainLength) {
        int consumeByte = 0;
        List<MqttTopic> topicList = new ArrayList<>();
        while (consumeByte < remainLength) {
            int len = ByteUtil.decodeMsbLsb(buf);
            String topic = decodeToString(buf, len);
            MqttQoS qoS = MqttQoS.valueOf(buf.readByte());
            consumeByte += 3 + len;
            topicList.add(new MqttTopic(topic, qoS));
        }
        MqttSubTopicPayload subTopicPayload = new MqttSubTopicPayload(topicList);
        subTopicPayload.setByteConsume(consumeByte);
        return subTopicPayload;

    }

    /**
     * 解码 订阅ack报文
     */
    private static MqttSubAckVarHeader decodeMqttSubAckVarHeader(ByteBuf buf) {
        int messageId = ByteUtil.decodeMsbLsb(buf);
        MqttSubAckVarHeader subAckVarHeader = new MqttSubAckVarHeader(messageId);
        subAckVarHeader.setByteConsume(2);
        return subAckVarHeader;
    }

    /**
     * 解码 订阅ack payload
     */
    private static MqttSubAckPayload decodeMqttSubAckPayload(ByteBuf buf, int remainLength) {
        int consumeByte = 0;
        List<Integer> resutls = new ArrayList<>();
        while (consumeByte < remainLength) {
            int result = buf.readByte();
            resutls.add(result);
            consumeByte += 1;
        }
        MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(resutls);
        mqttSubAckPayload.setByteConsume(consumeByte);
        return mqttSubAckPayload;
    }

    /**
     * 解码 订阅取消报文变长头部
     */
    private static MqttSubCancelVarHeader decodeMqttSubCancelVarcheder(ByteBuf buf) {
        int messageId = ByteUtil.decodeMsbLsb(buf);
        MqttSubCancelVarHeader mqttSubCancelVarHeader = new MqttSubCancelVarHeader(messageId);
        mqttSubCancelVarHeader.setByteConsume(2);
        return mqttSubCancelVarHeader;
    }

    /**
     * 解码 订阅取消报文payload
     */
    private static MqttSubCancelPayload decodeMqttSubCancelPayload(ByteBuf buf, int remainLength) {
        int consumeByte = 0;
        List<String> topicList = new ArrayList<>();
        while (consumeByte < remainLength) {
            int len = ByteUtil.decodeMsbLsb(buf);
            String topic = decodeToString(buf, len);
            consumeByte += 2 + len;
            topicList.add(topic);
        }
        MqttSubCancelPayload mqttSubCancelPayload = new MqttSubCancelPayload(topicList);
        mqttSubCancelPayload.setByteConsume(consumeByte);
        return mqttSubCancelPayload;

    }

    /**
     * 解码订阅取消确认报文 varheader
     */
    private static MqttSubCancelAckVarHeader decodeMqttSubCancelAckVarHeader(ByteBuf buf) {
        int messageId = ByteUtil.decodeMsbLsb(buf);
        MqttSubCancelAckVarHeader mqttSubCancelAckVarHeaderq = new MqttSubCancelAckVarHeader(messageId);
        mqttSubCancelAckVarHeaderq.setByteConsume(2);
        return mqttSubCancelAckVarHeaderq;
    }

    /**
     * 判断消息类型的报文有无 负载
     */
    private static boolean hasPayload(MqttMessageType messageType) {
        switch (messageType) {
            case CONNECT:
            case PUBLISH:
            case SUBSCRIBE:
            case UNSUBSCRIBE:
            case SUBACK:
                return true;
            default:
                return false;
        }
    }

    /**
     * 判断消息类型的报文有无 可变头部
     */
    private static boolean hasVarHeader(MqttMessageType mqttMessageType) {
        switch (mqttMessageType) {
            case DISCONNECT:
            case PINGRESP:
                return false;
            default:
                return true;
        }
    }

}
