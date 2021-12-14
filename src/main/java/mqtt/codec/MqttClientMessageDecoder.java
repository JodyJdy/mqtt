

package mqtt.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import mqtt.enums.MqttMessageType;
import mqtt.enums.MqttQoS;
import mqtt.mqttclient.MessageExchanger;
import mqtt.mqttclient.Publisher;
import mqtt.protocol.MqttMessage;
import mqtt.protocol.fixheader.MqttFixedHeader;
import mqtt.protocol.payload.MqttPublishPayload;
import mqtt.protocol.varheader.*;
import mqtt.storage.Message;

/**
 *客户端消息解码器
 *
 **/

public class MqttClientMessageDecoder extends SimpleChannelInboundHandler<MqttMessage> {

    /**
     * 用于消息的发布
     */
    public static final AttributeKey<Publisher> PUBLISHER = AttributeKey.valueOf(Publisher.class,"publisher");
    /**
     * 用于接收到消息后进行处理
     */
    public static final AttributeKey<MessageExchanger> EXCHANGER = AttributeKey.valueOf(MessageExchanger.class,"exchanger");


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg)  {
        MqttMessageType messageType = msg.fixedHeader().messageType();
        switch (messageType){
            case PUBLISH:
            case PUBREL:
            case PUBACK:
            case PUBREC:
            case PUBCOMP:
                publishProcess(msg,ctx);break;
            case CONNACK:connAck(ctx);break;
            case SUBACK:
            case UNSUBACK:
            case PINGRESP:
            default:break;
        }
    }

    /**
     * 发布相关 报文的处理
     */
    private void publishProcess(MqttMessage msg, ChannelHandlerContext ctx){
        MqttFixedHeader fixedHeader = msg.fixedHeader();
        Publisher publisher = ctx.channel().attr(PUBLISHER).get();
        //发布ack报文, qos = 1
        if(fixedHeader.messageType() == MqttMessageType.PUBACK){
            MqttPublishAckVarHeader ackVarHeader = (MqttPublishAckVarHeader)msg.variableHeader();
            publisher.getAck().ack(ackVarHeader.getPacketId());
        //收到发布收到报文，应返回 发布释放报文
        } else if(fixedHeader.messageType() == MqttMessageType.PUBREC){
            MqttPublishRecVarHeader varHeader = (MqttPublishRecVarHeader)msg.variableHeader();
            publisher.sendPubRel(varHeader.getPacketId());
        //收到发布完成报文，再将消息ack, qos = 2
        } else if(fixedHeader.messageType() == MqttMessageType.PUBCOMP){
            MqttPublishCompVarHeader varHeader = (MqttPublishCompVarHeader)msg.variableHeader();
            publisher.getAck().ack(varHeader.getPacketId());
        //收到订阅的消息
        } else if(fixedHeader.messageType() == MqttMessageType.PUBLISH){
            MqttPublishVarHeader varHeader = (MqttPublishVarHeader) msg.variableHeader();
            MqttPublishPayload payload = (MqttPublishPayload)msg.payload();
            //接收到消息，交由listener处理
            Message ms = new Message(varHeader.getPacketId(),varHeader.getTopicName(),
                    payload.getBytes(),fixedHeader.qosLevel().value());
            MessageExchanger exchanger = ctx.channel().attr(EXCHANGER).get();
            exchanger.submit(ms);
            //无需响应
            if (fixedHeader.qosLevel().value() == MqttQoS.AT_MOST_ONCE.value()) {
                return;
            }
            //qos级别 == 1返回 publishAck报文
            if (fixedHeader.qosLevel().value() == MqttQoS.AT_LEAST_ONCE.value()) {
                publisher.sendPubAck(varHeader.getPacketId());
            }
            // qos级别 ==2 返回 publisRec 报文
            if (fixedHeader.qosLevel().value() == MqttQoS.EXACTLY_ONCE.value()) {
                publisher.sendPubRec(varHeader.getPacketId());
            }
        } else if(fixedHeader.messageType() == MqttMessageType.PUBREL){
            MqttPublishRelVarHeader varHeader = (MqttPublishRelVarHeader)msg.variableHeader();
            publisher.sendPubComp(varHeader.getPacketId());
        }
    }

    /**
     * 连接ACk 报文的处理
     */
    private void connAck(ChannelHandlerContext ctx){
         Publisher publisher = ctx.channel().attr(PUBLISHER).get();
         //连接报文使用0标识消息
         publisher.getAck().ack(0);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.channel().close();
    }
}
