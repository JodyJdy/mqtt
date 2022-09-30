

package mqtt.codec;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import mqtt.enums.MqttMessageType;
import mqtt.protocol.MqttMessage;
import mqtt.protocol.MqttTopic;
import mqtt.protocol.fixheader.MqttFixedHeader;
import mqtt.protocol.payload.*;
import mqtt.protocol.varheader.*;
import mqtt.mqttserver.Session;
import mqtt.mqttserver.UserSessions;
import mqtt.storage.Message;
import mqtt.storage.MessageQueue;
import mqtt.util.MqttMessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 服务端 消息解码器
 **/
public class MqttServerMessageDecoder extends SimpleChannelInboundHandler<MqttMessage> {
    private static final Logger logger = LoggerFactory.getLogger(MqttServerMessageDecoder.class);
    /**
     * 用户会话信息
     */
    private final UserSessions userSessions;

    /**
     * 接收到消息后进行中转
     */
    private final MessageQueue messageQueue;
    /**
     * 用于存储，获取用户id
     */
    private final AttributeKey<String> ID = AttributeKey.valueOf(String.class, "id");

    public MqttServerMessageDecoder(UserSessions userSessions, MessageQueue messageQueue) {
        this.userSessions = userSessions;
        this.messageQueue = messageQueue;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        MqttMessageType messageType = msg.fixedHeader().messageType();
        MqttMessage result = null;

        switch (messageType) {
            case CONNECT:
                result = connReturn(msg, ctx);
                break;
            case PUBLISH:
            case PUBREC:
            case PUBREL:
                result = publishReturn(msg);
                break;
            case SUBSCRIBE:
            case UNSUBSCRIBE:
                result = subscribeReturn(msg, ctx);
                break;
            case PINGREQ:
                result = ping();
                break;
            case DISCONNECT:
                disconnect(ctx.channel());
                break;
            default:
                break;
        }
        if (result != null) {
            ctx.writeAndFlush(result);
        }
    }

    /**
     * 对于连接报文的处理
     */
    private MqttMessage connReturn(MqttMessage msg, ChannelHandlerContext ctx) {
        MqttConnectVarHeader connectVarHeader = (MqttConnectVarHeader) msg.variableHeader();
        MqttConnectPayload payload = (MqttConnectPayload) msg.payload();
        Channel channel = ctx.channel();
        channel.attr(ID).set(payload.getClientIdentifier());
        logger.info("接收到客户端的连接，id:{},username:{}",payload.getClientIdentifier(),payload.getUserName());
        userSessions.addUser(new Session(channel, connectVarHeader, payload));
        return MqttMessageUtil.connAck();
    }

    /**
     * 对于发布报文的处理以及返回
     */
    private MqttMessage publishReturn(MqttMessage msg) {
        MqttFixedHeader fixedHeader = msg.fixedHeader();
        if (msg.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
            MqttPublishVarHeader varHeader = (MqttPublishVarHeader) msg.variableHeader();
            MqttPublishPayload payload = (MqttPublishPayload) msg.payload();

            //接受到消息，将消息写入中转队列
            Message ms = new Message(varHeader.getPacketId(), varHeader.getTopicName(),
                    payload.getBytes(), fixedHeader.qosLevel().value());

            messageQueue.putMessage(ms);
            //无需响应
            if (fixedHeader.qosLevel().value() == 0) {
                return null;
            }
            //qos级别 == 1返回 publishAck报文
            if (fixedHeader.qosLevel().value() == 1) {
                return MqttMessageUtil.publishAck(varHeader.getPacketId());
            }
            // qos级别 ==2 返回 publisRec 报文
            if (fixedHeader.qosLevel().value() == 2) {
                return MqttMessageUtil.publishRec(varHeader.getPacketId());
            }
            // qos级别为2时，客户端会发送一个pubRel报文,返回发布完成
        } else if (msg.fixedHeader().messageType() == MqttMessageType.PUBREL) {
            MqttPublishRelVarHeader varHeader = (MqttPublishRelVarHeader) msg.variableHeader();
            return MqttMessageUtil.publishComp(varHeader.getPacketId());
        } else if (msg.fixedHeader().messageType() == MqttMessageType.PUBREC) {
            //qos==2,返回发布释放
            MqttPublishRecVarHeader varHeader = (MqttPublishRecVarHeader) msg.variableHeader();
            return MqttMessageUtil.publishRel(varHeader.getPacketId());
        }
        return null;
    }

    /**
     * 对于订阅相关报文的处理以及返回
     */
    private MqttMessage subscribeReturn(MqttMessage mqttMessage, ChannelHandlerContext ctx) {
        MqttFixedHeader fixedHeader = mqttMessage.fixedHeader();
        //获取用户标识符
        Channel channel = ctx.channel();
        String id = channel.attr(ID).get();

        if (fixedHeader.messageType() == MqttMessageType.SUBSCRIBE) {
            MqttSubTopicVarHeader varHeader = (MqttSubTopicVarHeader) mqttMessage.variableHeader();
            MqttSubTopicPayload payload = (MqttSubTopicPayload) mqttMessage.payload();
            List<MqttTopic> topicList = payload.getTopics();
            //添加订阅
            userSessions.addSub(id, topicList);
            //返回响应报文
            MqttFixedHeader returnHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, fixedHeader.qosLevel(), false, 2);
            MqttSubAckVarHeader varHeader1 = new MqttSubAckVarHeader(varHeader.getPacketId());
            List<Integer> returncodes = topicList.stream().map(topic -> topic.getQoS().value()).collect(Collectors.toList());
            MqttSubAckPayload payload1 = new MqttSubAckPayload(returncodes);
            return new MqttMessage(returnHeader, varHeader1, payload1);
        } else if (fixedHeader.messageType() == MqttMessageType.UNSUBSCRIBE) {
            MqttSubCancelVarHeader varHeader = (MqttSubCancelVarHeader) mqttMessage.variableHeader();
            MqttSubCancelPayload payload = (MqttSubCancelPayload) mqttMessage.payload();
            //删除订阅
            userSessions.rmSub(id, payload.getSubscribes());
            return MqttMessageUtil.unsubAck(varHeader.getPacketId());
        }
        return null;
    }

    /**
     * 处理心跳请求
     */
    private MqttMessage ping() {
        return MqttMessageUtil.pingRes();
    }

    /**
     * 断开连接
     */
    private void disconnect(Channel channel) {
        String id = channel.attr(ID).get();
        logger.info("客户端 id:{} 断开·连接",id);
        userSessions.rmUser(id);
        channel.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)  {
        String id = ctx.channel().attr(ID).get();
        logger.info("客户端 id:{} 异常断开连接",id);
        userSessions.rmUser(id);
        ctx.channel().close();
    }
}
