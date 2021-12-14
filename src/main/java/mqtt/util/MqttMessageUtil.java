

package mqtt.util;

import mqtt.enums.MqttConnectReturnCode;
import mqtt.enums.MqttMessageType;
import mqtt.enums.MqttQoS;
import mqtt.mqttclient.MqttConnectOptions;
import mqtt.protocol.MqttMessage;
import mqtt.protocol.MqttTopic;
import mqtt.protocol.fixheader.MqttFixedHeader;
import mqtt.protocol.payload.MqttConnectPayload;
import mqtt.protocol.payload.MqttPublishPayload;
import mqtt.protocol.payload.MqttSubCancelPayload;
import mqtt.protocol.payload.MqttSubTopicPayload;
import mqtt.protocol.varheader.*;
import mqtt.storage.Message;

import java.util.Collections;

/**
 * 生成网络通信用的 MqttMessage报文的工具类
 **/

public class MqttMessageUtil {

    /**
     * 连接报文
     */
    public static MqttMessage conn(MqttConnectOptions options){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNECT,false,MqttQoS.AT_LEAST_ONCE,false,-1);
        MqttConnectVarHeader varHeader = new MqttConnectVarHeader(options.getName()
        ,options.getVersion(),options.getUserName() != null,options.getPassword() !=null
        , options.isWillRetain(),options.getWillQos(),options.isWillFlag(),options.isCleanSession(),options.getKeepAliveTimeSeconds());
        MqttConnectPayload payload = new MqttConnectPayload(options.getClientIdentifier(),options.getWillTopic(),options.getWillMessage(),options.getUserName(),options.getPassword());
        return new MqttMessage(fixedHeader,varHeader,payload);
    }

    /**
     * 连接ack
     */
    public static MqttMessage connAck(){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK,false, MqttQoS.AT_MOST_ONCE,false,2);
        MqttConnectAckVarHeader varHeader = new MqttConnectAckVarHeader(false, MqttConnectReturnCode.CONNECTION_ACCEPTED);
        return new MqttMessage(fixedHeader,varHeader,null);
    }

    /**
     * 发布ack
     */
    public static MqttMessage publishAck(int packetId){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK,false, MqttQoS.AT_LEAST_ONCE,false,2);
        MqttPublishAckVarHeader varHeader = new MqttPublishAckVarHeader(packetId);
        return new MqttMessage(fixedHeader,varHeader,null);
    }
    /**
     * 发布收到
     */
    public static MqttMessage publishRec(int packetId){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC,false, MqttQoS.EXACTLY_ONCE,false,2);
        MqttPublishRecVarHeader varHeader = new MqttPublishRecVarHeader(packetId);
        return new MqttMessage(fixedHeader,varHeader,null);
    }
    /**
     * 发布释放
     */
    public static MqttMessage publishRel(int packetId){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL,false, MqttQoS.EXACTLY_ONCE,false,2);
        MqttPublishRelVarHeader varHeader = new MqttPublishRelVarHeader(packetId);
        return new MqttMessage(fixedHeader,varHeader,null);
    }
    /**
     * 发布完成
     */
    public static MqttMessage publishComp(int packetId){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBCOMP,false, MqttQoS.EXACTLY_ONCE,false,2);
        MqttPublishCompVarHeader varHeader = new MqttPublishCompVarHeader(packetId);
        return new MqttMessage(fixedHeader,varHeader,null);
    }

    /**
     * 订阅取消 确认
     */
    public static MqttMessage unsubAck(int packetId){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK,false, MqttQoS.EXACTLY_ONCE,false,2);
        MqttSubCancelVarHeader varHeader = new MqttSubCancelVarHeader(packetId);
        return new MqttMessage(fixedHeader,varHeader,null);
    }

    /**
     * ping的请求
     */
    public static MqttMessage pingReq(){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGREQ,false, MqttQoS.EXACTLY_ONCE,false,0);
        return new MqttMessage(fixedHeader,null,null);
    }

    /**
     * ping的响应
     */
    public static MqttMessage pingRes(){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP,false, MqttQoS.EXACTLY_ONCE,false,0);
        return new MqttMessage(fixedHeader,null,null);
    }
    /**
     * 获取一个 Publish的报文
     */
    public static MqttMessage publish(Message message,MqttQoS qoS){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH,false,qoS,false,-1);
        MqttPublishVarHeader varHeader = new MqttPublishVarHeader(message.getTopic(),message.getPacketId());
        MqttPublishPayload payload = new MqttPublishPayload(message.getMsg());
        return new MqttMessage(fixedHeader,varHeader,payload);
    }
    /**
     * 获取一个订阅的报文
     */
    public static MqttMessage subscribe(String sub,MqttQoS qoS,int packetId){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBSCRIBE,false,MqttQoS.AT_LEAST_ONCE,false,-1);
        MqttSubTopicVarHeader varHeader = new MqttSubTopicVarHeader(packetId);
        MqttTopic topic = new MqttTopic(sub,qoS);
        MqttSubTopicPayload payload = new MqttSubTopicPayload(Collections.singletonList(topic));
        return new MqttMessage(fixedHeader,varHeader,payload);
    }
    /**
     * 获取一个取消订阅的报文
     */
    public static MqttMessage unsub(String sub,int packetId){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE,false,MqttQoS.AT_MOST_ONCE,false,-1);
        MqttSubCancelVarHeader varHeader = new MqttSubCancelVarHeader(packetId);
        MqttSubCancelPayload payload = new MqttSubCancelPayload(Collections.singletonList(sub));
        return new MqttMessage(fixedHeader,varHeader,payload);
    }
}
