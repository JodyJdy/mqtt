

package mqtt.mqttclient;

import io.netty.channel.Channel;
import mqtt.enums.MqttQoS;
import mqtt.protocol.MqttMessage;
import mqtt.storage.Message;
import mqtt.util.MqttMessageUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 用于处理客户端所有的消息发布
 **/

public class Publisher {

    /**
     * 报文id
     */
    private AtomicLong packetId = new AtomicLong(1);

    private final Ack ack;

    private final Channel channel;

    public Map<String, MessageListener> getSubMap() {
        return subMap;
    }

    /**
     * 记录订阅对应的 MessageListener
     */
    private Map<String,MessageListener> subMap = new ConcurrentHashMap<>(2);


    Publisher(Channel channel, Ack ack) {
        this.channel = channel;
        this.ack = ack;
    }

    public Ack getAck() {
        return ack;
    }

    /**
      发布消息
     */
    public PublishResult publish(Message message){
        int id = (int)(packetId.getAndIncrement() % 65536);
        message.setPacketId(id);
        //qos级别为0，不需要等待服务端响应，不需要进行ack的处理
        if(message.getQos() != 0) {
            ack.addNeedAckMsg(id);
        }
        MqttMessage msg = MqttMessageUtil.publish(message, MqttQoS.valueOf(message.getQos()));
        channel.writeAndFlush(msg);
        return new PublishResult(ack,id);
    }
    /**
     * 发送连接报文， 连接报文没有报文id 使用0
     */
    PublishResult sendConn(MqttConnectOptions options){
        MqttMessage msg = MqttMessageUtil.conn(options);
        ack.addNeedAckMsg(0);
        channel.writeAndFlush(msg);
        return new PublishResult(ack,0);
    }
    /**
     * 发送 发布释放报文
     */
    public void sendPubRel(int packetId){
        MqttMessage msg = MqttMessageUtil.publishRel(packetId);
        channel.writeAndFlush(msg);
    }
    /**
     * 发送 发放ack报文
     */
    public void sendPubAck(int packetId){
        MqttMessage msg = MqttMessageUtil.publishAck(packetId);
        channel.writeAndFlush(msg);
    }
    /**
     * 发送 发送Rec报文
     */
    public void sendPubRec(int packetId){
        MqttMessage msg = MqttMessageUtil.publishRec(packetId);
        channel.writeAndFlush(msg);
    }
    /**
     * 发送 发送完成报文
     */
    public void sendPubComp(int packetId){
        MqttMessage msg = MqttMessageUtil.publishComp(packetId);
        channel.writeAndFlush(msg);
    }
    /**
     * 发送心跳报文
     */
    void sendPing(){
        MqttMessage msg = MqttMessageUtil.pingReq();
        channel.writeAndFlush(msg);
    }
    /**
     * 发送订阅 报文
     */
    public void sendSubscribe(String subscribe,int qos,MessageListener listener){
        int id = (int)(packetId.getAndIncrement() % 65536);
        MqttMessage msg = MqttMessageUtil.subscribe(subscribe,MqttQoS.valueOf(qos),id);
        channel.writeAndFlush(msg);
        subMap.put(subscribe,listener);

    }
    /**
     * 发送订阅取消报文
     */
    public void sendUnsubscribe(String subscribe){
        int id = (int)(packetId.getAndIncrement() % 65536);
        MqttMessage msg = MqttMessageUtil.unsub(subscribe,id);
        channel.writeAndFlush(msg);
        subMap.remove(subscribe);
    }

}
