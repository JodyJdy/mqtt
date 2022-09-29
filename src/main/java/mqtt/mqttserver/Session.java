

package mqtt.mqttserver;

import io.netty.channel.Channel;
import mqtt.enums.MqttQoS;
import mqtt.protocol.payload.MqttConnectPayload;
import mqtt.protocol.MqttTopic;
import mqtt.protocol.varheader.MqttConnectVarHeader;
import mqtt.util.TopicUtil;

import java.util.*;

/**
 *
 * 一个用户对应的会话信息，服务端存储
 **/

public class Session {
    private final Channel channel;
    private final MqttConnectVarHeader mqttConnectVarHeader;
    private final MqttConnectPayload mqttConnectPayload;
    /**
     * 用户的订阅
     */
    private final Map<String, MqttQoS> subscribes = new HashMap<>(2);


    public Session(Channel channel, MqttConnectVarHeader mqttConnectVarHeader, MqttConnectPayload mqttConnectPayload) {
        this.channel = channel;
        this.mqttConnectVarHeader = mqttConnectVarHeader;
        this.mqttConnectPayload = mqttConnectPayload;
    }

    public Channel getChannel() {
        return channel;
    }

    private MqttConnectVarHeader getMqttConnectVarHeader() {
        return mqttConnectVarHeader;
    }

    MqttConnectPayload getMqttConnectPayload() {
        return mqttConnectPayload;
    }

    void addSubscribe(List<MqttTopic> topics){
        topics.forEach(topic ->subscribes.put(topic.getTopic(),topic.getQoS()));
    }

    void rmSubscribe(List<String> topics){
        topics.forEach(subscribes::remove);
    }

    /**
     * 判断发布的publish是否对应的上用户的订阅
     */
    MqttQoS isMatch(String publish){
        for(String key : subscribes.keySet()){
            if(TopicUtil.isMatch(key,publish)){
                return subscribes.get(key);
            }
        }
        return null;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Session)) {
            return false;
        }
        Session that = (Session) o;
        return Objects.equals(mqttConnectVarHeader.name(), that.getMqttConnectVarHeader().name());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mqttConnectVarHeader.name());
    }
}
