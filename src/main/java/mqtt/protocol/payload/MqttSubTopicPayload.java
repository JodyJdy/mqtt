

package mqtt.protocol.payload;

import mqtt.protocol.MqttTopic;

import java.util.List;

/**
 *订阅报文 负载
 *
 **/

public class MqttSubTopicPayload extends Payload {
    private final List<MqttTopic> topics;

    public List<MqttTopic> getTopics() {
        return topics;
    }

    public MqttSubTopicPayload(List<MqttTopic> topics) {

        this.topics = topics;
    }
}

