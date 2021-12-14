

package mqtt.protocol;

import mqtt.enums.MqttQoS;

/**
 * 对主题的描述
 */
public class MqttTopic{
    private final String topic;
    private final MqttQoS qoS;

    public MqttTopic(String topic, MqttQoS qoS) {
        this.topic = topic;
        this.qoS = qoS;
    }

    public String getTopic() {
        return topic;
    }

    public MqttQoS getQoS() {
        return qoS;
    }
}
