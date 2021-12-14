

package mqtt.mqttserver;

import mqtt.enums.MqttQoS;

import java.util.Objects;

/**
 *
 * 消息的接收者， 存储了消息接收者的 id 以及 Qos级别
 **/

public class Receiver {
    private final String id;
    private final MqttQoS mqttQoS;

    Receiver(String id, MqttQoS mqttQoS) {
        this.id = id;
        this.mqttQoS = mqttQoS;
    }

    public String getId() {
        return id;
    }

    public MqttQoS getMqttQoS() {
        return mqttQoS;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Receiver)) {
            return false;
        }
        Receiver receiver = (Receiver) o;
        return Objects.equals(getId(), receiver.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }
}
