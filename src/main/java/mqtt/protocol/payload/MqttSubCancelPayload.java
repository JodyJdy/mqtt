

package mqtt.protocol.payload;

import java.util.List;

/**
 *
 * 订阅取消 报文 负载
 **/

public class MqttSubCancelPayload extends Payload {
    private final List<String> subscribes;

    public MqttSubCancelPayload(List<String> subscribes) {
        this.subscribes = subscribes;
    }

    public List<String> getSubscribes() {
        return subscribes;
    }
}
