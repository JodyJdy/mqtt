

package mqtt.protocol.payload;

import java.util.List;

/**
 *
 * 订阅确认报文 负载
 *
 **/

public class MqttSubAckPayload extends Payload {
    private  final  List<Integer> resultCode;

    public MqttSubAckPayload(List<Integer> resultCode) {
        this.resultCode = resultCode;
    }

    public List<Integer> getResultCode() {
        return resultCode;
    }
}
