

package mqtt.protocol;

import io.netty.util.internal.StringUtil;
import mqtt.enums.MqttMessageType;
import mqtt.enums.MqttQoS;
import mqtt.protocol.fixheader.MqttFixedHeader;
import mqtt.protocol.payload.Payload;
import mqtt.protocol.varheader.VaraibleHeader;

/**
 *
 * @date 2021/12/8
 **/

public class MqttMessage {
    private final MqttFixedHeader mqttFixedHeader;
    private final VaraibleHeader variableHeader;
    private final Payload payload;

    public MqttMessage(MqttFixedHeader mqttFixedHeader, VaraibleHeader variableHeader, Payload payload) {
        this.mqttFixedHeader = mqttFixedHeader;
        this.variableHeader = variableHeader;
        this.payload = payload;
    }


    public MqttFixedHeader fixedHeader() {
        return mqttFixedHeader;
    }

    public VaraibleHeader variableHeader() {
        return variableHeader;
    }

    public Payload payload() {
        return payload;
    }


    @Override
    public String toString() {
        return StringUtil.simpleClassName(this) +
                '[' +
                "fixedHeader=" + (fixedHeader() != null ? fixedHeader().toString() : "") +
                ", variableHeader=" + (variableHeader() != null ? variableHeader.toString() : "") +
                ", payload=" + (payload() != null ? payload.toString() : "") +
                ']';
    }
}
