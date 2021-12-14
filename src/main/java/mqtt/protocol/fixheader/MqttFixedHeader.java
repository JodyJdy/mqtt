

package mqtt.protocol.fixheader;

import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.StringUtil;
import mqtt.enums.MqttMessageType;
import mqtt.enums.MqttQoS;

/**
 * 固定报文头
 **/

public class MqttFixedHeader {
    private final MqttMessageType messageType;
    private final boolean isDup;
    private final MqttQoS qosLevel;
    private final boolean isRetain;
    private final int remainingLength;

    public MqttFixedHeader(
            MqttMessageType messageType,
            boolean isDup,
            MqttQoS qosLevel,
            boolean isRetain,
            int remainingLength) {
        this.messageType = ObjectUtil.checkNotNull(messageType, "messageType");
        this.isDup = isDup;
        this.qosLevel = ObjectUtil.checkNotNull(qosLevel, "qosLevel");
        this.isRetain = isRetain;
        this.remainingLength = remainingLength;
    }

    public MqttMessageType messageType() {
        return messageType;
    }

    public boolean isDup() {
        return isDup;
    }

    public MqttQoS qosLevel() {
        return qosLevel;
    }

    public boolean isRetain() {
        return isRetain;
    }

    public int remainingLength() {
        return remainingLength;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this) +
                '[' +
                "messageType=" + messageType +
                ", isDup=" + isDup +
                ", qosLevel=" + qosLevel +
                ", isRetain=" + isRetain +
                ", remainingLength=" + remainingLength +
                ']';
    }
}
