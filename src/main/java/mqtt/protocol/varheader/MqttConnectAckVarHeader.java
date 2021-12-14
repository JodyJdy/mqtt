

package mqtt.protocol.varheader;

import io.netty.util.internal.StringUtil;
import mqtt.enums.MqttConnectReturnCode;

/**
 * 连接ack 报文可变头部
 **/

public class MqttConnectAckVarHeader extends VaraibleHeader {
    private final boolean sessionPresent;
    private final MqttConnectReturnCode returnCode;
    public MqttConnectAckVarHeader(boolean sessionPresent, MqttConnectReturnCode returnCode) {
        this.sessionPresent = sessionPresent;
        this.returnCode = returnCode;
    }

    public boolean isSessionPresent() {
        return sessionPresent;
    }

    public MqttConnectReturnCode getReturnCode() {
        return returnCode;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this) +
                '[' +
                "returnCode=" + returnCode +
                ", sessionPresent=" + sessionPresent +
                ']';
    }



}
