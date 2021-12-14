

package mqtt.protocol.payload;

import io.netty.util.internal.StringUtil;

import java.util.Arrays;

/**
 * 连接报文  负载
 **/

public class MqttConnectPayload extends Payload {
    private final String clientIdentifier;
    private final String willTopic;
    private final byte[] willMessage;
    private final String userName;
    private final byte[] password;
    public MqttConnectPayload(
            String clientIdentifier,
            String willTopic,
            byte[] willMessage,
            String userName,
            byte[] password) {
        this.clientIdentifier = clientIdentifier;
        this.willTopic = willTopic;
        this.willMessage = willMessage;
        this.userName = userName;
        this.password = password;
    }

    public String getClientIdentifier() {
        return clientIdentifier;
    }

    public String getWillTopic() {
        return willTopic;
    }

    public byte[] getWillMessage() {
        return willMessage;
    }

    public String getUserName() {
        return userName;
    }

    public byte[] getPassword() {
        return password;
    }
    @Override
    public String toString() {
        return StringUtil.simpleClassName(this) +
                '[' +
                "clientIdentifier=" + clientIdentifier +
                ", willTopic=" + willTopic +
                ", willMessage=" + Arrays.toString(willMessage) +
                ", userName=" + userName +
                ", password=" + Arrays.toString(password) +
                ']';
    }
}
