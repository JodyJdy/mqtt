

package mqtt.protocol.payload;

import io.netty.util.CharsetUtil;

/**
 *
 * 消息发布报文 负载
 **/

public class MqttPublishPayload extends Payload {
    private final byte[] bytes;

    public MqttPublishPayload(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return "MqttPublishPayload{" +
                "bytes=" +new String(bytes,CharsetUtil.UTF_8) +
                '}';
    }
}
