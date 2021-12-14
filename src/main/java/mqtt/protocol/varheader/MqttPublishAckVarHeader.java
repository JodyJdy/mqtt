

package mqtt.protocol.varheader;

/**
 * 发布确认 可变头部
 */
public class MqttPublishAckVarHeader  extends VaraibleHeader{
    private final int packetId;

    public MqttPublishAckVarHeader(int packetId) {
        this.packetId = packetId;
    }

    public int getPacketId() {
        return packetId;
    }
}
