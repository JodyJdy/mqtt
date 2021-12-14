

package mqtt.protocol.varheader;

/**
 *订阅ack报文 可变头部
 **/

public class MqttSubAckVarHeader extends VaraibleHeader {
    private final int packetId;

    public MqttSubAckVarHeader(int packetId) {
        this.packetId = packetId;
    }

    public int getPacketId() {
        return packetId;
    }
}
