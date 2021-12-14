

package mqtt.protocol.varheader;

/**
 *订阅取消 ack报文 可变头部
 **/

public class MqttSubCancelAckVarHeader extends VaraibleHeader {
    private final int packetId;

    public MqttSubCancelAckVarHeader(int packetId) {
        this.packetId = packetId;
    }

    public int getPacketId() {
        return packetId;
    }
}
