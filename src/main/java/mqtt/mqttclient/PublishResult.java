

package mqtt.mqttclient;

/**
 * 消息发布时异步的，可通过 PublishResult等待到结果
 **/

public class PublishResult {
    private final Ack ack;
    private final int packetId;

    public PublishResult(Ack ack, int packetId) {
        this.ack = ack;
        this.packetId = packetId;
    }

    public void waitForAck(){
        ack.waitAck(packetId);
    }
}
