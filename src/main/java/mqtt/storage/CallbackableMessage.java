package mqtt.storage;

/**
 * 支持回调的消息
 */
public class CallbackableMessage extends  Message{
    private Runnable callback;

    public CallbackableMessage(String topic, byte[] msg, int qos) {
        super(topic, msg, qos);
    }

    public CallbackableMessage(int packetId, String topic, byte[] msg, int qos) {
        super(packetId, topic, msg, qos);
    }

    public void setCallback(Runnable callback) {
        this.callback = callback;
    }
    public void invokeCallback() {
        if (callback != null) {
            callback.run();
        }
    }
}
