

package mqtt.protocol.payload;

/**
 *
 * 负载  基类
 **/

public class Payload {
    /**
     * 使用的字节数
     */
    private int byteConsume;

    public int getByteConsume() {
        return byteConsume;
    }

    public void setByteConsume(int byteConsume) {
        this.byteConsume = byteConsume;
    }
}
