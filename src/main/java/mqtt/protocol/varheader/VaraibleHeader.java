

package mqtt.protocol.varheader;

/**
 *
 * 可变报文
 **/

public class VaraibleHeader {

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
