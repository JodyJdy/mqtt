

package mqtt.enums;

/**
 * Decoder中解码到的状态， mqtt协议由多个部分组成，解析时也要解析多个部分
 */
public enum  DecoderState {
    /**
     * 固定报文头
     */
    READ_FIXED_HEADER,
    /**
     * 可变报文头
     */
    READ_VARIABLE_HEADER,
    /**
     * 负载
     */
    READ_PAYLOAD
}
