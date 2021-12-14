

package mqtt.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.CharsetUtil;

import static io.netty.buffer.ByteBufUtil.reserveAndWriteUtf8;

/**
 *字节处理工具类
 *
 **/

public class ByteUtil {
    /**
     * 解码 MSB LSB 返回其对应的整数
     */
    public static int decodeMsbLsb(ByteBuf buf){
        //高位
        short msb = buf.readUnsignedByte();
        //低位
        short lsb = buf.readUnsignedByte();
        return (msb<<8)|lsb;
    }
    /**
     * Byte 的一部分转成 字符串
     *
     */
    public static String decodeToString(ByteBuf buf,int len){
        String str = buf.toString(buf.readerIndex(),len, CharsetUtil.UTF_8);
        buf.skipBytes(len);
        return str;
    }
    /**
     * ByteBuf 的一部分转成 byte[]
     *
     */
    public static byte[] decodeToByteArray(ByteBuf buf,int len){
        byte[] result = new byte[len];
        buf.readBytes(result);
        return result;
    }

    /**
     * 返回字符串占据的byte
     */
    public static int utf8Bytes(String str){
        return str == null ? 0 : ByteBufUtil.utf8Bytes(str);
    }
    /**
     * 获取长度占据的字节
     */
    public static int getLengthByteNum(int num){
        int count = 0;
        do {
            num /= 128;
            count++;
        } while (num > 0);
        return count;
    }
    /**
     * 字符串写入
     */
    public static void writeString(ByteBuf buf,String s){
        int utfBytesLen = s.getBytes(CharsetUtil.UTF_8).length;
        buf.ensureWritable(utfBytesLen + 2);
        buf.writeShort(utfBytesLen);
        reserveAndWriteUtf8(buf, s, utfBytesLen);
    }

}
