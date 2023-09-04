

package mqtt.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.CharsetUtil;

import java.nio.ByteBuffer;

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


    public static byte[] char2Bytes(char ch){
        ByteBuffer b = ByteBuffer.wrap(new byte[2]);
        b.putChar(ch);
        return b.array();
    }

    public static byte[] short2Bytes(short sh){
        ByteBuffer b = ByteBuffer.wrap(new byte[2]);
        b.putShort(sh);
        return b.array();
    }
    public static byte[] int2Bytes(int i) {
        ByteBuffer b = ByteBuffer.wrap(new byte[4]);
        b.putInt(i);
        return b.array();
    }
    public static byte[] float2Bytes(float f) {
        return int2Bytes(Float.floatToIntBits(f));
    }

    public static byte[] long2Bytes(long l) {
        ByteBuffer b = ByteBuffer.wrap(new byte[8]);
        b.putLong(l);
        return b.array();
    }

    public static byte[] double2Bytes(double d) {
        return long2Bytes(Double.doubleToLongBits(d));
    }

    public static short bytes2Short(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes);
        return b.getShort();
    }

    public static char bytes2Char(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes);
        return b.getChar();
    }
    public static float bytes2Float(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes);
        return b.getFloat();
    }


    public static int bytes2Int(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes);
        return b.getInt();
    }
    public static long bytes2Long(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes);
        return b.getLong();
    }
    public static double bytes2Double(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes);
        return b.getDouble();
    }

}
