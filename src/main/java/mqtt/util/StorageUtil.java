

package mqtt.util;

import mqtt.storage.Message;
import mqtt.storage.ReadWriteMultiFile;
import mqtt.storage.StoredMessage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 消息存储工具类
 **/

public class StorageUtil {
    /**
     * 使用 MappedByteBuffer写文件
     */
    public static void writeMessage(ReadWriteMultiFile buffer, StoredMessage sM) {
        buffer.put((byte) ((sM.getPacketId() & 0xff00) >> 8));
        buffer.put((byte) (sM.getPacketId() & 0xff));
        buffer.put((byte) sM.getQos());
        buffer.put((byte) ((sM.getTopicLen() & 0xff00) >> 8));
        buffer.put((byte) ((sM.getTopicLen() & 0xff)));
        buffer.put(sM.getTopic());
        buffer.put((byte) ((sM.getMsgLen() & 0xff00) >> 8));
        buffer.put((byte) (sM.getMsgLen() & 0xff));
        buffer.put(sM.getMsg());
    }

    /**
     * 使用随机读取，读取消息
     */
    public static Message readMessage(ReadWriteMultiFile.RandomAccessReader randomAccessFile) throws IOException {
        int packetId = (randomAccessFile.readUnsignedByte() << 8) + randomAccessFile.readUnsignedByte();
        int qos = randomAccessFile.readUnsignedByte();
        int topicLen = (randomAccessFile.readUnsignedByte() << 8) + randomAccessFile.readUnsignedByte();
        byte[] topics = new byte[topicLen];
        randomAccessFile.read(topics, 0, topics.length);
        int msgLen = (randomAccessFile.readUnsignedByte() << 8) + randomAccessFile.readUnsignedByte();
        byte[] msg = new byte[msgLen];
        randomAccessFile.read(msg, 0, msgLen);
        return StoredMessage.transToMessage(new StoredMessage(packetId, topicLen, topics, msgLen, msg, qos));
    }


    private static Pattern topicSuffix = Pattern.compile("_\\d+\\.topic$");
    /**
     * 读取topic
     */
    public static List<String> readTopic() throws IOException {
        File dir = FileUtil.getIndexFileDir();
        List<String> topics = new ArrayList<>();
        if (dir.listFiles() == null) {
            return topics;
        }
        for (File file : dir.listFiles()) {
            if (file.getName().startsWith(".")) {
               continue;
            }
            String topic = topicSuffix.split(file.getName())[0];
            if (!topics.contains(topic)) {
                topics.add(topic);
            }
        }
        return topics;
    }


}
