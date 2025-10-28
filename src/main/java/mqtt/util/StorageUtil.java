

package mqtt.util;

import mqtt.storage.Message;
import mqtt.storage.ReadWriteMultiFile;
import mqtt.storage.StoredMessage;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 消息存储工具类
 **/

public class StorageUtil {

    /**
     * 每条消息的内容写入同一个临时的缓存中，避免重复创建byte[]
     */
    private static final ByteArrayOutputStream tempBuffer = new ByteArrayOutputStream(1024);
    /**
     * 使用 MappedByteBuffer写文件
     */
    public static void writeMessage(ReadWriteMultiFile buffer, StoredMessage sM) {
        tempBuffer.reset();
        try {
            tempBuffer.write((byte) ((sM.getPacketId() & 0xff00) >> 8));
            tempBuffer.write((byte) (sM.getPacketId() & 0xff));
            tempBuffer.write((byte) sM.getQos());
            tempBuffer.write((byte) ((sM.getTopicLen() & 0xff00) >> 8));
            tempBuffer.write((byte) ((sM.getTopicLen() & 0xff)));
            tempBuffer.write(sM.getTopic());
            tempBuffer.write((byte) ((sM.getMsgLen() & 0xff00) >> 8));
            tempBuffer.write((byte) (sM.getMsgLen() & 0xff));
            tempBuffer.write(sM.getMsg());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buffer.put(tempBuffer.toByteArray());
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


    private static final Pattern topicSuffix = Pattern.compile("_\\d+\\.topic$");
    /**
     * 读取topic
     */
    public static List<String> readTopic() {
        File dir = FileUtil.getIndexFileDir();
        List<String> topics = new ArrayList<>();
        File[] files = dir.listFiles();
        if (files == null) {
            return topics;
        }
        for (File file : files) {
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
