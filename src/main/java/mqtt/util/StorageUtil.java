

package mqtt.util;

import mqtt.storage.Message;
import mqtt.storage.StoredMessage;
import mqtt.storage.TopicIndexFileReaderPointer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 消息存储工具类
 **/

public class StorageUtil {
    /**
     * 使用 MappedByteBuffer写文件
     */
    public static void writeMessage(MappedByteBuffer buffer, StoredMessage sM) {
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
    public static Message readMessage(RandomAccessFile randomAccessFile) throws IOException {
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

    /**
     * 读取topic
     */
    public static List<TopicIndexFileReaderPointer> readTopic(RandomAccessFile randomAccessFile)throws IOException{
        List<TopicIndexFileReaderPointer> topics = new ArrayList<>();
        randomAccessFile.seek(0);
        while (randomAccessFile.getFilePointer() < randomAccessFile.length()){
            //主题所在的位置
            long topicPos = randomAccessFile.getFilePointer();
            long readPos = randomAccessFile.readLong();
            int len = randomAccessFile.readInt();
            byte[] bytes = new byte[len];
            randomAccessFile.read(bytes);
            String topic = new String(bytes);
            topics.add(new TopicIndexFileReaderPointer(topic,readPos,topicPos));
        }
        return topics;
    }
    /**
     * 添加topic
     */
    public static void addTopic(RandomAccessFile randomAccessFile,String topic) throws IOException {
        long topicPos = randomAccessFile.getFilePointer();
        randomAccessFile.seek(topicPos);
        //topic对应的索引文件读取到的下标
        randomAccessFile.writeLong(0);
        randomAccessFile.writeInt(topic.length());
        randomAccessFile.writeBytes(topic);
    }


}
