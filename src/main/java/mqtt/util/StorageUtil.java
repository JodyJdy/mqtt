

package mqtt.util;

import mqtt.storage.Message;
import mqtt.storage.StoredMessage;
import mqtt.storage.TopicIndexFileReaderPointer;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 消息存储工具类
 **/

public class StorageUtil {
    private static final Logger logger = LoggerFactory.getLogger(StorageUtil.class);
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
    public static Message readMessage(RandomAccessFile randomAccessFile){
        try {
            int packetId = (randomAccessFile.readUnsignedByte() << 8) + randomAccessFile.readUnsignedByte();
            int qos = randomAccessFile.readUnsignedByte();
            int topicLen = (randomAccessFile.readUnsignedByte() << 8) + randomAccessFile.readUnsignedByte();
            byte[] topics = new byte[topicLen];
            randomAccessFile.read(topics, 0, topics.length);
            int msgLen = (randomAccessFile.readUnsignedByte() << 8) + randomAccessFile.readUnsignedByte();
            byte[] msg = new byte[msgLen];
            randomAccessFile.read(msg, 0, msgLen);
            return StoredMessage.transToMessage(new StoredMessage(packetId, topicLen, topics, msgLen, msg, qos));
        }catch (Exception e){
            logger.error("读message异常",e);
            return null;
        }
    }

    /**
     * 读取topic
     */
    public static List<TopicIndexFileReaderPointer> readTopic(RandomAccessFile randomAccessFile){
        List<TopicIndexFileReaderPointer> topics = new ArrayList<>();
        try {
            randomAccessFile.seek(0);
            if(randomAccessFile.length() == 0){
                return topics;
            }
            while (randomAccessFile.getFilePointer() < randomAccessFile.length()) {
                //主题所在的位置
                long topicPos = randomAccessFile.getFilePointer();
                long readPos = randomAccessFile.readLong();
                int fileIndex = randomAccessFile.readInt();
                int len = randomAccessFile.readInt();
                byte[] bytes = new byte[len];
                randomAccessFile.read(bytes);
                String topic = new String(bytes);
                topics.add(new TopicIndexFileReaderPointer(topic, readPos, topicPos, fileIndex));
            }
        }catch (Exception e){
            logger.error("读topic异常",e);
        }
        return topics;
    }
    public static synchronized void writeTopic(RandomAccessFile randomAccessFile,List<TopicIndexFileReaderPointer> topics){
        long len = 0;
        try {
            randomAccessFile.seek(0);
            for(TopicIndexFileReaderPointer topic : topics){
                randomAccessFile.writeLong(topic.getReadPos());
                randomAccessFile.writeInt(topic.getReadFileIndex());
                randomAccessFile.writeInt(topic.getTopic().length());
                randomAccessFile.writeBytes(topic.getTopic());
                len+= 8 + 4 + 4 + topic.getTopic().length();
            }
            //对文件进行截断
            randomAccessFile.setLength(len);
        } catch (IOException e) {
            logger.error("写topic异常",e);
        }

    }
    /**
     * 添加topic
     */
    public static synchronized void addTopic(RandomAccessFile randomAccessFile,String topic,int readFileIndex) {
        try {
            long topicPos = randomAccessFile.length();
            randomAccessFile.seek(topicPos);
            //topic对应的索引文件读取到的下标，新建时，是0
            randomAccessFile.writeLong(0);
            //索引文件下标指向的消息文件
            randomAccessFile.writeInt(readFileIndex);
            randomAccessFile.writeInt(topic.length());
            randomAccessFile.writeBytes(topic);
        }catch (Exception e){
            logger.error("添加topic到文件中失败",e);
        }
    }


}
