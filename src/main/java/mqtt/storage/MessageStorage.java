

package mqtt.storage;


import mqtt.util.StorageUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static mqtt.util.FileUtil.*;

/**
 * 消息存储
 */
public class MessageStorage {

    /**
     * 用于 读取/写入消息
     */
    private final ReadWriteMultiFile messageFile;

    /**
     * 存储topic->topic索引文件 indexFile 的映射
     */
    private final Map<String, IndexFileReader> topic2IndexFileWriterReader = new ConcurrentHashMap<>();

    public MessageStorage() throws IOException {
        this.messageFile = new ReadWriteMultiFile("mqtt", MSG_TYPE, ROOT_PATH, MAX_FILE_SIZE);
        initIndexSet();
    }

    /**
     * 初始化索引相关
     */
    private void initIndexSet() throws IOException {
        //获取所有主题
        List<String> topics = StorageUtil.readTopic();
        //放入map中
        for (String topic : topics) {
            addTopic2Map(topic);
        }
    }

    private boolean addTopic2Map(String topic) throws IOException {
        if (topic2IndexFileWriterReader.containsKey(topic)) {
            return false;
        }
        topic2IndexFileWriterReader.put(topic, new IndexFileReader(topic, messageFile.getRandomAccessReader()));
        return true;
    }

    /**
     * 添加主题,
     *
     * @param topic 名称
     */
    public synchronized void addTopic(String topic) throws IOException {
        addTopic2Map(topic);
    }


    /**
     * 向文件写数据
     */
    void writeMessage(Message msg) throws IOException {
        StoredMessage storedMessage = Message.transToStoredMessage(msg);
        long globalPos = writeMessage(storedMessage);
        String topic = msg.getTopic();
        //主题不存在，创建主题
        if (!topic2IndexFileWriterReader.containsKey(topic)) {
            //新创建的主题，一定在mqtt.topic文件的尾部
            addTopic(topic);
        }
        //将消息位置，记录在 主题对应的索引文件里面
        topic2IndexFileWriterReader.get(topic).writeMessagePos(globalPos);
    }

    /**
     * 返回写入消息的指针位置
     */
    private long writeMessage(StoredMessage msg) {
        //获取消息全局写入的位置
        long pos = messageFile.getGlobalWritePos();
        StorageUtil.writeMessage(messageFile, msg);
        return pos;
    }

    /**
     * 从文件里面读取消息
     */
    public Message readMessage(String topic) {
        IndexFileReader indexFileReader = topic2IndexFileWriterReader.get(topic);
        return indexFileReader.readMessage();
    }

    public void stopRead(String topic) {
        topic2IndexFileWriterReader.get(topic).stopRead();
    }

}
