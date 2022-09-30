

package mqtt.storage;


import mqtt.mqttserver.Receiver;
import mqtt.mqttserver.UserSessions;
import mqtt.util.StorageUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 消息存储
 */
public class MessageStorage {
    private static final Logger logger = LoggerFactory.getLogger(MessageStorage.class);
    /**
     * 执行文件刷盘的定时任务
     */
    private final ScheduledThreadPoolExecutor sync = new ScheduledThreadPoolExecutor(2);
    /**
     * 主题不活跃时存活时间
     */
    private static final int TOPIC_DEAD_EXIST_TIME = 10 * 1000;
    /**
     * 最大文件大小, 50 MB
     */
    private static final int MAX_FILE_SIZE = 1024;//1024 * 1024 * 50;
    /**
     * 消息存储使用的文件格式
     */
    final static String MSG_TYPE = ".msg";
    /**
     * 存储所有topic 文件
     */
    private final static String TOPIC_SET = "mqtt.topic";
    /**
     * 索引文件的文件格式
     */
    private final static String INDEX_TYPE = ".index";
    /**
     * 文件写指针使用的文件名称
     */
    private final static String FILE_POINTER_NAME = "mqtt.pos";
    /**
     * 文件存储根路径
     */
    final static String ROOT_PATH = "store/";
    /**
     * 文件索引路径
     */
    private final static String INDEX_PATH = ROOT_PATH + "index/";
    /**
     * 文件写指针
     */
    private final MessageFileWriterPointer messageFileWriterPointer;
    /**
     * 用来访问 MessageFileWriterPointer所在的文件
     */
    private final RandomAccessFile messageFilePointerAccess;
    /**
     * 用来访问 mqtt.topic文件
     */
    private RandomAccessFile topicFileAccess;
    /**
     * 用来向文件写入数据，使用了内存文件映射，需要定时的刷盘
     */
    private MappedByteBuffer writer;

    /**
     * topic 的hashCode到topic的映射
     */
    private final Map<Integer, String> topicMap = new ConcurrentHashMap<>();
    /**
     * 存储topic->indexFile 的映射
     */
    private final Map<String, IndexFileWriterReader> topic2IndexFile = new ConcurrentHashMap<>();
    /**
     * 用于读取对应的 消息文件
     */
    private final Map<Integer, RandomAccessFile> messageFileReaderMap = new ConcurrentHashMap<>();


    private final UserSessions userSessions;

    public MessageStorage(UserSessions userSessions) throws IOException {
        this.userSessions = userSessions;
        //获取文件写指针
        File pointer = new File(ROOT_PATH + FILE_POINTER_NAME);
        if (!pointer.exists()) {
            FileUtils.touch(pointer);
            messageFilePointerAccess = new RandomAccessFile(pointer, "rw");
            messageFileWriterPointer = new MessageFileWriterPointer();
            writeFileWriterPointer();
        } else {
            messageFilePointerAccess = new RandomAccessFile(pointer, "rw");
            messageFileWriterPointer = readFileWriterPointer();
        }
        //获取当前在写的文件的 MappedFile
        File writerAccessFile = new File(ROOT_PATH + messageFileWriterPointer.getWriteFile() + MSG_TYPE);
        if (!writerAccessFile.exists()) {
            FileUtils.touch(writerAccessFile);
        }
        RandomAccessFile temp = new RandomAccessFile(writerAccessFile, "rw");
        writer = temp.getChannel().map(FileChannel.MapMode.READ_WRITE, messageFileWriterPointer.getWritePos(), MAX_FILE_SIZE);
        loadIndexFromFile();
        syncTask();
    }

    /**
     * 初始化索引相关
     */
    private void loadIndexFromFile() {
        try {
            File indexFile = new File(INDEX_PATH + TOPIC_SET);
            if (!indexFile.exists()) {
                FileUtils.touch(indexFile);
            }
            //用于读写 mqtt.topic文件
            topicFileAccess = new RandomAccessFile(indexFile, "rw");
            //获取所有主题
            List<TopicIndexFileReaderPointer> topics = StorageUtil.readTopic(topicFileAccess);
            //放入map中
            for (TopicIndexFileReaderPointer topic : topics) {
                addTopic2Map(topic);
            }
        } catch (Exception e) {
           logger.error("初始化索引文件失败",e);
        }
    }

    /**
     * 使用默认参数添加主题到 map中
     */
    private synchronized boolean addTopic2Map(String topic) {
        TopicIndexFileReaderPointer topicPointer;
        try {
            topicPointer = new TopicIndexFileReaderPointer(topic, 0L, topicFileAccess.length(), messageFileWriterPointer.getWriteFile());
        } catch (IOException e) {
            logger.error("添加主题失败", e);
            return false;
        }
        return addTopic2Map(topicPointer);
    }

    /**
     * 使用从文件中读取的参数， 添加主题到map中
     */
    private synchronized boolean addTopic2Map(TopicIndexFileReaderPointer topicPointer) {
        String topic = topicPointer.getTopic();
        //添加到userSessions中的缓冲
        userSessions.addUsingTopic(topic);
        int hashCode = topicPointer.getTopic().hashCode();
        if (topicMap.containsKey(hashCode)) {
            return false;
        }
        topicMap.put(hashCode, topic);
        File indexFile = new File(INDEX_PATH + hashCode + INDEX_TYPE);
        if (!indexFile.exists()) {
            try {
                FileUtils.touch(indexFile);
            } catch (IOException e) {
                logger.error("主题索引文件创建失败", e);
                return false;
            }
        }
        topic2IndexFile.put(topic, new IndexFileWriterReader(topic, indexFile, topicPointer.getReadPos(), topicPointer.getTopicPos(), topicPointer.getReadFileIndex()
                , messageFileWriterPointer.getWriteFile()));
        return true;
    }

    /**
     * 添加主题,
     *
     * @param topic 名称
     */
    public synchronized void createTopic(String topic) {
        if (addTopic2Map(topic)) {
            StorageUtil.addTopic(topicFileAccess, topic, messageFileWriterPointer.getWriteFile());
        }
    }

    private void syncTask() {
        //文件写指针1s刷新一次,随机写性能慢
        sync.scheduleAtFixedRate(this::writeFileWriterPointer, 1, 1, TimeUnit.SECONDS);
        //将 内存中的数据刷盘,5s一次
        sync.scheduleAtFixedRate(() -> writer.force(), 0, 5, TimeUnit.SECONDS);
        //定时更新 主题索引文件的读取位置
        sync.scheduleAtFixedRate(this::updateIndexFileReadPos, 1, 1, TimeUnit.SECONDS);
        //定时删除 不活跃的topic
        sync.scheduleAtFixedRate(this::deleteTopics, 1, 10, TimeUnit.SECONDS);
    }

    /**
     * 向文件写数据
     */
    void writeMessage(Message msg) throws IOException {
        StoredMessage storedMessage = Message.transToStoredMessage(msg);
        long pos = writeMessage(storedMessage);
        storeMessagePos(pos, msg);
    }

    /**
     * 存储消息位置
     */
    void storeMessagePos(long pos, Message message) {
        String topic = message.getTopic();
        //主题不存在，创建主题
        if (!topicMap.containsKey(topic.hashCode())) {
            //新创建的主题，一定在mqtt.topic文件的尾部
            createTopic(topic);
        }
        //将消息位置，记录在 主题对应的索引文件里面
        topic2IndexFile.get(topic).writeMessagePos(pos, messageFileWriterPointer.getWriteFile());
    }

    /**
     * 返回写入消息的指针位置
     */
    private long writeMessage(StoredMessage msg) throws IOException {
        long pos = messageFileWriterPointer.getWritePos();
        int size = msg.getBytesSize();
        //可以写入
        if (MAX_FILE_SIZE - pos >= size) {
            messageFileWriterPointer.setWritePos(pos + size);
            StorageUtil.writeMessage(writer, msg);
            return pos;
        } else {
            createWriter();
            return writeMessage(msg);
        }
    }

    /**
     * 从文件里面读取消息
     */
    Message readMessage(String topic) throws IOException {
        IndexFileWriterReader indexFileWriterReader = topic2IndexFile.get(topic);
        if (indexFileWriterReader == null) {
            return null;
        }
        if (indexFileWriterReader.couldRead()) {
            MessagePos msgPos = indexFileWriterReader.readMessagePos();
            if (msgPos == null) {
                return null;
            }
            RandomAccessFile reader = messageFileReaderMap.get(msgPos.getFileIndex());
            if (reader == null) {
                reader = new RandomAccessFile(ROOT_PATH + msgPos.getFileIndex() + MSG_TYPE, "r");
                messageFileReaderMap.put(msgPos.getFileIndex(), reader);
            }
            reader.seek(msgPos.getPos());
            return StorageUtil.readMessage(reader);
        }
        return null;
    }

    /**
     * 更换一个新的文件写数据
     */
    private void createWriter() throws IOException {
        synchronized (messageFileWriterPointer) {
            int writeFile = messageFileWriterPointer.getWriteFile() + 1;
            File newFile = new File(ROOT_PATH + writeFile + MSG_TYPE);
            FileUtils.touch(newFile);
            RandomAccessFile randomAccessFile = new RandomAccessFile(newFile, "rw");
            writer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, MAX_FILE_SIZE);
            messageFileWriterPointer.setWritePos(0);
            messageFileWriterPointer.setWriteFile(writeFile);
            //文件指针写入文件
            writeFileWriterPointer();
        }
    }

    /**
     * 更新 每个索引文件 读取的下标
     */
    private synchronized void updateIndexFileReadPos() {
        topic2IndexFile.forEach(
                (s, indexFileWriterReader) -> {
                    try {
                        topicFileAccess.seek(indexFileWriterReader.getTopicPos());
                        topicFileAccess.writeLong(indexFileWriterReader.getReadPos());
                        topicFileAccess.writeInt(indexFileWriterReader.getReadFileIndex());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        );
    }

    /**
     * 写文件写指针
     */
    private void writeFileWriterPointer() {
        synchronized (messageFileWriterPointer) {
            try {
                messageFilePointerAccess.seek(0);
                messageFilePointerAccess.writeLong(messageFileWriterPointer.getWritePos());
                messageFilePointerAccess.writeInt(messageFileWriterPointer.getWriteFile());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 读取文件写指针
     */
    private MessageFileWriterPointer readFileWriterPointer() throws IOException {
        messageFilePointerAccess.seek(0);
        return new MessageFileWriterPointer(messageFilePointerAccess.readLong(), messageFilePointerAccess.readInt());
    }


    /**
     * 删除不在使用的topic
     */
    private synchronized void deleteTopics() {
        Set<String> deletes = new HashSet<>();
        //获取不再写入的topic
        topic2IndexFile.forEach((k, indexFileWriterReader) -> {
            if (indexFileWriterReader.read2End() && System.currentTimeMillis() - indexFileWriterReader.lastModifyTime() > TOPIC_DEAD_EXIST_TIME) {
                deletes.add(k);
            }
        });
        //过滤掉还在使用中的topic
        deletes.removeIf(d -> {
            Set<Receiver> r = userSessions.getReceiver(d);
            return r != null && !r.isEmpty();
        });
        List<TopicIndexFileReaderPointer> topics = StorageUtil.readTopic(topicFileAccess);
        int before = topics.size();
        topics.removeIf(o -> deletes.contains(o.getTopic()));
        //什么都不需要做
        if(before == topics.size()){
            return;
        }
        //写入文件中
        StorageUtil.writeTopic(topicFileAccess, topics);
        //有必要全部清空，重建索引文件
        topicMap.clear();
        deletes.forEach(d -> {
            logger.info("删除订阅:{}", d);
            topic2IndexFile.get(d).close();
            //删除订阅对应的文件
            File indexFile = new File(INDEX_PATH + d.hashCode() + INDEX_TYPE);
            try {
                FileUtils.delete(indexFile);
            } catch (IOException e) {
                logger.error("删除索引文件失败",e);
            }
        });
        topic2IndexFile.clear();
        loadIndexFromFile();
        userSessions.deleteUsingTopics(deletes);
    }

}
