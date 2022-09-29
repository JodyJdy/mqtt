

package mqtt.storage;


import mqtt.util.StorageUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
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
    /**
     * 执行文件刷盘的定时任务
     */
    private final ScheduledThreadPoolExecutor sync = new ScheduledThreadPoolExecutor(1);
    /**
     * 最大文件大小, 50 MB
     */
    private static final int MAX_FILE_SIZE = 1024 * 1024 * 50;
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
    private final Map<Integer, String> hashCode2Topic = new ConcurrentHashMap<>();
    /**
     * 存储topic->indexFile 的映射
     */
    private final Map<String,IndexFileWriterReader> topic2IndexFileWriterReader = new ConcurrentHashMap<>();

    public MessageStorage() throws IOException {
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
        initIndexSet();
        syncTask();
    }

    /**
     * 初始化索引相关
     */
    private void initIndexSet() throws IOException {
        File indexFile = new File(INDEX_PATH + TOPIC_SET);
        if(!indexFile.exists()){
            FileUtils.touch(indexFile);
        }
        //用于读写 mqtt.topic文件
        topicFileAccess = new RandomAccessFile(indexFile, "rw");
        //获取所有主题
        List<TopicIndexFileReaderPointer> topics = StorageUtil.readTopic(topicFileAccess);
        //放入map中
        for(TopicIndexFileReaderPointer topic : topics){
            addTopic2Map(topic.getTopic(),topic.getReadPos(),topic.getTopicPos());
        }
    }
    private boolean addTopic2Map(String topic,long readPos,long topicPos) throws IOException {
        if(hashCode2Topic.containsKey(topic.hashCode())){
            return false;
        }
        hashCode2Topic.put(topic.hashCode(), topic);
        File indexFile = new File(INDEX_PATH + topic.hashCode() + INDEX_TYPE);
        if(!indexFile.exists()){
            FileUtils.touch(indexFile);
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(indexFile,"rw");
        topic2IndexFileWriterReader.put(topic,new IndexFileWriterReader(topic,randomAccessFile,readPos,topicPos));
        return true;
    }

    /**
     * 添加主题,
     * @param topic 名称
     * @param readPos 主题索引文件的读取位置
     * @param topicPos 主题在 mqtt.topic中存储的位置
     */
    public synchronized void  addTopic(String topic,long readPos,long topicPos) throws IOException {
        if(addTopic2Map(topic,readPos,topicPos)) {
             StorageUtil.addTopic(topicFileAccess, topic);
        }
    }

    private void syncTask() {
        //文件写指针1s刷新一次,随机写性能慢
        sync.scheduleAtFixedRate(this::writeFileWriterPointer, 1, 1, TimeUnit.SECONDS);
        //将 内存中的数据刷盘,5s一次
        sync.scheduleAtFixedRate(() -> writer.force(), 0, 5, TimeUnit.SECONDS);
        //定时更新 主题索引文件的读取位置
        sync.scheduleAtFixedRate(this::updateIndexFileReadPos,1,1,TimeUnit.SECONDS);
    }

    /**
     * 向文件写数据
     */
    void writeMessage(Message msg) throws IOException {
        StoredMessage storedMessage = Message.transToStoredMessage(msg);
        long pos = writeMessage(storedMessage);
        String topic = msg.getTopic();
        //主题不存在，创建主题
        if (!hashCode2Topic.containsKey(topic.hashCode())) {
            //新创建的主题，一定在mqtt.topic文件的尾部
            addTopic(topic,0L,topicFileAccess.length());
        }
        //将消息位置，记录在 主题对应的索引文件里面
        topic2IndexFileWriterReader.get(topic).writeMessagePos(pos, messageFileWriterPointer.getWriteFile());
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
        IndexFileWriterReader indexFileWriterReader = topic2IndexFileWriterReader.get(topic);
        if(indexFileWriterReader.couldRead()){
            return indexFileWriterReader.readMessage();
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
    private void updateIndexFileReadPos(){
        topic2IndexFileWriterReader.forEach(
                (s, indexFileWriterReader) -> {
                    try {
                        topicFileAccess.seek(indexFileWriterReader.getTopicPos());
                        topicFileAccess.writeLong(indexFileWriterReader.getReadPos());
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
     * 读文件写指针
     */
    private MessageFileWriterPointer readFileWriterPointer() throws IOException {
        messageFilePointerAccess.seek(0);
        return new MessageFileWriterPointer(messageFilePointerAccess.readLong(), messageFilePointerAccess.readInt());
    }
    /**
     * 获取所有在使用中的topic
     */
    public Set<String> topicSet(){
        return topic2IndexFileWriterReader.keySet();
    }

}
