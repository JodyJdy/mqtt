

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

import static mqtt.util.FileUtil.*;

/**
 * 消息存储
 */
public class MessageStorage {
    /**
     * 执行文件刷盘的定时任务
     */
    private final ScheduledThreadPoolExecutor sync = new ScheduledThreadPoolExecutor(1);

    /**
     * 文件写指针
     */
    private final MessageFileWriterPointer messageFileWriterPointer;
    /**
     * 用来访问 MessageFileWriterPointer所在的文件
     */
    private final RandomAccessFile messageFilePointerAccess;

    /**
     * 用来向文件写入数据，使用了内存文件映射，需要定时的刷盘
     */
    private MappedByteBuffer writer;

    /**
     * 存储topic->topic索引文件 indexFile 的映射
     */
    private final Map<String, IndexFileWriterReader> topic2IndexFileWriterReader = new ConcurrentHashMap<>();

    public MessageStorage() throws IOException {
        //获取文件写指针
        File pointer = getMessageFileWriterPointer();
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
        File writerAccessFile = getMessageFile(messageFileWriterPointer.getWriteFile());
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
        File indexFile = getIndexFile(topic);
        if (!indexFile.exists()) {
            FileUtils.touch(indexFile);
        }
        IndexFileReadPointer indexFileReadPointer = new IndexFileReadPointer(getIndexFileReadPosFile(topic));
        topic2IndexFileWriterReader.put(topic, new IndexFileWriterReader(topic, indexFile, indexFileReadPointer));
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

    private void syncTask() {
        //文件写指针1s刷新一次,随机写性能慢
        sync.scheduleAtFixedRate(this::writeFileWriterPointer, 1, 1, TimeUnit.SECONDS);
        //将 内存中的数据刷盘,5s一次
        sync.scheduleAtFixedRate(() -> writer.force(), 0, 5, TimeUnit.SECONDS);
        //定时更新 主题索引文件的读取位置
        sync.scheduleAtFixedRate(this::updateIndexFileReadPos, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * 向文件写数据
     */
    void writeMessage(Message msg) throws IOException {
        StoredMessage storedMessage = Message.transToStoredMessage(msg);
        long pos = writeMessage(storedMessage);
        String topic = msg.getTopic();
        //主题不存在，创建主题
        if (!topic2IndexFileWriterReader.containsKey(topic)) {
            //新创建的主题，一定在mqtt.topic文件的尾部
            addTopic(topic);
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
    public Message readMessage(String topic) {
        IndexFileWriterReader indexFileWriterReader = topic2IndexFileWriterReader.get(topic);
        return indexFileWriterReader.readMessage();
    }

    public void stopRead(String topic) {
        topic2IndexFileWriterReader.get(topic).stopRead();
    }

    /**
     * 更换一个新的文件写数据
     */
    private void createWriter() throws IOException {
        synchronized (messageFileWriterPointer) {
            int writeFile = messageFileWriterPointer.getWriteFile() + 1;
            File newFile = getMessageFile(writeFile);
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
    private void updateIndexFileReadPos() {
        topic2IndexFileWriterReader.forEach(
                (s, indexFileWriterReader) -> {
                    indexFileWriterReader.getIndexFileReadPointer().flush();
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
    public Set<String> topicSet() {
        return topic2IndexFileWriterReader.keySet();
    }

}
