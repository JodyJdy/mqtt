package mqtt.storage;

import mqtt.util.StorageUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 读写 topic 对应的 索引文件
 */
public class IndexFileWriterReader {
    /**
     *索引文件对应的topic
     */
    private String topic;
    /**
     *主题在索引集合中的位置
     */
    private final long topicPos;
    /**
     * 读指针
     */
    private volatile long readPos;

    /**
     *随机读写
     */
    private final RandomAccessFile indexFile;

    /**
     * 当前读取的 消息文件的索引
     */
    private int fileIndex = -1;
    /**
     * 下次可以读取消息的时间
     */
    private long couldReadMessageTime = -1;

    public long getTopicPos() {
        return topicPos;
    }

    /**
     * 存放消息的文件
     */
    private RandomAccessFile messageFile;

    public IndexFileWriterReader(String topic,RandomAccessFile randomAccessFile,long readPos,long topicPos) {
        this.topic = topic;
        this.readPos = readPos;
        this.indexFile = randomAccessFile;
        this.topicPos = topicPos;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getReadPos() {
        return readPos;
    }

    public void setReadPos(long readPos) {
        this.readPos = readPos;
    }


    public synchronized void writeMessagePos(long messagePos,int fileIndex){
        try {
            //总是追加到最后
            indexFile.seek(indexFile.length());
            indexFile.writeLong(messagePos);
            indexFile.writeInt(fileIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public boolean couldRead(){
        return System.currentTimeMillis() > couldReadMessageTime;
    }
    public synchronized Message readMessage(){
        try {
            //此时无数据
            if(readPos == indexFile.length()){
                couldReadMessageTime = System.currentTimeMillis() + 1000L;
                return null;
            }
            indexFile.seek(readPos);
            long messagePos = indexFile.readLong();
            int fileIndex = indexFile.readInt();
            readPos = indexFile.getFilePointer();
            if(fileIndex != this.fileIndex){
                this.fileIndex = fileIndex;
                messageFile = new RandomAccessFile(new File(MessageStorage.ROOT_PATH + fileIndex +MessageStorage.MSG_TYPE),"r");
            }
            messageFile.seek(messagePos);
            return StorageUtil.readMessage(messageFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        couldReadMessageTime = System.currentTimeMillis() + 1000L;
        return null;
    }
}
