package mqtt.storage;

import java.io.File;
import java.io.FileNotFoundException;
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
    private RandomAccessFile indexFile;
    private final File file;
    /**
     * 当前写入的消息所在的文件
     */
    private  int writeFileIndex;
    /**
     * 当前读取的消息所在的文件
     */
    private int readFileIndex;
    /**
     * 下次可以读取消息的时间
     */
    private long couldReadMessageTime = -1;

    public long getTopicPos() {
        return topicPos;
    }


    public IndexFileWriterReader(String topic, File file, long readPos, long topicPos, int writeFileIndex,int readFileIndex) {
        this.topic = topic;
        this.readPos = readPos;
        this.file = file;
        try {
            this.indexFile = new RandomAccessFile(file,"rw");
        } catch (FileNotFoundException ignored) {
        }
        this.topicPos = topicPos;
        this.writeFileIndex = writeFileIndex;
        this.readFileIndex = readFileIndex;
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

    public int getReadFileIndex() {
        return readFileIndex;
    }

    public void setReadPos(long readPos) {
        this.readPos = readPos;
    }


    public synchronized void writeMessagePos(long messagePos,int fileIndex){
        try {
            //总是追加到最后
            indexFile.seek(indexFile.length());
            //消息写入到了新的文件，在索引文件里面进行记录，使用Long.MIN_VALUE作为分隔符
            if(fileIndex != this.writeFileIndex){
                indexFile.writeLong(Long.MIN_VALUE);
                indexFile.writeInt(fileIndex);
                this.writeFileIndex = fileIndex;
            }
            indexFile.writeLong(messagePos);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public boolean couldRead(){
        return System.currentTimeMillis() > couldReadMessageTime;
    }
    public synchronized MessagePos readMessagePos(){
        try {
            //此时无数据
            if(read2End()){
                couldReadMessageTime = System.currentTimeMillis() + 1000L;
                return null;
            }
            indexFile.seek(readPos);
            long messagePos = indexFile.readLong();
            //遇到分割符，表示要读取的文件在下一个 MessageFile里面
            if(messagePos == Long.MIN_VALUE){
                this.readFileIndex= indexFile.readInt();
                messagePos = indexFile.readLong();
            }
            readPos = indexFile.getFilePointer();
            return new MessagePos(readFileIndex,messagePos);
        } catch (IOException e) {
            e.printStackTrace();
        }
        couldReadMessageTime = System.currentTimeMillis() + 1000L;
        return null;
    }

    /**
     * 获取索引文件上次修改的时间
     */
    public long lastModifyTime(){
        return file.lastModified();
    }

    /**
     * 返回是否读取到结尾
     */
    public boolean read2End(){
        try {
            return readPos == indexFile.length();
        } catch (IOException ignored) {
            return true;
        }
    }
    public boolean close(){
        try {
            indexFile.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
