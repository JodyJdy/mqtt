package mqtt.storage;

import mqtt.util.FileUtil;
import mqtt.util.Pair;
import mqtt.util.StorageUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.*;

/**
 * 读写 topic 对应的 索引文件
 */
public class IndexFileWriterReader {
    /**
     *索引文件对应的topic
     */
    private String topic;
    /**
     * 索引文件读取进度
     */
    private final IndexFileReadPointer indexFileReadPointer;

    public IndexFileReadPointer getIndexFileReadPointer() {
        return indexFileReadPointer;
    }
    /**
     *随机读写
     */
    private final RandomAccessFile indexFile;

    /**
     * 当前读取的 消息文件的索引
     */
    private int fileIndex = -1;


    private final ConcurrentLinkedQueue<Pair<Long, MessagePos>> messagePosQueue = new ConcurrentLinkedQueue<>();


    private BlockingBool waitMessage = new  BlockingBool();

    /**
     * 存放消息的文件
     */
    private RandomAccessFile messageFile;

    public IndexFileWriterReader(String topic, RandomAccessFile randomAccessFile, IndexFileReadPointer indexFileReadPointer) {
        this.topic = topic;
        this.indexFile = randomAccessFile;
        this.indexFileReadPointer = indexFileReadPointer;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public synchronized void writeMessagePos(long messagePos,int fileIndex){
        try {
            //总是追加到最后
            indexFile.seek(indexFile.length());
            indexFile.writeLong(messagePos);
            indexFile.writeInt(fileIndex);
            //如果在等待，进行唤醒
            waitMessage.setTrue();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public synchronized Message readMessage(){
        try {
            if (messagePosQueue.isEmpty()) {
                //尝试进行读取,读取不到进行阻塞
                tryRead();
            }
            Pair<Long,MessagePos> posPair = messagePosQueue.poll();
            MessagePos pos = posPair.getV();
            // 更新索引读取进度
            indexFileReadPointer.updateReadPos(posPair.getK() + FileUtil.getMessageIndexSize());
            checkFileIndex(pos);
            messageFile.seek(pos.getReadPos());
            return StorageUtil.readMessage(messageFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    private void checkFileIndex( MessagePos pos){
        try {
            if (pos.fileIndex != this.fileIndex) {
                this.fileIndex = pos.fileIndex;
                messageFile = new RandomAccessFile(FileUtil.getMessageFile(fileIndex), "r");
            }
        } catch (Exception e) {
            System.out.println("读取消息文件失败:"+e);
            System.exit(-2);
        }
    }
    private void tryRead() {
        try {
            //如果没有消息可以读取，进行阻塞
            if (indexFileReadPointer.getReadPos() == indexFile.length()) {
                // 如果读取失败，进行等待
                waitMessage.waitTrue();
            }
            // 进行读取
            long curPos = indexFileReadPointer.getReadPos();
            long length = indexFile.length();
            //一次最多读取 100条
            indexFile.seek(curPos);
            int count = 0;
            while (curPos < length && count < 100) {
                MessagePos messagePos = new MessagePos(indexFile.readLong(), indexFile.readInt());
                messagePosQueue.add(Pair.create(curPos,messagePos));
                count++;
                curPos+=FileUtil.getMessageIndexSize();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static class MessagePos{

        private final long readPos;
        private final int fileIndex;

        public long getReadPos() {
            return readPos;
        }

        public MessagePos(long readPos, int fileIndex) {
            this.readPos = readPos;
            this.fileIndex = fileIndex;
        }
    }
}
