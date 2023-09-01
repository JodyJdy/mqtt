package mqtt.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import mqtt.util.FileUtil;
import mqtt.util.Pair;
import mqtt.util.StorageUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.WeakHashMap;
import java.util.concurrent.*;

import static mqtt.util.FileUtil.MAX_FILE_SIZE;

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
     *随机读
     */
    private final RandomAccessFile indexFileReader;


    private MappedByteBuffer indexFileWriter;

    /**
     * 当前读取的 消息文件的索引
     */
    private int fileIndex = -1;


    private final ConcurrentLinkedQueue<Pair<Long, MessagePos>> messagePosQueue = new ConcurrentLinkedQueue<>();


    private final BlockingBool waitMessage = new  BlockingBool();



    /**
     * 存放消息的文件
     */
    private RandomAccessFile messageFile;

    public IndexFileWriterReader(String topic, File indexFile, IndexFileReadPointer indexFileReadPointer) {
        this.topic = topic;
        try {
            // 一个用来读取，一个用来写入
            this.indexFileReader = new RandomAccessFile(indexFile,"r");
            this.indexFileWriter = new RandomAccessFile(indexFile, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, indexFileReadPointer.getWriteMaxPos(), MAX_FILE_SIZE);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.indexFileReadPointer = indexFileReadPointer;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public synchronized void writeMessagePos(long messagePos,int fileIndex){
        //总是追加到最后
        long writeMaxPos = indexFileReadPointer.getWriteMaxPos();
        indexFileWriter.position((int) writeMaxPos);
        indexFileWriter.putLong(messagePos);
        indexFileWriter.putInt(fileIndex);
        //更新文件写入索引
        indexFileReadPointer.updateWriteMaxPos(writeMaxPos + FileUtil.getMessageIndexSize());
        //如果在等待，进行唤醒
        waitMessage.setTrue();
    }
    public Message readMessage(){
        try {
            if(messagePosQueue.isEmpty()) {
                //尝试进行读取,读取不到进行阻塞
                tryRead();
            }
            Pair<Long,MessagePos> posPair = messagePosQueue.poll();
            if (posPair == null) {
                return null;
            }
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
            if (indexFileReadPointer.getReadPos() + FileUtil.getMessageIndexSize() > indexFileReadPointer.getWriteMaxPos()) {
                waitMessage.waitTrue();
            }
            //最大可读位置
            long maxReadPos =indexFileReadPointer.getWriteMaxPos();
            // 进行读取
            long curPos = indexFileReadPointer.getReadPos();
            indexFileReader.seek(curPos);
            int readSize = (int) (maxReadPos - indexFileReadPointer.getReadPos());
            //一次最多读取一页
            readSize = Math.min(readSize, FileUtil.READ_MESSAGE_INDEX);
            byte[] bytes = new byte[readSize];
            indexFileReader.read(bytes);
            ByteBuffer byteBuffer =ByteBuffer.wrap(bytes);
            //读取了num条消息索引
            int num = bytes.length / FileUtil.getMessageIndexSize();
            for (int i = 0; i < num; i++) {
                MessagePos messagePos = new MessagePos(byteBuffer.getLong(), byteBuffer.getInt());
                messagePosQueue.add(Pair.create(curPos,messagePos));
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
