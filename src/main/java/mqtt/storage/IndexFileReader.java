package mqtt.storage;

import mqtt.util.FileUtil;
import mqtt.util.Pair;
import mqtt.util.StorageUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 读写 topic 对应的 索引文件
 */
public class IndexFileReader {
    /**
     *索引文件对应的topic
     */
    private String topic;

    private final ConcurrentLinkedQueue<Pair<Long, Long>> messagePosQueue = new ConcurrentLinkedQueue<>();


    private final BlockingBool waitMessage = new  BlockingBool();



    /**
     * 读取消息文件
     */
    private final ReadWriteMultiFile.RandomAccessReader messageFileReader;


    /**
     * 主题索引文件
     */
    private final ReadWriteMultiFile topicIndexFile;



    private final ReadWriteMultiFile.InputStreamReader topicIndexFileReader;

    public IndexFileReader(String topic,ReadWriteMultiFile.RandomAccessReader messageFileReader) {
        this.topic = topic;
        topicIndexFile = new ReadWriteMultiFile(topic, ".topic", FileUtil.INDEX_PATH, FileUtil.DEFAULT_SINGLE_FILE_SIZE);
        topicIndexFileReader = topicIndexFile.getInputStreamReader(topic);
        this.messageFileReader = messageFileReader;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public synchronized void writeMessagePos(long messageGlobalPos){
        topicIndexFile.putLong(messageGlobalPos);
        //如果在等待，进行唤醒
        waitMessage.setTrue();
    }
    public Message readMessage(){
        try {
            if(messagePosQueue.isEmpty()) {
                //尝试进行读取,读取不到进行阻塞
                tryRead();
            }
            Pair<Long,Long> posPair = messagePosQueue.poll();
            if (posPair == null) {
                return null;
            }
            long messageGlobalPos = posPair.getV();
            messageFileReader.seek(messageGlobalPos);
            return StorageUtil.readMessage(messageFileReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    private void tryRead() {
        try {
            //如果没有消息可以读取，进行阻塞
            if (topicIndexFileReader.getGlobalReadPos() + FileUtil.getMessageIndexSize() > topicIndexFile.getGlobalWritePos() ) {
                waitMessage.waitTrue();
            }
            if (stop) {
                return;
            }
            //最大可读位置
            long maxReadPos =topicIndexFile.getGlobalWritePos();
            // 进行读取
            long curPos = topicIndexFileReader.getReadPos();
            int readSize = (int) (maxReadPos - topicIndexFileReader.getReadPos());
            //一次最多读取一页
            readSize = Math.min(readSize, FileUtil.READ_MESSAGE_INDEX);
            byte[] bytes = new byte[readSize];
            topicIndexFileReader.read(bytes);
            ByteBuffer byteBuffer =ByteBuffer.wrap(bytes);
            //读取了num条消息索引
            int num = bytes.length / FileUtil.getMessageIndexSize();
            for (int i = 0; i < num; i++) {
                messagePosQueue.add(Pair.create(curPos,byteBuffer.getLong()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private volatile boolean stop =false;
    public void stopRead(){
        //唤醒停止的线程，并停止使用
        waitMessage.stopUse();
        //设置停止标记
        stop = true;
    }
}
