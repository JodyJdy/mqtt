import mqtt.storage.Message;
import mqtt.storage.ReadWriteMultiFile;
import mqtt.storage.StoredMessage;
import mqtt.util.FileUtil;
import mqtt.util.StorageUtil;

public class TestReadWriteMultiFile {
    public static void main(String[] args) throws Exception {
        writeBenchmark();
    }
    /**
     * 每条消息体 1kb, 写 500 * 1024 * 1024 条
     */
    public static void writeBenchmark() {
        byte[] content = new byte[1024];
        Message message = new Message("hello", content, 0);
        StoredMessage storedMessage = Message.transToStoredMessage(message);
        ReadWriteMultiFile readWriteMultiFile = new ReadWriteMultiFile("test", ".xxx", "store", FileUtil.MAX_FILE_SIZE);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 5242880; i++) {
            StorageUtil.writeMessage(readWriteMultiFile, storedMessage);
        }
        readWriteMultiFile.force();
        //计算速度
        System.out.println(5242880 / ((double)(System.currentTimeMillis() - start)/1000.0)+"条/s");
        System.out.println(((double)(System.currentTimeMillis() - start)/1000.0)+"s");
    }


    public static void readBenchmark() throws Exception {
        ReadWriteMultiFile readWriteMultiFile = new ReadWriteMultiFile("test", ".xxx", "store", FileUtil.MAX_FILE_SIZE);
        ReadWriteMultiFile.RandomAccessReader reader = readWriteMultiFile.getRandomAccessReader();
        long start = System.currentTimeMillis();
        int count = 0;
        for (int i = 0; i < 5242880; i++) {
            StorageUtil.readMessage(reader);
            count++;
            if (count % 10000 == 0) {
                count = 0;
                //打印每s速度
                System.out.println((double) i / ((double) (System.currentTimeMillis() - start)/1000.0) +"条/s");
            }
        }
    }
}
