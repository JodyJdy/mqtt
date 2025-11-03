import mqtt.storage.*;

import java.util.ArrayList;
import java.util.List;

public class TestMessageWriterReader {


    public static void main(String[] args) throws Exception {
        readerBenchmark();
    }

    public static void writeBenchmark() throws Exception {
        //用于将消息写入文件
        MessageStorage messageStorage = new MessageStorage();
        //用于写数据
        byte[] content = new byte[1024];
        CallbackableMessage message = new CallbackableMessage("hello", content, 0);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 5242880; i++) {
            messageStorage.writeMessage(message);
        }
        //计算速度
        System.out.println(5242880 / ((double)(System.currentTimeMillis() - start)/1000.0)+"条/s");
        System.out.println(((double)(System.currentTimeMillis() - start)/1000.0)+"s");
    }

    public static void readerBenchmark() throws Exception {
        //用于将消息写入文件
        MessageStorage messageStorage = new MessageStorage();
        //用于写数据
        long start = System.currentTimeMillis();
        List<Integer> results = new ArrayList<>();
        for (int i = 0; i < 524288; i++) {
            Message ms = messageStorage.readMessage("hello");
            results.add(ms.getPacketId());
        }
        System.out.println(results.size());
        //计算速度
        System.out.println(524288 / ((double)(System.currentTimeMillis() - start)/1000.0)+"条/s");
        System.out.println(((double)(System.currentTimeMillis() - start)/1000.0)+"s");
    }

}
