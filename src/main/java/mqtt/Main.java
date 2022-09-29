package mqtt;

import mqtt.util.StorageUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws IOException {
//        File readerAccessFile = new File("store/" +"1." + "msg");
//        if (!readerAccessFile.exists()) {
//            FileUtils.touch(readerAccessFile);
//        }
//        RandomAccessFile temp = new RandomAccessFile(readerAccessFile, "rw");
//        RandomAccessFile temp2 = new RandomAccessFile(readerAccessFile, "rw");
//        System.out.println(temp.getFilePointer());
//        System.out.println(temp.getFilePointer());
//        temp.writeLong(1L);
//        temp.writeLong(2L);
//        temp.writeLong(3L);
//
//        temp.seek(0);
//        System.out.println(temp.readLong());
//        System.out.println(temp.readLong());
//        System.out.println(temp.readLong());
//
        File indexFile = new File("/storage/index/mqtt.topic");
        FileUtils.touch(indexFile);
        Map<Integer,String> hashCode2Topic =new HashMap<>();
        RandomAccessFile randomAccessFile = new RandomAccessFile(indexFile,"rw");

        StorageUtil.addTopic(randomAccessFile,"a");
        StorageUtil.addTopic(randomAccessFile,"b");
        StorageUtil.readTopic(randomAccessFile);
        System.out.println();


//        System.out.println(temp2.readLong());



    }
}
