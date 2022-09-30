package mqtt;

import mqtt.util.StorageUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {
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
        RandomAccessFile randomAccessFile = new RandomAccessFile(indexFile,"rw");
        randomAccessFile.seek(0);
        randomAccessFile.writeLong(1L);
        System.out.println(indexFile.lastModified());
        Thread.sleep(1000);
        randomAccessFile.writeLong(2L);
        System.out.println(indexFile.lastModified());
        Thread.sleep(1000);
        System.out.println(indexFile.lastModified());


//        System.out.println(temp2.readLong());



    }
}
