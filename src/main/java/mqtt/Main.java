package mqtt;

import mqtt.util.StorageUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
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
        File indexFile = new File("/storage/a.txt");
        RandomAccessFile randomAccessFile = new RandomAccessFile(indexFile,"rw");
       MappedByteBuffer buffer =  randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE,0,1024 * 1024);
       buffer.putInt(1);
       buffer.putInt(2);
       buffer.putInt(3);
       int pos = buffer.position();
       buffer.position(0);
        System.out.println(buffer.getInt());
        System.out.println(buffer.getInt());
        System.out.println();



//        System.out.println(temp2.readLong());



    }
}
