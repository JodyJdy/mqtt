package mqtt;

import mqtt.util.StorageUtil;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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
        FileOutputStream fos = new FileOutputStream(indexFile);
        for(int i =0;i<1000;i++){
            fos.write("hello world".getBytes());
        }
        fos.close();
        MappedByteBuffer ma= new RandomAccessFile(indexFile,"r").getChannel().map(FileChannel.MapMode.READ_ONLY,0,1024);

        MappedByteBuffer mb= new RandomAccessFile(indexFile,"rw").getChannel().map(FileChannel.MapMode.READ_WRITE,0,1024);
//        System.out.println(temp2.readLong());

        mb.position(0);
        byte[] bs = "xxxx".getBytes(StandardCharsets.UTF_8);
        mb.put(bs);
        mb.position(100);

        ma.position(0);
        byte[] xx = new byte[bs.length];
        ma.get(xx);


        System.out.println(new String(xx));



    }
}
