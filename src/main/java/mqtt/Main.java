package mqtt;

import mqtt.storage.ReadWriteMultiFile;
import mqtt.util.ByteUtil;
import mqtt.util.FileUtil;
import mqtt.util.StorageUtil;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws IOException {


        ReadWriteMultiFile readWriteMultiFile = new ReadWriteMultiFile("test", ".msg", "store", 1);

//        readWriteMultiFile.put('a');
//        readWriteMultiFile.putShort((short) 2);
//        readWriteMultiFile.putInt(12);
//        readWriteMultiFile.putFloat(12.0f);
//        readWriteMultiFile.putLong(13);
//        readWriteMultiFile.putDouble(13.0);


        ReadWriteMultiFile.RandomAccessReader reader = readWriteMultiFile.getRandomAccessReader();
        System.out.println(reader.readChar());
        System.out.println(reader.readShort());
        System.out.println(reader.readInt());
        System.out.println(reader.readFloat());
        System.out.println(reader.readLong());
        System.out.println(reader.readDouble());
//
//        byte[] bytes = new byte[10];
//        reader.read(bytes);
//        System.out.println(Arrays.toString(bytes));



    }
}
