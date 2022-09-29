package mqtt.storage;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * 控制文件读取的信号量实现,用于提高读写性能，
 * 如果当前无文件可读，阻塞住线程，有文件读取，会唤醒线程进行读取
 *
 */
@Deprecated
class FileOperationSemaphore {
    private final Sync sync;

     static class Sync extends AbstractQueuedSynchronizer {
        private final FilePointer filePointer;
        Sync(FilePointer filePointer) {
            setState(1);
            this.filePointer = filePointer;
        }

        @Override
        protected final boolean tryReleaseShared(int releases) {
            return true;
        }
        @Override
        protected int tryAcquireShared(int arg) {
            long curWriterFile = filePointer.getWriteFile();
            long curReadFile = filePointer.getReadFile();
            long readPos = filePointer.getReadPos();
            long writePos = filePointer.getWritePos();
            return couldRead(curWriterFile,curReadFile,writePos,readPos) ? arg : -arg;
        }
    }

    FileOperationSemaphore(FilePointer filePointer){
        this.sync = new Sync(filePointer);
    }

    /**
     * 可读的
     */
    void readable(){
        this.sync.releaseShared(1);
    }

    /**
     * 等待到可读的时候
     */
    void waitUntilReadable(){
        this.sync.acquireShared(1);
    }

    /**
     * 返回是否可以读取文件
     */
    private static boolean couldRead(long curWriteFile,long curReadFile,long curWritePos,long curReadPos){
        return curReadFile < curWriteFile || (curReadFile == curWriteFile && curReadPos < curWritePos);
    }

}
