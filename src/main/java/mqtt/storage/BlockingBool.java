package mqtt.storage;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * 阻塞的布尔值
 * @author jdy
 * @title: BlockingBool
 * @description:
 * @data 2023/9/1 17:13
 */
public class BlockingBool {

    private final Sync sync = new Sync();

    static class Sync extends AbstractQueuedSynchronizer {
        Sync() {
            setState(0);
        }

        @Override
        protected final boolean tryReleaseShared(int releases) {
            setState(releases);
            return true;
        }
        @Override
        protected int tryAcquireShared(int arg) {
            if(getState() == 1){
                compareAndSetState(1,0);
                return 1;
            }
            return -1;
        }
    }
    public void setTrue(){
        sync.releaseShared(1);
    }

    /**
     * 等待为真
     */
    public void waitTrue(){
        sync.acquireShared(1);
    }


}
