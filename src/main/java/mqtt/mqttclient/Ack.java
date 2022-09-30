

package mqtt.mqttclient;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 *
 * 用于高性能的处理消息ack
 **/

public class Ack {
    private final Sync sync;
    private final Set<Integer> set;
    static class Sync extends AbstractQueuedSynchronizer {
        Set<Integer> set;
        Sync(Set<Integer> set) {
            this.set = set;
        }
        @Override
        protected final boolean tryReleaseShared(int releases) {
            set.remove(releases);
            return true;
        }
        @Override
        protected int tryAcquireShared(int arg) {
            if(set.contains(arg)){
                return -arg;
            }
            return arg;
        }
    }
    Ack(){
        this.set = Collections.synchronizedSet(new HashSet<>());
        this.sync = new Sync(set);
    }
    /**
     * 添加需要ack的消息
     */
    void addNeedAckMsg(int msgId){
        set.add(msgId);
    }
    /**
     * ack消息
     */
    public void ack(int msgId){
        this.sync.releaseShared(msgId);
    }
    /**
     * 阻塞等待ack结束
     */
    void waitAck(int msgId){
        this.sync.acquireShared(msgId);
    }
}
