

package mqtt.util;

import java.util.Objects;

/**
 *
 * 主题 工具类
 *
 **/

public class TopicUtil {
    /**
     * 要求主题和订阅一致
     */
    public static boolean isMatch(String subscribe, String topic) {
        return Objects.equals(subscribe, topic);
    }
}
