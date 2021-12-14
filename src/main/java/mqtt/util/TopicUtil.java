

package mqtt.util;

/**
 *
 * 主题 工具类
 *
 **/

public class TopicUtil {
    private static final String LEVEL = "/";
    private static final String ONE_LEVEL = "+";
    private static final String MULTI_LEVEL = "#";

    /**
     * 判断订阅和主题是否对的上
     * 使用 / 分割层级， + 代表一层匹配， #代表任意层的匹配
     * 例如:
     * topic:  /a/b/c 匹配 subscribe:  /a/b/+，也匹配 /a/#
     */
    public static boolean isMatch(String subscribe, String topic) {
        subscribe = subscribe.trim();
        topic = topic.trim();
        String[] subSplits = subscribe.split(LEVEL);
        String[] topSplits = topic.split(LEVEL);
        int j = 0;
        int i = 0;
        for (; i < subSplits.length; i++) {
            if (ONE_LEVEL.equals(subSplits[i]) || subSplits[i].equals(topSplits[j])) {
                j++;
            } else {
                return MULTI_LEVEL.equals(subSplits[i]) && i == subSplits.length - 1;
            }
        }
        return i == subSplits.length && j == topSplits.length;
    }

}
