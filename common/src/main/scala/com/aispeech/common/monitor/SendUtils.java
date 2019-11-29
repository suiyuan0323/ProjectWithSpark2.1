package com.aispeech.common.monitor;

import com.aispeech.common.monitor.messageBean.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author xiaomei.wang
 * @version 1.0
 * @date 2019/8/16 16:24
 */
public class SendUtils implements Serializable {

    private static RobotClient robot = new RobotClient();

    /**
     * 发送普通文本消息
     *
     * @param message
     * @return
     */
    public SendResult send(String message, String token) throws IOException {
        TextMessage textMessage = new TextMessage(message);
        return robot.send(token, textMessage);
    }

    /**
     * 发送文本消息 可以@部分人
     *
     * @param message
     * @param atMobiles 要@人的电话号码 ArrayList<String>
     * @return
     */
    public SendResult sendWithAt(String message, String token, ArrayList<String> atMobiles) throws IOException {
        TextMessage textMessage = new TextMessage(message);
        textMessage.setAtMobiles(atMobiles);
        return robot.send(token, textMessage);
    }

    /**
     * 发送文本消息 并@所有人
     *
     * @param message
     * @return
     */
    public SendResult sendWithAtAll(String message, String token) throws IOException {
        TextMessage textMessage = new TextMessage(message);
        textMessage.setIsAtAll(true);
        return robot.send(token, textMessage);
    }
}
