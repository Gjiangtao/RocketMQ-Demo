package com.test.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * 发送异步消息
 */

public class AsyncProducer {
    public static void main(String[] args)  throws Exception {
        // 1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        // 2.制定NameServer地址
        producer.setNamesrvAddr("172.21.2.107:9876;172.21.2.106:9876");
        // 2.启动producer
        producer.start();

        for (int i = 0; i < 10; i++) {
            // 4.创建消息对象，指定主题Topic、Tag和消息体
            /*
            参数1：消息主题Topic
            参数2：消息Tag
            参数3：消息内容
             */
            Message msg = new Message("base", "Tag2", ("Hello World" + i).getBytes());
            // 5.发送异步消息
//            producer.send(msg);
            producer.send(msg, new SendCallback() {
                /**
                 * 发送成功回调函数
                 * @param sendResult
                 */
                public void onSuccess(SendResult sendResult) {
                    System.out.println("发送结果：" + sendResult);
                }

                /**
                 * 发送失败回调函数
                 * @param throwable
                 */
                public void onException(Throwable throwable) {
                    System.out.println("发送异常：" + throwable);
                }
            });
            //e发送状态
//            SendStatus sendStatus = result.getSendStatus();
//            //消息ID
//            String msgId = result.getMsgId();
//            //消息接受队列ID
//            int queueId = result.getMessageQueue().getQueueId();
//            System.out.println("发送状态" + sendStatus +"，消息ID" + msgId + "，队列ID" + queueId);

            //线程睡1s
            TimeUnit.SECONDS.sleep(1);

        }

        // 6.关闭生产者producer
        producer.shutdown();
        System.out.println("发送完成");
    }
}
