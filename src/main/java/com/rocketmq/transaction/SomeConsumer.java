package com.rocketmq.transaction;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

//定义消费者
public class SomeConsumer {
    public static void main(String[] args) throws MQClientException {
        //创建一个push模型的消费者，就是rocketMQ里面有消息就推送给消费者的模型
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");
        //设置nameserver地址
        consumer.setNamesrvAddr("122.51.174.149:9876");
        //从consumequeue中存储的该queueoffset的第一个开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //订阅该topic的所有tag消息
        consumer.subscribe("TTopic", "*");
        //注册消息监听器，与broker建立长连接
        // 一旦broker中有了其订阅的消息就会触发该方法的执行，
        // 其返回值为当前consumer消费的状态
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

                for (Message message:msgs) {
                    //消费消息逻辑，这里就简单的输出一下吧
                    System.out.println((message));
                }
                return  ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //开启消费者消费
        consumer.start();
        System.out.println("消费者已开启");
    }
}
