package com.rocketmq.order;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

//顺序消息生产者
//这个是分区有序，因为没指定有多少个queue，默认都是4个queue
public class OrderProducer {
    public static void main(String[] args) throws Exception {
        //创建一个producer
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定producer要连接的nameserver的ip+端口号
        producer.setNamesrvAddr("122.51.174.149:9876");
        //开启生产者
        producer.start();
        for (int i = 0; i < 100; i++) {
            Integer orderId=i;
            byte[] body=("Hi-"+i).getBytes();
            Message message = new Message("TopicA", "TagA", body);
            //将orderId作为消息key
            message.setKeys(orderId.toString());
            //send（）方法中第三个参数会传递给select(List<MessageQueue> mqs, Message msg, Object arg)的第三个参数中也就是arg里面，配合选择器工作
            SendResult result = producer.send(message, new MessageQueueSelector() {
                //具体的选择算法在该方法中定义
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    System.out.println(arg);
                    String s = msg.getKeys();
                    System.out.println(s);
                    //以下是使用消息key作为选择key
                    int id = Integer.parseInt(s);
                    //以下是使用arg作为选择key的选择算法，这两种方法二选一
//                    Integer id = (Integer) arg;

                    int index = id % mqs.size();
                    return mqs.get(index);
                }
            }, orderId);
            //输出传输结果
            System.out.println(result);
        }
        //关闭连接
        producer.shutdown();
    }
}
