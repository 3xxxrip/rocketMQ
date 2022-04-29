package com.rocketmq.general;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

//单向发送消息，不需要等待broker的ACK，发送完毕直接关闭连接
public class OnewayProducer {
    public static void main(String[] args) throws Exception{
        //创建一个producer
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定producer要连接的nameserver的ip+端口号
        producer.setNamesrvAddr("122.51.174.149:9876");
        //开启生产者
        producer.start();
        for (int i = 0; i < 100; i++) {
            byte[] body=("Hi,-"+i).getBytes();
            Message message = new Message("single", "someTag", body);
            //单向发送
            producer.sendOneway(message);
        }
        producer.shutdown();
        System.out.println("连接关闭");
    }
}
