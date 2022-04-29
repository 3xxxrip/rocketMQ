package com.rocketmq.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

//tag过滤的生产者
public class TagFilterProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("122.51.174.149:9876");
        producer.start();
        String[] tags={"TagA","TagB","TagC"};
        for (int i = 0; i < 100; i++) {
            byte[] body=("Hi-"+i).getBytes(StandardCharsets.UTF_8);
//            分开设置tag
            Message message = new Message("TagFilterProducer", tags[i%tags.length], body);
            SendResult result = producer.send(message);
            System.out.println(result);
        }
        producer.shutdown();
    }
}
