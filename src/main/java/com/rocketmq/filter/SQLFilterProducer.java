package com.rocketmq.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

//sql过滤生产者
public class SQLFilterProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("122.51.174.149:9876");
        producer.start();
        for (int i = 0; i < 100; i++) {
            byte[] body=("Hi-"+i).getBytes(StandardCharsets.UTF_8);
            Message message = new Message("SQLFilterProducer", "sometag", body);
            //在property里面自定义一个sql过滤用的值
            message.putUserProperty("age", i+"");
            SendResult result = producer.send(message);
            System.out.println(result);
        }
        producer.shutdown();
    }
}
