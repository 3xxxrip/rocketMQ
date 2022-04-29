package com.rocketmq.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

//批量发送消息生产者
public class BatchProducer {
    public static void main(String[] args) throws Exception{
        //创建属于pg生产者组的生产者
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定namserver地址
        producer.setNamesrvAddr("122.51.174.149:9876");
        producer.start();
        List<Message> messages=new ArrayList<>();
        //批量发送100条消息
        for (int i = 0; i < 100; i++) {
            byte[] body=("Hi-"+i).getBytes();
            Message message = new Message("BatchTopic", "someTag", body);
            messages.add(message);
        }
        MessageListSplitter splitter = new MessageListSplitter(messages);
        while(splitter.hasNext()){
            //每次发送最多4m大小的批量消息，超过的下次发
            List<Message> listItem = splitter.next();
            producer.send(listItem);
        }
        producer.shutdown();
    }
}
