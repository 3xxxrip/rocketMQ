package com.rocketmq.delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

//定义定时消息生产者
public class DelayProducer {
    public static void main(String[] args) throws Exception {
        //创建属于pg生产者组的生产者
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定namserver地址
        producer.setNamesrvAddr("122.51.174.149:9876");
        producer.start();
        for (int i = 0; i < 100; i++) {
            byte[] body=("Hi-"+i).getBytes();
            Message message = new Message("TopicB", "someTag", body);
            //设置等级3的延迟等级，在conf文件里面配置的等级时间，这里等级3是10s
            message.setDelayTimeLevel(3);
            SendResult result = producer.send(message);
            System.out.println(result);
        }
        //关闭连接
        producer.shutdown();
    }
}
