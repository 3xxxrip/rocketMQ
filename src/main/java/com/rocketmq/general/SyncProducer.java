package com.rocketmq.general;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

//同步发送消息生产者，要等待broker回返ACK才发送下一条消息
public class SyncProducer {
    public static void main(String[] args) throws Exception {
        //创建一个producer
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定producer要连接的nameserver的ip+端口号
        producer.setNamesrvAddr("122.51.174.149:9876");
        //设置当发送失败的重试次数，默认2次
        producer.setRetryTimesWhenSendFailed(3);
        //设置超时时长为5s，默认为3s
        producer.setSendMsgTimeout(100000);
        //开启生产者
        producer.start();
        for (int i = 0; i < 100; i++) {
            byte[] body=("Hi,with keys"+i).getBytes();
            Message message = new Message("someTopic","someTag",body);
            //发送消息
            //为消息设置key
            message.setKeys("key-"+i);
            SendResult result = producer.send(message);
            System.out.println(result);
        }
        //关闭producer与nameserver的长连接
        producer.shutdown();
    }
}
