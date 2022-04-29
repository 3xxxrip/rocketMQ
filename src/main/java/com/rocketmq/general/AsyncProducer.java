package com.rocketmq.general;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

//异步发送消息生产者，即不用等待ACK就可以发送下一条消息，但是broker还是会回送ACK
public class AsyncProducer {
    public static void main(String[] args) throws Exception {
        //创建一个producer
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定producer要连接的nameserver的ip+端口号
        producer.setNamesrvAddr("122.51.174.149:9876");
        //设置异步发送失败不进行重新发送
        producer.setRetryTimesWhenSendAsyncFailed(0);
        //开启生产者
        producer.start();

        for (int i = 0; i < 100; i++) {
            byte[] body=("Hi,"+i).getBytes();
            Message message = new Message("myTopicA","myTag",body);
            //异步发送，指定回调
            producer.send(message, new SendCallback() {
                //当producer接收到MQ发送回来的ACK之后，就会触发该回调方法的执行
                @Override
                public void onSuccess(SendResult sendResult) {
                    //输出回调结果
                    System.out.println(sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    //出现错误的话打印错误信息
                    e.printStackTrace();
                }
            });
            //休息一会，不然异步发送还没有发送完就已经调用下面的关闭producer了
        }
        TimeUnit.SECONDS.sleep(3);
        producer.shutdown();
    }
}
