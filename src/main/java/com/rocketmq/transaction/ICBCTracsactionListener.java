package com.rocketmq.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

//事务监听器
public class ICBCTracsactionListener implements TransactionListener {
    // 回调操作方法
    // 消息预提交成功就会触发该方法的执行，用于完成本地事务
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.println("预提交消息成功：" + msg);
        // 假设接收到TAGA的消息就表示扣款操作成功，TAGB的消息表示扣款失败，
        // TAGC表示扣款结果不清楚，需要执行消息回查
        if (StringUtils.equals("TAGA", msg.getTags())) {
            return LocalTransactionState.COMMIT_MESSAGE;
        } else if (StringUtils.equals("TAGB", msg.getTags())) {
            return LocalTransactionState.ROLLBACK_MESSAGE;
        } else if (StringUtils.equals("TAGC", msg.getTags())) {
            return LocalTransactionState.UNKNOW;
        }
        return LocalTransactionState.UNKNOW;
    }
    // 消息回查方法
    // 引发消息回查的原因最常见的有两个：
    // 1)回调操作返回UNKNWON
    // 2)TC没有接收到TM的最终全局事务确认指令
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        System.out.println(msg.getTags()+"回查");
        //这里直接回传本地事务成功了，实际应用中要根据业务的具体实现结果来回传本地事务处理结果
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
