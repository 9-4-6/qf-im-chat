package org.gz.imserver.listener;

import cn.hutool.core.lang.Assert;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.SessionEvent;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.gz.imcommon.constants.MqConstant;
import org.gz.imcommon.enums.MessageCommandEnum;
import org.gz.imcommon.exception.BizException;
import org.gz.imserver.netty.SessionSocketHolder;
import org.gz.imserver.proto.MessageResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.Objects;
/**
 * @author 17853
 */
@Slf4j
@Component
public class MessageReceiver  {
    @Value("${rocketmq.name-server}")
    private String nameServer;
    @Value("${netty.server.broker-id}")
    private  Integer brokerId;
    public void init() throws MQClientException {
        Assert.isTrue(Objects.nonNull(brokerId),()->new BizException("IM消费者启动失败,brokerId不存在"));
        startMessageReceiver();
    }
    private  void startMessageReceiver() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(MqConstant.IM_CHAT_SINGLE+brokerId);
        consumer.setNamesrvAddr(nameServer);
        consumer.setPullBatchSize(1);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setMaxReconsumeTimes(0);
        consumer.subscribe(MqConstant.IM_CHAT_SINGLE, String.valueOf(brokerId));
        consumer.registerMessageListener((MessageListenerConcurrently) (msgS, context) -> {
            for (MessageExt msg : msgS) {
                log.info("IM消费者接受到消息:{}", JSONUtil.toJsonStr(msg));
                try {
                    String msgBody = new String(msg.getBody(), "UTF-8");
                    JSONObject jsonObject = JSONUtil.parseObj(msgBody);
                    Long userId = jsonObject.getLong("toId");
                    String content = jsonObject.getStr("content");
                    Channel channel = SessionSocketHolder.get(userId);
                    MessageResponse<String> msgR = new MessageResponse<>();
                    msgR.setCommand(MessageCommandEnum.MSG_ACK.getCommand());
                    msgR.setData(content);
                    channel.writeAndFlush(msgR);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        });

        consumer.start();

    }
}
