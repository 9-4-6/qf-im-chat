package org.gz.imbiz.consumer;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.gz.imcommon.constants.MqConstant;
import org.gz.imcommon.constants.RedisConstant;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

/**
 * @author 17853
 */
@Component
@RocketMQMessageListener(consumerGroup = "im-chat",
        topic = "im-chat",
        maxReconsumeTimes = 0,
        messageModel = MessageModel.CLUSTERING)
@Slf4j
public class ImMessageConsumer implements RocketMQListener<String> {
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private RocketMQTemplate rocketMqImTemplate;

    @Override
    public void onMessage(String msg) {
        log.info("业务端接受到消息:{}",msg);
        JSONObject content = JSONUtil.parseObj(msg);
        Long userId = content.getLong("toId");
        RMap<Long, Integer> userInstanceHash = redissonClient.getMap(RedisConstant.USER_INSTANCE_HASH_KEY);
        Integer brokeId = userInstanceHash.get(userId);
        //发送消息
        String destination = MqConstant.IM_CHAT_SINGLE + ":" + brokeId;
        rocketMqImTemplate.asyncSend(destination, msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult r) {}
            @Override
            public void onException(Throwable e) {
                log.error("发送失败: {}", destination, e);
            }
        });
    }
}
