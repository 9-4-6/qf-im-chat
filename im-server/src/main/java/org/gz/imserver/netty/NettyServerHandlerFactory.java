package org.gz.imserver.netty;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.gz.imserver.manager.UserInstanceBindComponent;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class NettyServerHandlerFactory {
    private final RocketMQTemplate rocketMqImTemplate;
    private final UserInstanceBindComponent userInstanceBindComponent;
    @Value("${netty.server.broker-id}")
    private  Integer brokerId;

    public NettyServerHandlerFactory(@Qualifier("rocketMqImTemplate") RocketMQTemplate rocketMqImTemplate,
    UserInstanceBindComponent userInstanceBindComponent) {
        this.rocketMqImTemplate = rocketMqImTemplate;
        this.userInstanceBindComponent = userInstanceBindComponent;
    }

    // 工厂方法创建新实例
    public NettyServerHandler createHandler() {
        return new NettyServerHandler(brokerId,rocketMqImTemplate, userInstanceBindComponent);
    }
}
