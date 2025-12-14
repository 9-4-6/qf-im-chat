package org.gz.imserver.netty;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.gz.imserver.listener.MessageReceiver;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * @author guozhong
 * @date 2025/11/18
 */
@Component
@Slf4j
public class NettyServerRunner implements CommandLineRunner , DisposableBean {

    private final ChatServer chatServer;
    private final MessageReceiver messageReceiver;
    public NettyServerRunner(ChatServer chatServer, MessageReceiver messageReceiver) {
        this.chatServer = chatServer;
        this.messageReceiver = messageReceiver;
    }

    @Override
    public void run(String... args) throws MQClientException {
        chatServer.init();
        messageReceiver.init();
    }

    @Override
    public void destroy() throws Exception {
        // Spring 容器关闭时，关闭 Netty 服务
        if (chatServer != null && chatServer.isRunning()) {
            chatServer.shutdown();
        }
    }
}
