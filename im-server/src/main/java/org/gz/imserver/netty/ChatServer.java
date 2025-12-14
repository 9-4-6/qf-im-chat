package org.gz.imserver.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import lombok.extern.slf4j.Slf4j;
import org.gz.imcommon.exception.BizException;
import org.gz.imserver.codec.WebSocketMessageDecoder;
import org.gz.imserver.codec.WebSocketMessageEncoder;
import org.gz.imserver.config.NettyProperties;

import org.springframework.stereotype.Component;

/**
 * @author guozhong
 * @date 2025/11/18
 */
@Slf4j
@Component
public class ChatServer {
    private final NettyProperties nettyProperties;
    private final NettyServerHandlerFactory nettyServerHandlerFactory;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private volatile boolean isRunning = false;

    // 注入配置
    public ChatServer(NettyProperties nettyProperties,
                      NettyServerHandlerFactory nettyServerHandlerFactory) {
        this.nettyProperties = nettyProperties;
        this.nettyServerHandlerFactory = nettyServerHandlerFactory;
        // 校验配置
        validateConfig();
    }

    public void init()  {
        this.start();
    }

    // 初始化并启动 Netty 服务
    private void start() {
        if (isRunning) {
            log.warn("Netty 服务已启动，端口：{}", nettyProperties.getTcpPort());
            return;
        }

        bossGroup = new NioEventLoopGroup(nettyProperties.getBossThreadSize());
        workerGroup = new NioEventLoopGroup(nettyProperties.getWorkThreadSize());

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    // 服务端可连接队列大小
                    .option(ChannelOption.SO_BACKLOG, 10240)
                    // 参数表示允许重复使用本地地址和端口
                    .option(ChannelOption.SO_REUSEADDR, true)
                    // 是否禁用Nagle算法
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    // TCP 保活机制的原理
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            // websocket 基于http协议，所以要有http编解码器
                            pipeline.addLast("http-codec", new HttpServerCodec());
                            // 对写大数据流的支持
                            pipeline.addLast("http-chunked", new ChunkedWriteHandler());
                            //聚合
                            pipeline.addLast("aggregator", new HttpObjectAggregator(65535));
                            pipeline.addLast(new WebSocketServerProtocolHandler("/chat"));
                            pipeline.addLast(new WebSocketMessageDecoder());
                            pipeline.addLast(new WebSocketMessageEncoder());
                            pipeline.addLast(nettyServerHandlerFactory.createHandler());
                        }
                    });


            // 绑定端口并同步等待
            ChannelFuture future = bootstrap.bind(nettyProperties.getTcpPort()).sync();
            serverChannel = future.channel();
            isRunning = true;
            log.info("Netty 服务启动成功，端口：{}", nettyProperties.getTcpPort());

            // 监听服务关闭事件
            serverChannel.closeFuture().addListener(f -> {
                log.info("Netty 服务通道关闭");
                shutdown();
            });
        } catch (Exception e) {
            log.error("Netty 服务启动失败", e);
            // 启动失败时释放资源
            shutdown();
        }
    }

    // 优雅关闭服务
    public void shutdown() {
        if (!isRunning){ return;}

        try {
            if (serverChannel != null) {
                serverChannel.close().sync();
            }
        } catch (Exception e) {
            log.warn("Netty 通道关闭异常", e);
        } finally {
            if (workerGroup != null) {workerGroup.shutdownGracefully();}
            if (bossGroup != null) {bossGroup.shutdownGracefully();}
            isRunning = false;
            log.info("Netty 服务已关闭");
        }
    }

    // 校验配置合法性
    private void validateConfig() {
        int port = nettyProperties.getTcpPort();
        if (port < 1 || port > 65535) {
            throw new BizException("无效的 Netty 端口：" + port);
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

}
