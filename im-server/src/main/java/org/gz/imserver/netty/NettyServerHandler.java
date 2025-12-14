package org.gz.imserver.netty;


import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.gz.imcommon.enums.MessageCommandEnum;
import org.gz.imcommon.enums.SystemCommandEnum;
import org.gz.imserver.manager.UserInstanceBindComponent;
import org.gz.imserver.proto.Message;
import org.gz.imserver.proto.MessageResponse;


/**
 * @author guozhong
 */
@Slf4j
public class NettyServerHandler extends SimpleChannelInboundHandler<Message> {
    private final RocketMQTemplate rocketMqImTemplate;
    private final UserInstanceBindComponent userInstanceBindComponent;
    private final Integer brokerId;

    public NettyServerHandler(Integer brokerId, RocketMQTemplate rocketMqImTemplate,
                              UserInstanceBindComponent userInstanceBindComponent) {
        this.brokerId = brokerId;
        this.rocketMqImTemplate = rocketMqImTemplate;
        this.userInstanceBindComponent = userInstanceBindComponent;
    }

    /**
     * 核心方法：处理客户端发送的消息（已解码为Message对象）
     * 当ChannelPipeline中前面的解码器（如自定义的ByteBufToMessageDecoder）将ByteBuf解析为Message对象后，会触发此方法
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg)  {
        String clientAddress = ctx.channel().remoteAddress().toString();
        log.info("收到客户端[{}]的消息：{}", clientAddress, msg);

        int command = msg.getMessageHeader().getCommand();
        log.info("消息命令为：{}，开始处理业务逻辑", command);
        JSONObject jsonObject = JSONUtil.parseObj(msg.getMessageBody());
        if(command == SystemCommandEnum.LOGIN.getCommand()){
            //登录
            Long userId = jsonObject.getLong("fromId");
            MessageResponse<Long> msgR = new MessageResponse<>();
            msgR.setCommand(SystemCommandEnum.LOGIN_ACK.getCommand());
            msgR.setData(userId);
            ctx.channel().writeAndFlush(msgR);
            SessionSocketHolder.put(userId,ctx.channel());
            //绑定当前实例id与用户
            userInstanceBindComponent.bindUser(userId,brokerId);

        }else if(command == MessageCommandEnum.MSG_P2P.getCommand()){
            //登录
            Long fromId = jsonObject.getLong("fromId");
            //消息内容
            String content = jsonObject.getStr("content");
            log.info("消息内容为：{}", content);
            MessageResponse<String> msgR = new MessageResponse<>();
            msgR.setCommand(MessageCommandEnum.MSG_ACK.getCommand());
            msgR.setData(content);
            Channel channel = SessionSocketHolder.get(fromId);
            channel.writeAndFlush(msgR);
            //发送消息
            String message = JSONUtil.toJsonStr(msg.getMessageBody());
            rocketMqImTemplate.asyncSend("im-chat",message, new SendCallback() {
                @Override
                public void onSuccess(SendResult r) {}
                @Override
                public void onException(Throwable e) {
                    log.error("发送失败: {}", "im-chat", e);
                }
            });
        }

    }

    /**
     * 处理用户事件（非I/O事件），常见场景：心跳检测（IdleStateEvent）、连接状态变更等
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {

    }

    /**
     * 处理channel中的异常（如网络中断、解码失败、业务逻辑异常等）
     * 若不重写此方法，默认会关闭channel
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        String clientAddress = ctx.channel().remoteAddress().toString();
        // 打印异常信息（包含客户端地址，便于定位问题）
        log.error("客户端[{}]发生异常，原因：", clientAddress, cause);

        // 异常处理策略：根据异常类型决定是否关闭连接
        ctx.close();
    }

    // 补充：常用的其他生命周期方法（可选重写）
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        String clientAddress = ctx.channel().remoteAddress().toString();
        log.info("客户端[{}]连接成功", clientAddress);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        String clientAddress = ctx.channel().remoteAddress().toString();
        log.info("客户端[{}]断开连接", clientAddress);
        super.channelInactive(ctx);
    }
}
