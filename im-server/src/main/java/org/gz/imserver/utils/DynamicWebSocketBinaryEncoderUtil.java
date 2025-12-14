package org.gz.imserver.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DynamicWebSocketBinaryEncoderUtil {
    // Jackson JSON序列化器（线程安全，全局单例）
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * 生成符合协议的二进制数据，并转换为16进制字符串
     * @param command 指令码（动态入参）
     * @param version 版本号（动态入参）
     * @param messageType 消息类型（动态入参）
     * @param clientType 客户端类型（动态入参）
     * @param appId 应用ID（动态入参）
     * @param deviceId 设备IMEI（动态入参）
     * @param body 消息体对象（任意Object，动态入参）
     * @return 16进制字符串（大写，无空格）
     * @throws JsonProcessingException JSON序列化异常
     */
    public static String encodeToHex(int command,
                                     int version,
                                     int messageType,
                                     int clientType,
                                     int appId,
                                     String deviceId,
                                     Object body) throws JsonProcessingException {
        // 1. 序列化消息体为JSON字节流（任意Object对象）
        byte[] bodyBytes = OBJECT_MAPPER.writeValueAsBytes(body);

        // 2. 处理IMEI字节流
        byte[] deviceIdBytes = deviceId.getBytes(StandardCharsets.UTF_8);
        int deviceIdLength = deviceIdBytes.length;
        int bodyLength = bodyBytes.length;

        // 3. 计算总字节数，初始化ByteBuffer（避免扩容）
        // 5个动态4字节字段 + IMEI长度(4)+内容 + 消息体长度(4)+内容
        int totalLength = 4 * 5 + 4 + deviceIdLength + 4 + bodyLength;
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.order(java.nio.ByteOrder.BIG_ENDIAN);

        // 4. 按顺序写入所有动态字段
        // 协议头固定结构（动态入参）
        buffer.putInt(command);
        buffer.putInt(version);
        buffer.putInt(messageType);
        buffer.putInt(clientType);
        buffer.putInt(appId);
        // 设备标识与消息体长度相关（动态入参）
        buffer.putInt(deviceIdLength);
        buffer.putInt(bodyLength);
        // 消息体相关（动态入参）
        buffer.put(deviceIdBytes);
        buffer.put(bodyBytes);

        // 5. 转换为字节数组并转16进制
        byte[] binaryData = buffer.array();
        return bytesToHex(binaryData);
    }

    /**
     * 字节数组转16进制字符串（大写，无分隔符）
     * @param bytes 原始字节数组
     * @return 16进制字符串
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder hexBuilder = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            hexBuilder.append(String.format("%02X", b));
        }
        return hexBuilder.toString();
    }

    // 测试主方法：演示动态传入参数的使用方式
    public static void main(String[] args) {
        try {
            // 1. 定义动态入参（可根据业务需求任意修改）
            int command = 4;
            int version = 1;
            int messageType = 0;
            int clientType = 0;
            int appId = 1;
            String deviceId = UUID.randomUUID().toString().replace("-", "");
            // 2. 构建消息体（也可传入自定义的Object对象，如自定义POJO）
            Map<String, Object> bodyMap = new HashMap<>();
            bodyMap.put("fromId", 1);
            bodyMap.put("toId", 2);
            bodyMap.put("content", "你好B,我是A");
            // 3. 生成16进制字符串
            String hexStr = DynamicWebSocketBinaryEncoderUtil.encodeToHex(
                    command, version, messageType, clientType, appId, deviceId, bodyMap
            );

            // 4. 输出结果
            System.out.println("生成的16进制字符串（用于Postman WebSocket二进制请求）：");
            System.out.println(hexStr);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
