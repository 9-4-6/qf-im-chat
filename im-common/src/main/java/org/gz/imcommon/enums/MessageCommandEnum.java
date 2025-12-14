package org.gz.imcommon.enums;

import lombok.Getter;

@Getter
public enum MessageCommandEnum {
    //单聊消息
    MSG_P2P(4),

    //单聊消息ACK
    MSG_ACK(5),
    ;
    private int command;

    MessageCommandEnum(int command){
        this.command=command;
    }
}
