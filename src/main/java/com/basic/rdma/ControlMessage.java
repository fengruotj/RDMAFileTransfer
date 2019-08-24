package com.basic.rdma;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * locate com.basic.rdma
 * Created by master on 2019/8/25.
 */
public enum ControlMessage {
    CLOSE_MESSAGE((short)-100),
    EOB_MESSAGE((short)-201),
    OK_RESPONSE((short)-200),
    FAILURE_RESPONSE((short)-400),
    SASL_TOKEN_MESSAGE_REQUEST((short)-202),
    SASL_COMPLETE_REQUEST((short)-203);

    private short code;

    //private constructor
    private ControlMessage(short code) {
        this.code = code;
    }

    public void write(ByteBuffer bout) throws IOException {
        bout.putShort(code);
    }
}
