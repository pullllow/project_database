package com.example.db.transport;

/**
 * @author Chang Qi
 * @date 2022/7/16 14:37
 * @description
 * @Version V1.0
 */

public class Package {
    byte[] data;
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
