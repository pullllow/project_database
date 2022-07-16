package com.example.db.transport;

import java.io.IOException;

/**
 * @author Chang Qi
 * @date 2022/7/16 14:38
 * @description
 * @Version V1.0
 */

public class Packager {
    private Transporter transporter;
    private Encoder encoder;

    public Packager(Transporter transporter, Encoder encoder) {
        this.transporter = transporter;
        this.encoder = encoder;
    }

    public void send(Package pkg) throws Exception {
        byte[] data = encoder.encode(pkg);
        transporter.send(data);
    }

    public Package receive() throws  Exception {
        byte[] data = transporter.receive();
        return encoder.decode(data);
    }

    public void close() throws Exception {
        transporter.close();
    }

}
