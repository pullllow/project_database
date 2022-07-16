package com.example.db.client;

import com.example.db.transport.Encoder;
import com.example.db.transport.Packager;
import com.example.db.transport.Transporter;

import java.io.IOException;
import java.net.Socket;

/**
 * @author Chang Qi
 * @date 2022/7/16 14:58
 * @description
 * @Version V1.0
 */

public class Launcher {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("localhost",9999);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t,e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();

    }
}
