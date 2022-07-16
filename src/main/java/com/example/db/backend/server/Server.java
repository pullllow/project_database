package com.example.db.backend.server;

import com.example.db.backend.tbm.TableManager;
import com.example.db.transport.Encoder;
import com.example.db.transport.Package;
import com.example.db.transport.Packager;
import com.example.db.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Chang Qi
 * @date 2022/7/16 15:21
 * @description
 * @Version V1.0
 */

public class Server {
    private int port;

    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen from port:" + port);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100));

        try {
            while (true) {
                Socket socket = ss.accept();
                Runnable worker = new HandlerSocket(socket, tbm);
                executor.execute(worker);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ss.close();
            } catch (IOException ignored) {
            }
        }
    }
}


class HandlerSocket implements Runnable {
    private Socket socket;
    private TableManager tbm;

    public HandlerSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress) socket.getRemoteSocketAddress();
        System.out.println("Establish connection:" + address.getAddress().getHostAddress() + ":" + address.getPort());
        Packager packager = null;
        try {
            Transporter t = new Transporter(socket);
            Encoder e = new Encoder();
            packager = new Packager(t, e);
        } catch (IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return;
        }

        Executor executor = new Executor(tbm);
        while (true) {
            Package pkg = null;
            try {
                pkg = packager.receive();
            } catch (Exception e) {
                break;
            }
            byte[] sql = pkg.getData();
            byte[] result = null;
            Exception e = null;
            try {
                result = executor.execute(sql);
            } catch (Exception ex) {
                e = ex;
                e.printStackTrace();
            }
            pkg = new Package(result, e);
            try {
                packager.send(pkg);
            } catch (Exception ex) {
                ex.printStackTrace();
                break;
            }
        }
        executor.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}