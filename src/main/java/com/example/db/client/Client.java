package com.example.db.client;

import com.example.db.transport.Package;
import com.example.db.transport.Packager;

/**
 * @author Chang Qi
 * @date 2022/7/16 14:53
 * @description
 * @Version V1.0
 */

public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTripper(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}
