package com.example.db.client;

import com.example.db.transport.Package;
import com.example.db.transport.Packager;

import java.io.IOException;

/**
 * @author Chang Qi
 * @date 2022/7/16 14:53
 * @description
 * @Version V1.0
 */

public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTripper(Package pkg) throws  Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
