package com.example.backend.dm.logger.impl;

import com.example.backend.dm.logger.Logger;

import java.io.RandomAccessFile;

/**
 * @author Chang Qi
 * @date 2022/5/27 10:20
 * @description
 * @Version V1.0
 */

public class LoggerImpl implements Logger {


    private RandomAccessFile raf;




    @Override
    public void log(byte[] data) {

    }

    @Override
    public void truncate(long x) throws Exception {

    }

    @Override
    public byte[] next() {
        return new byte[0];
    }

    @Override
    public void rewind() {

    }

    @Override
    public void close() {

    }
}
