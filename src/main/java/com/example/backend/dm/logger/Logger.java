package com.example.backend.dm.logger;

/**
 * @author Chang Qi
 * @date 2022/5/27 10:00
 * @description
 * @Version V1.0
 */

public interface Logger {

    void log(byte[] data);
    void truncate(long x) throws Exception;
    byte[] next();
    void rewind();
    void close();

}
