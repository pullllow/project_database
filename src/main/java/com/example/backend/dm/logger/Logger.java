package com.example.backend.dm.logger;

import com.example.backend.dm.logger.impl.LoggerImpl;
import com.example.backend.utils.Panic;
import com.example.backend.utils.Parser;
import com.example.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Chang Qi
 * @date 2022/5/27 10:00
 * @description 日志读写
 *
 *  日志文件标准格式为：
 *  [XCheckSum] [Log1] [Log2] ... [LogN] [BadTail]
 *  XCheckSum 为后续所有日志计算的Checksum，int类型
 *  Logn 单条日志
 *  BadTail 数据库崩溃时没有来得及完成的日志数据
 *
 *  每条正确日志的格式为：
 *  [Size] [CheckSum] [Data]
 *  Size 4字节 int 标识Data长度
 *  CheckSum 4字节 int
 *
 *  日志类型 InsertLog  UpdateLog
 *
 *  InsertLog 格式：
 *  [LogType][XID][PageNo][Offset][Raw]
 *  UpdateLog 格式：
 *  [LogType][XID][UID][OldRaw][NewRaw]
 *
 *
 * @Version V1.0
 */

public interface Logger {

    static final int SEED = 13331;


    static final int LOGGER_OS_XCHECKSUM = 0;
    static final int LOGGER_OS_LOG = 4;

    static final int LOG_OS_SIZE = 0;
    static final int LOG_HEADER_SIZE = 4;
    static final int LOG_OS_CHECKSUM = LOG_OS_SIZE + LOG_HEADER_SIZE;
    static final int LOG_OS_DATA = LOG_OS_CHECKSUM + LOG_HEADER_SIZE;

    public static final String LOG_SUFFIX = ".log";

    void log(byte[] data);
    void truncate(long x) throws Exception;
    byte[] next();
    void rewind();
    void close();



    public static Logger create(String path) {
        File file = new File(path+LOG_SUFFIX);
        try {
            if(!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotReadOrWriteException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;

        try {
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));

        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new LoggerImpl(raf,fc,0);

    }

    public static Logger open(String path) {
        File file = new File(path+LOG_SUFFIX);
        if(!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!file.canWrite()|| !file.canRead()) {
            Panic.panic(Error.FileCannotReadOrWriteException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;

        try {
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        LoggerImpl logger = new LoggerImpl(raf, fc);
        logger.init();

        return logger;
    }


}
