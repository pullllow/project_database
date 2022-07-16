package com.example.db.backend.dm.logger.impl;

import com.example.db.backend.dm.logger.Logger;
import com.example.db.backend.utils.Panic;
import com.example.db.backend.utils.Parser;
import com.example.db.common.Error;
import com.google.common.primitives.Bytes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Chang Qi
 * @date 2022/5/27 10:20
 * @description
 * @Version V1.0
 */

public class LoggerImpl implements Logger {


    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock lock;

    private long position;              // 当前指针位置
    private long fileSize;              // 初始化记录，log操作不更新
    private int xCheckSum;              // 记录当前日志XCheckSum


    public LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        this.lock = new ReentrantLock();

    }

    public LoggerImpl(RandomAccessFile raf, FileChannel fc, int xCheckSum) {
        this.raf = raf;
        this.fc = fc;
        this.xCheckSum = xCheckSum;
        this.lock = new ReentrantLock();
    }

    public void init() {
        long size = 0;

        try {
            size = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        if (size < 4) {
            Panic.panic(Error.BadLogFileErrorException);
        }

        // 读取日志文件CheckSum
        ByteBuffer raw = ByteBuffer.allocate(4);

        try {
            fc.position(LOGGER_OS_XCHECKSUM);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xCheckSum = Parser.parseInt(raw.array());
        this.fileSize = size;
        checkAndRemoveTail();


    }


    /**
     * 校验日志文件的XCheckSum，并移除文件尾部可能存在的BadTail
     *
     * @return void
     **/
    private void checkAndRemoveTail() {

        rewind();

        int xCheck = 0;

        // 计算日志文件的XCheckSum
        while (true) {
            byte[] log = internNext();
            if (log == null) break;
            xCheck = calCheckSum(xCheck, log);
        }
        if (xCheck != xCheckSum) {
            Panic.panic(Error.BadLogFileErrorException);
        }

        //截断文件到正常日志尾部
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            raf.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }

        rewind();

    }


    /**
     * 单条日志的校验和
     *
     * @param xCheck
     * @param log
     * @return int
     **/
    private int calCheckSum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }


    /**
     * 将封装data为日志，并写入日志文件中
     *
     * @param data
     * @return void
     **/
    @Override
    public void log(byte[] data) {

        byte[] log = wrapLog(data);

        ByteBuffer buf = ByteBuffer.wrap(log);

        lock.lock();
        // 将日志写入文件
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }

        // 更新日志文件
        updateXCheckSum(log);

    }

    private void updateXCheckSum(byte[] log) {
        this.xCheckSum = calCheckSum(xCheckSum, log);
        try {
            fc.position(LOGGER_OS_XCHECKSUM);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xCheckSum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

    }

    private byte[] wrapLog(byte[] data) {
        byte[] size = Parser.int2Byte(data.length);
        byte[] checkSum = Parser.int2Byte(calCheckSum(0, data));
        return Bytes.concat(size, checkSum, data);
    }


    /**
     * 截断日志文件
     *
     * @param x
     * @return void
     **/
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();

        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, LOG_OS_DATA, log.length);
        } finally {
            lock.unlock();
        }

    }

    /**
     * 迭代器模式
     * 从日志文件中读取下一个个日志
     *
     * @return byte[]
     **/
    private byte[] internNext() {
        if (position + LOG_OS_DATA >= fileSize) {
            return null;
        }
        // 读取日志 [Size]
        ByteBuffer tmp = ByteBuffer.allocate(LOG_HEADER_SIZE);

        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }

        int size = Parser.parseInt(tmp.array());
        if (position + size + LOG_OS_DATA > fileSize) {
            return null;
        }

        //读取日志 [CheckSum][Data]
        ByteBuffer buf = ByteBuffer.allocate(LOG_OS_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();

        // 校验CheckSum
        int check1 = calCheckSum(0, Arrays.copyOfRange(log, LOG_OS_DATA, log.length));  // 根据数据计算CheckSum
        int check2 = Parser.parseInt(Arrays.copyOfRange(log, LOG_OS_CHECKSUM, LOG_OS_DATA));      // 直接读取日志中的CheckSum

        if (check1 != check2) return null;

        position += log.length;
        return log;

    }

    /**
     * position 重新设定日志指针指向日志起始位置
     *
     * @return void
     **/
    @Override
    public void rewind() {
        position = LOGGER_OS_LOG;
    }

    @Override
    public void close() {
        try {
            raf.close();
            fc.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
