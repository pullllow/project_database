package com.example.backend.tm.impl;

import com.example.backend.tm.TransactionManager;
import com.example.backend.utils.Panic;
import com.example.backend.utils.Parser;
import com.example.common.Error;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * @author Chang Qi
 * @date 2022/5/25 19:23
 * @description
 * @Version V1.0
 */

public class TransactionManagerImpl implements TransactionManager {


    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;


    public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;

        this.counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 校验XID文件是否合法
     * 读取XID_FILE_HEADER中的xid_counter，计算文件理论长度，对比实际长度
     *
     * @return void
     **/
    private void checkXIDCounter() {
        long fileLen = 0;

        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }

        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);

        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXIDPosition(xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }


    }

    /**
     * 根据事务XID取得其在xid文件中的位置
     *
     * @param xid
     * @return long
     **/
    private long getXIDPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }


    /**
     * 开始一个事务，返回XID
     *
     * @return long
     **/
    @Override
    public long begin() {
        counterLock.lock();

        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRANS_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }

    }

    /**
     * 更新XID事务的状态为status，并更新XID文件
     *
     * @param xid
     * @param status
     * @return void
     **/
    private void updateXID(long xid, byte status) {
        long offset = getXIDPosition(xid);
        byte[] temp = new byte[XID_FIELD_SIZE];
        temp[0] = status;

        ByteBuffer buf = ByteBuffer.wrap(temp);

        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }


    }

    /**
     * XID自增，并更新XID文件XID HEADER
     *
     * @return void
     **/
    private void incrXIDCounter() {
        xidCounter++;

        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRANS_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRANS_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        return xid == SUPER_XID ? false : checkXIDStatus(xid, FIELD_TRANS_ACTIVE);


    }

    @Override
    public boolean isCommitted(long xid) {
        return xid == SUPER_XID ? true : checkXIDStatus(xid, FIELD_TRANS_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        return xid == SUPER_XID ? false : checkXIDStatus(xid, FIELD_TRANS_ABORTED);
    }

    /**
     * 事务XID的状态是否和status状态一致
     *
     * @param xid
     * @param status
     * @return boolean
     **/
    public boolean checkXIDStatus(long xid, byte status) {
        long offset = getXIDPosition(xid);

        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);

        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return buf.array()[0] == status;

    }

    @Override
    public void close() {

        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
