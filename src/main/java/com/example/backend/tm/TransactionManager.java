package com.example.backend.tm;

import com.example.backend.tm.impl.TransactionManagerImpl;
import com.example.backend.utils.Panic;
import com.example.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Chang Qi
 * @date 2022/5/25 19:15
 * @description
 *
 * 事务文件格式：
 * [XID_Counter][T1][T2]...[Tn]
 * XID_Counter 事务计数器 8字节
 * Ti  事务状态 1字节
 *  0 active
 *  1 committed
 *  2 aborted
 *
 * @Version V1.0
 */

public interface TransactionManager {

    //XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    //每个事务的占用长度
    static final int XID_FIELD_SIZE = 1;

    //事务的三种状态
    static final byte FIELD_TRANS_ACTIVE = 0;
    static final byte FIELD_TRANS_COMMITTED = 1;
    static final byte FIELD_TRANS_ABORTED = 2;

    //超级事务，保持committed 状态
    static final long SUPER_XID = 0;

    //XID文件后缀
    public static final String XID_SUFFIX = ".xid";


    long begin();                   //开启一个新事务

    void commit(long xid);          //提交一个事务

    void abort(long xid);           //撤销一个事务

    boolean isActive(long xid);     //查询一个事务的状态是否正在进行

    boolean isCommitted(long xid);  //查询一个事务的状态是否已提交

    boolean isAborted(long xid);    //查询一个事务的状态是否已提交

    void close();                   //关闭TM


    /**
     * 创建一个XID文件，并创建TM
     *
     * @param path
     * @return com.example.backend.tm.TransactionManager
     **/
    public static TransactionManager create(String path) {

        File file = new File(path + XID_SUFFIX);

        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotReadOrWriteException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        //写空XID文件XID HEADER

        ByteBuffer header = ByteBuffer.wrap(new byte[LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(header);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);

    }


    /**
     * 从一个已有的XID文件创建TM
     *
     * @param path
     * @return com.example.backend.tm.TransactionManager
     **/
    public static TransactionManager open(String path) {

        File file = new File(path + XID_SUFFIX);

        try {
            if (!file.exists()) {
                Panic.panic(Error.FileNotExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotReadOrWriteException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);



    }


}
