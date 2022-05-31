package com.example.backend.vm.entry;

import com.example.backend.common.SubArray;
import com.example.backend.dm.dataitem.DataItem;
import com.example.backend.utils.Parser;
import com.example.backend.vm.VersionManager;
import com.google.common.primitives.Bytes;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Chang Qi
 * @date 2022/5/31 13:59
 * @description
 *  Entry 维护MVCC记录
 *  数据格式如下：
 *  [XMIN][XMAX][DATA]
 *  XMIN 创建该条记录的事务编号 long 8字节
 *  XMAX 删除该记录的事务编号 long 8字节
 *  DATA 记录蚩尤数据
 *
 * @Version V1.0
 */

public class Entry {


    private static final int OS_XMIN = 0;
    private static final int OS_XMAX = OS_XMIN + 8;
    private static final int OS_DATA = OS_XMAX + 8;


    private long uid;
    private DataItem dataItem;
    private VersionManager vm;




    public static Entry loadEntry(VersionManager vm, long uid) {
        return null;
    }


    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin,xmax,data);
    }

    public void release() {

    }

    public void remove() {
        dataItem.release();
    }

    public byte[] data() {
        dataItem.rLock();

        try {
            SubArray sa = dataItem.data();
            /*byte[] data = new byte[sa.end-sa.start-OS_DATA];
            System.arraycopy(sa.raw,sa.start+OS_DATA,data,0,data.length);*/
            return Arrays.copyOfRange(sa.raw,sa.start+OS_DATA,sa.end);
        } finally {
            dataItem.rUnlock();
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw,sa.start+OS_XMIN,sa.start+OS_XMAX));
        } finally {
            dataItem.rUnlock();
        }

    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw,sa.start+OS_XMAX,sa.start+OS_DATA));
        } finally {
            dataItem.rUnlock();
        }
    }

    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid),0,sa.raw,sa.start+OS_XMAX,8);
        } finally {
            dataItem.after(xid);
        }
    }


    public long getUid() {
        return uid;
    }



}
