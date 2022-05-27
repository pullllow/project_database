package com.example.backend.dm.recover;

import com.example.backend.dm.logger.Logger;
import com.example.backend.dm.page.Page;
import com.example.backend.dm.page.PageX;
import com.example.backend.dm.pageCache.PageCache;
import com.example.backend.tm.TransactionManager;
import com.example.backend.utils.Panic;

import java.util.*;

/**
 * @author Chang Qi
 * @date 2022/5/27 14:06
 * @description
 * @Version V1.0
 */

public class Recover {

    // 日志类型
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;


    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo{
        long xid;
        int pageNo;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo{
        long xid;
        int pageNo;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * 重做所有已完成事务
     * @param tm
     * @param lg
     * @param pc
     * @return void
     *
     **/
    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();

        while(true) {
            byte[] log = lg.next();
            if(log==null) break;
            if(isInsertLog(log)) {
                InsertLogInfo il = parseInsertLog(log);
                long xid = il.xid;
                if(!tm.isActive(xid)) {
                    doInsertLog(pc,log, REDO);
                }
            } else {
                UpdateLogInfo ul = parseUpdateLog(log);
                long xid = ul.xid;
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc,log,REDO);
                }
            }

        }
    }




    private static void undoTransactions(TransactionManager tm,Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache  = new HashMap<>();

        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log==null) break;
            if(isInsertLog(log)) {
                InsertLogInfo il = parseInsertLog(log);
                long xid = il.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid,new ArrayList<>());
                    }
                }
                logCache.get(xid).add(log);
            } else {
                UpdateLogInfo ul = parseUpdateLog(log);
                long xid = ul.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid,new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        //对所有active log进行倒序undo
        for(Map.Entry<Long, List<byte[]>> entry: logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for(int i=logs.size()-1;i>=0;i--) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc,log,UNDO);
                } else {
                    doUpdateLog(pc,log,UNDO);
                }
            }
            tm.abort(entry.getKey());
        }

    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        return null;
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        return null;
    }

    private static boolean isInsertLog(byte[] log) {
        return false;
    }
    private static boolean isUpDateLog(byte[] log) {
        return false;
    }

    /**
     * 插入操作的重做和撤销处理
     * @param pc
     * @param log
     * @param flag
     * @return void
     *
     **/
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {

        InsertLogInfo il = parseInsertLog(log);
        Page pg = null;

        try {
            pg = pc.getPage(il.pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            if(flag==UNDO) {
                DataItem.setDataItemRawInValid(il.raw);
            }
            PageX.recoverInsert(pg,il.raw,il.offset);
        } finally {
            pg.release();
        }

    }

    /**
     * 更新操作的重做和撤销处理
     * @param pc
     * @param log
     * @param flag
     * @return void
     *
     **/
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pageNo;
        short offset;
        byte[] raw;

        if(flag==REDO) {
            // 重做
            UpdateLogInfo ul = parseUpdateLog(log);
            pageNo = ul.pageNo;
            offset = ul.offset;
            raw = ul.newRaw;
        } else {
            // 撤销
            UpdateLogInfo ul = parseUpdateLog(log);
            pageNo = ul.pageNo;
            offset = ul.offset;
            raw = ul.oldRaw;
        }

        Page pg = null;

        try {
            pc.getPage(pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg,raw,offset);
        } finally {
            pg.release();
        }

    }


}
