package com.example.db.backend.dm.recover;

import com.example.db.backend.common.SubArray;
import com.example.db.backend.dm.dataitem.DataItem;
import com.example.db.backend.dm.logger.Logger;
import com.example.db.backend.dm.page.Page;
import com.example.db.backend.dm.page.PageX;
import com.example.db.backend.dm.pageCache.PageCache;
import com.example.db.backend.tm.TransactionManager;
import com.example.db.backend.utils.Panic;
import com.example.db.backend.utils.Parser;
import com.google.common.primitives.Bytes;

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


    static class InsertLogInfo {
        long xid;
        int pageNo;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pageNo;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * 恢复策略执行函数
     * @param tm
     * @param logger
     * @param pc
     * @return void
     *
     **/
    public static void recover(TransactionManager tm, Logger logger, PageCache pc) {
        System.out.println("DM recovering...");

        logger.rewind();
        int maxPageNo = 0;
        while(true) {
            byte[] log = logger.next();
            if(log==null) break;
            int pageNo = -1;
            if(isInsertLog(log)) {
                InsertLogInfo il = parseInsertLog(log);
                pageNo = il.pageNo;
            } else {
                UpdateLogInfo ul = parseUpdateLog(log);
                pageNo = ul.pageNo;
            }
            if(pageNo > maxPageNo) {
                maxPageNo = pageNo;
            }
        }
        if(maxPageNo ==0 ){
            maxPageNo = 1;
        }
        pc.truncateByPageNo(maxPageNo);
        System.out.println("Truncate to" + maxPageNo + "pages.");

        redoTransactions(tm, logger, pc);
        System.out.println("Redo Transactions Over");

        undoTransactions(tm, logger, pc);
        System.out.println("Undo Transactions Over");

        System.out.println("Recovery Over.");

    }



    /**
     * 重做所有已完成事务
     *
     * @param tm
     * @param lg
     * @param pc
     * @return void
     **/
    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();

        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo il = parseInsertLog(log);
                long xid = il.xid;
                if (!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo ul = parseUpdateLog(log);
                long xid = ul.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }

        }
    }


    private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();

        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo il = parseInsertLog(log);
                long xid = il.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                }
                logCache.get(xid).add(log);
            } else {
                UpdateLogInfo ul = parseUpdateLog(log);
                long xid = ul.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        //对所有active log进行倒序undo
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }

    }


    /**********************************************************************************************************************/

    /************************* Insert Log ***********************************/
    // [LogType][XID][PageNo][Offset][Raw]
    // LogType: 日志类型 1字节
    // XID: 事务ID 8字节
    // PageNo: 数据分页页码 4字节
    // Offset: Page偏移  2字节
    // Raw: 插入数据


    private static final int LOG_TYPE = 0;
    private static final int LOG_XID = LOG_TYPE + 1;
    private static final int INSERT_PAGENO = LOG_XID + 8;
    private static final int INSERT_OFFSET = INSERT_PAGENO + 4;
    private static final int INSERT_RAW = INSERT_OFFSET + 2;

    public static byte[] insertLog(long xid, Page page, byte[] raw) {
        byte[] logType ={LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pageNo = Parser.int2Byte(page.getPageNo());
        byte[] offset = Parser.short2Byte(PageX.getFSO(page));

        return Bytes.concat(logType,xidRaw,pageNo,offset,raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {

        InsertLogInfo il = new InsertLogInfo();
        il.xid = Parser.parseLong(Arrays.copyOfRange(log, LOG_XID, INSERT_PAGENO));
        il.pageNo = Parser.parseInt(Arrays.copyOfRange(log, INSERT_PAGENO, INSERT_OFFSET));
        il.offset = Parser.parseShort(Arrays.copyOfRange(log, INSERT_OFFSET, INSERT_RAW));
        il.raw  = Arrays.copyOfRange(log,INSERT_RAW,log.length);
        return il;
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }


    /************************* UPDATE Log ***********************************/
    // [LogType][XID][UID][OldRaw][NewRaw]
    // LogType: 日志类型 1字节
    // XID: 事务ID 8字节
    // UID: 数据位置 8字节
    // Raw: 数据


    private static final int UPDATE_UID = LOG_XID + 8;
    private static final int UPDATE_RAW = UPDATE_UID + 8;


    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType ={LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType,xidRaw,uidRaw,oldRaw,newRaw);

    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo ul = new UpdateLogInfo();
        ul.xid = Parser.parseLong(Arrays.copyOfRange(log, LOG_XID, UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, UPDATE_UID, UPDATE_RAW));
        ul.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        ul.pageNo = (int) (uid & ((1L << 32) - 1));
        int length = (log.length - UPDATE_RAW) / 2;
        ul.oldRaw = Arrays.copyOfRange(log, UPDATE_RAW, UPDATE_RAW + length);
        ul.newRaw = Arrays.copyOfRange(log, UPDATE_RAW + length, UPDATE_RAW + length * 2);
        return ul;
    }


    private static boolean isUpDateLog(byte[] log) {
        return log[0] == LOG_TYPE_UPDATE;
    }

    /**********************************************************************************************************************/

    /**
     * 插入操作的重做和撤销处理
     *
     * @param pc
     * @param log
     * @param flag
     * @return void
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
            if (flag == UNDO) {
                DataItem.setDataItemRawInValid(il.raw);
            }
            PageX.recoverInsert(pg, il.raw, il.offset);
        } finally {
            pg.release();
        }

    }

    /**
     * 更新操作的重做和撤销处理
     *
     * @param pc
     * @param log
     * @param flag
     * @return void
     **/
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pageNo;
        short offset;
        byte[] raw;

        if (flag == REDO) {
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
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }

    }


}
