package com.example.db.backend.tm.transaction;

import com.example.db.backend.tm.TransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Chang Qi
 * @date 2022/5/31 14:22
 * @description
 * @Version V1.0
 */

public class Transaction {
    public long xid;
    public int level;

    public Map<Long, Boolean> snapshot;

    public Exception error;

    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;

        if (level != 0) {
            t.snapshot = new HashMap<>();
            for (Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }

        return t;
    }

    public boolean isInSnapshot(long xid) {
        return xid == TransactionManager.SUPER_XID ? false : snapshot.containsKey(xid);
    }


}
