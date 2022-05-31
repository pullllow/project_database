package com.example.backend.vm.visibility;

import com.example.backend.tm.TransactionManager;
import com.example.backend.tm.transaction.Transaction;
import com.example.backend.vm.entry.Entry;

/**
 * @author Chang Qi
 * @date 2022/5/31 16:24
 * @description
 * @Version V1.0
 */

public class Visibility {

    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry entry) {
        long xmax = entry.getXmax();
        if(t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax>t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry entry) {
        if(t.level == 0) {
            return readCommitted(tm,t,entry);
        } else {
            return repeatableRead(tm,t,entry);
        }
    }



    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry entry) {
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry entry) {
        return false;
    }

}
