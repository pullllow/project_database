package com.example.backend.vm.impl;

import com.example.backend.common.AbstractCache;
import com.example.backend.dm.DataManager;
import com.example.backend.tm.TransactionManager;
import com.example.backend.tm.transaction.Transaction;
import com.example.backend.utils.Panic;
import com.example.backend.vm.VersionManager;
import com.example.backend.vm.entry.Entry;
import com.example.backend.vm.lock.LockTable;
import com.example.backend.vm.visibility.Visibility;
import com.example.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Chang Qi
 * @date 2022/5/31 13:58
 * @description
 * @Version V1.0
 */

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {


    private TransactionManager tm;
    private DataManager dm;
    private Map<Long, Transaction> activeTransaction;  // 可以使用ConcurrentHashMap
    private Lock lock;


    private LockTable lockTable;


    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);

        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManager.SUPER_XID, Transaction.newTransaction(TransactionManager.SUPER_XID,0,null));
        this.lock = new ReentrantLock();
        this.lockTable = new LockTable();

    }


    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.error != null) {
            throw t.error;
        }

        Entry entry = null;

        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }

        }

        try {
            if(Visibility.isVisible(tm,t,entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }

    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.error !=null) {
            throw t.error;
        }

        return dm.insert(xid, Entry.wrapEntryRaw(xid,data));
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.error !=null) {
            throw t.error;
        }


        Entry entry = null;

        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }

        }

        try {
            if(!Visibility.isVisible(tm,t,entry)) {
                return false;
            }

            Lock l = null;

            try {
                l = lockTable.add(xid,uid);
            } catch (Exception e) {
                t.error = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.error;
            }
            if(l!=null) {
                l.lock();
                l.unlock();
            }
            if(entry.getXmax()==xid) {
                return false;
            }
            if(Visibility.isVersionSkip(tm,t,entry)) {
                t.error = Error.ConcurrentUpdateException;
                internAbort(xid,true);
                t.autoAborted = true;
                throw t.error;
            }

            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }

    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid,level,activeTransaction);
            activeTransaction.put(xid,t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void committed(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.error!= null) {
                throw t.error;
            }
        } catch (NullPointerException e) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(e);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lockTable.remove(xid);
        tm.commit(xid);

    }

    @Override
    public void aborted(long xid) {
        internAbort(xid,false);
    }

    private void internAbort(long xid, boolean autoAborted){
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) return;
        lockTable.remove(xid);
        tm.abort(xid);

    }



    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this,uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

}
