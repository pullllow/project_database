package com.example.db.backend.dm.dataitem.impl;

import com.example.db.backend.common.SubArray;
import com.example.db.backend.dm.dataitem.DataItem;
import com.example.db.backend.dm.impl.DataManagerImpl;
import com.example.db.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Chang Qi
 * @date 2022/5/29 9:45
 * @description
 * @Version V1.0
 */

public class DataItemImpl implements DataItem {


    private SubArray raw;
    private byte[] oldRaw;
    private DataManagerImpl dm;  //dm释放依赖dm的依赖，修改数据时落日志
    private long uid;
    private Page page;

    private Lock rLock;
    private Lock wLock;

    public DataItemImpl(SubArray raw, byte[] oldRaw, DataManagerImpl dm, long uid, Page page) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.dm = dm;
        this.uid = uid;
        this.page = page;

        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        this.wLock = lock.writeLock();
        this.rLock = lock.readLock();

    }

    public boolean isValid() {
        return raw.raw[raw.start + DATAITEM_OS_VALID] == (byte) 0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + DATAITEM_OS_DATA, raw.end);
    }

    /**
     * 修改DataItem之前调用
     *
     * @return void
     **/
    @Override
    public void before() {
        wLock.lock();
        page.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);

    }

    /**
     * 撤销修改时调用
     *
     * @return void
     **/
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    /**
     * 对修改操作写入日志
     *
     * @param xid
     * @return void
     **/
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();

    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void wLock() {
        wLock.lock();
    }

    @Override
    public void wUnlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnlock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
