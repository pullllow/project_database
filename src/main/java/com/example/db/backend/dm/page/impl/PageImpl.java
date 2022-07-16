package com.example.db.backend.dm.page.impl;

import com.example.db.backend.dm.page.Page;
import com.example.db.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Chang Qi
 * @date 2022/5/26 16:57
 * @description
 * @Version V1.0
 */

public class PageImpl implements Page {

    private int pageNumber;
    private byte[] data;
    private boolean dirty;              //脏页面指示 （数据发生更改，需要重新刷入的文件中）
    private Lock lock;

    private PageCache pc;

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        this.dirty = false;


        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNo() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
