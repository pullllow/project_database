package com.example.backend.dm.pageCache.impl;

import com.example.backend.common.AbstractCache;
import com.example.backend.dm.page.Page;
import com.example.backend.dm.page.impl.PageImpl;
import com.example.backend.dm.pageCache.PageCache;
import com.example.backend.utils.Panic;
import com.example.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Chang Qi
 * @date 2022/5/26 17:00
 * @description
 * @Version V1.0
 */

public class PageCacheImpl extends AbstractCache<Page> implements PageCache  {



    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private  AtomicInteger pageNumber; // 记录当前打开的数据库文件页数

    public PageCacheImpl(RandomAccessFile file, FileChannel fc, int maxResource) {

        super(maxResource);
        if(maxResource<MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }

        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }


        this.file = file;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        this.pageNumber = new AtomicInteger((int) length/ Page.PAGE_SIZE);

    }

    /**
     * 当资源不存在缓存时的获取行为
     * @param key
     * @return com.example.backend.dm.page.Page
     *
     **/
    @Override
    protected Page getForCache(long key) throws Exception {
        int pageNo = (int) key;

        long offset = PageCacheImpl.pageOffset(pageNo);

        ByteBuffer buf = ByteBuffer.allocate(Page.PAGE_SIZE);

        fileLock.lock();

        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        fileLock.unlock();
        return new PageImpl(pageNo,buf.array(),this);

    }

    private static long pageOffset(int pageNo) {
        // 页号从1开始
        return (pageNo-1)*Page.PAGE_SIZE;
    }

    /**
     * 当资源被驱逐时的回写行为
     * @param page
     * @return void
     *
     **/
    @Override
    protected void releaseForCache(Page page) {
        if(page.isDirty()) {
            flush(page);
            page.setDirty(false);
        }

    }

    /**
     * 将Page写回文件
     * @param page
     * @return void
     *
     **/
    private void flush(Page page) {

        int pageNo = page.getPageNo();
        long offset = pageOffset(pageNo);

        fileLock.lock();

        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }


    }

    /**
     * 新增页面处理函数
     * @param initData
     * @return int
     *
     **/
    @Override
    public int newPage(byte[] initData) {
        int pageNo = pageNumber.incrementAndGet();
        Page page = new PageImpl(pageNo, initData, null);
        flush(page);
        return pageNo;
    }

    /**
     * 得到pageNo的页面资源
     * @param pageNo
     * @return com.example.backend.dm.page.Page
     *
     **/
    @Override
    public Page getPage(int pageNo) throws Exception {
        return get((long)pageNo);
    }

    /**
     * 安全关闭功能，将缓存中的所有资源强行回源
     * @return void
     *
     **/
    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }

    }

    @Override
    public void release(Page page) {
        release((long)page.getPageNo());
    }

    @Override
    public void truncateByPageNo(int maxPageNo) {

    }

    @Override
    public int getPageNumber() {
        return pageNumber.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }
}
