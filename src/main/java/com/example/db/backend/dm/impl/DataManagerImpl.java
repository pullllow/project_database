package com.example.db.backend.dm.impl;

import com.example.db.backend.common.AbstractCache;
import com.example.db.backend.dm.DataManager;
import com.example.db.backend.dm.dataitem.DataItem;
import com.example.db.backend.dm.dataitem.impl.DataItemImpl;
import com.example.db.backend.dm.logger.Logger;
import com.example.db.backend.dm.page.Page;
import com.example.db.backend.dm.page.PageOne;
import com.example.db.backend.dm.page.PageX;
import com.example.db.backend.dm.pageCache.PageCache;
import com.example.db.backend.dm.pageIndex.PageIndex;
import com.example.db.backend.dm.pageIndex.PageInfo;
import com.example.db.backend.dm.recover.Recover;
import com.example.db.backend.tm.TransactionManager;
import com.example.db.backend.utils.Panic;
import com.example.db.backend.utils.Types;
import com.example.db.common.Error;

/**
 * @author Chang Qi
 * @date 2022/5/26 15:51
 * @description
 * @Version V1.0
 */

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    private TransactionManager tm;
    private PageCache pc;
    private Logger logger;
    private PageIndex pageIndex;


    private Page pageOne; //数据页面验证页面的记录指针

    public Page getPageOne() {
        return pageOne;
    }

    public PageCache getPageCache() {
        return pc;
    }

    public DataManagerImpl(TransactionManager tm, PageCache pc, Logger logger) {
        super(0);
        this.tm = tm;
        this.pc = pc;
        this.logger = logger;

        this.pageIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {

        DataItemImpl di = (DataItemImpl) super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        // PageIndex 5
        for(int i=0;i<5;i++) {
            pi = pageIndex.select(raw.length);
            if(pi!=null) {
                break;
            } else {
                int newPageNo = pc.newPage(PageX.initRaw());
                pageIndex.add(newPageNo,PageX.MAX_FREE_SPACE);
            }
        }
        if(pi ==null) {
            throw Error.DatabaseBusyException;
        }

        Page page = null;
        int freeSpace = 0;

        try {
            page = pc.getPage(pi.pageNo);
            byte[] log = Recover.insertLog(xid, page, raw);
            logger.log(log);

            short offset = PageX.insert(page,raw);

            page.release();
            return Types.addressToUid(pi.pageNo,offset);

        } finally {
            // 将取出的page放入到PageIndex
            if(page!=null) {
                pageIndex.add(pi.pageNo, PageX.getFreeSpace(page));
            } else {
                pageIndex.add(pi.pageNo,freeSpace);
            }
        }

    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();

    }

    /**
     * xid生成update 日志
     * @param xid
     * @param di
     * @return void
     *
     **/
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }


    public void releaseDataItem(DataItemImpl dataItem) {
        super.release(dataItem.getUid());
    }

    /**
     * DataItem缓存 根据key解析出页号，从pageCache中获取页面，再根据偏移解析DataItem
     * @param uid
     * @return com.example.db.backend.dm.dataitem.DataItem
     *
     **/
    @Override
    protected DataItem getForCache(long uid) throws Exception {

        short offset = (short) (uid & (1L<<16)-1);
        uid >>>= 32;
        int pageNo = (int )(uid & (1L<<32)-1);
        Page page = pc.getPage(pageNo);
        return DataItem.parseDataItem(page,offset,this);
    }

    /**
     *DataItem缓存释放，将DataItem写回数据源
     * 对文件的读写是以页为单位进行的，只需要将 DataItem 所在的页 release
     * @param dataItem
     * @return void
     *
     **/
    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    /**
     * 创建文件时初始化PageOne
     * @return void
     *
     **/
    @Override
    public void initPageOne() {
        int pageNo = pc.newPage(PageOne.initRaw());
        assert  pageNo ==1;
        try {
            pageOne = pc.getPage(pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }



    /**
     * 打开已有文件时读入PageOne, 并验证正确性
     * @return boolean
     *
     **/
    public boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     *初始化pageIndex
     * @return void
     *
     **/
    @Override
    public void fillPageIndex() {
        int pageNumber = pc.getPageNumber();

        for(int i=2;i<=pageNumber;i++) {
            Page page = null;
            try {
                page = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pageIndex.add(page.getPageNo(),PageX.getFreeSpace(page));
            page.release();
        }
    }




}
