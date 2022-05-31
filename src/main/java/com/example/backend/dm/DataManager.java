package com.example.backend.dm;

import com.example.backend.dm.dataitem.DataItem;
import com.example.backend.dm.impl.DataManagerImpl;
import com.example.backend.dm.logger.Logger;
import com.example.backend.dm.page.Page;
import com.example.backend.dm.page.PageOne;
import com.example.backend.dm.pageCache.PageCache;
import com.example.backend.dm.recover.Recover;
import com.example.backend.tm.TransactionManager;
import com.example.backend.tm.impl.TransactionManagerImpl;


/**
 * @author Chang Qi
 * @date 2022/5/26 15:50
 * @description DM层直接对外提供方法的类
 * 实现DataItem 对象的缓存
 * @Version V1.0
 */

public interface DataManager {

    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void initPageOne();

    void fillPageIndex();



    void close();


    public static DataManager create(String path, long memory, TransactionManager tm) {
        PageCache pc = PageCache.create(path, memory);
        Logger logger = Logger.create(path);
        DataManager dm = new DataManagerImpl(tm,pc,logger);
        dm.initPageOne();
        dm.fillPageIndex();
        return dm;


    }


    public static DataManager open(String path, long memory, TransactionManager tm) {
        PageCache pc = PageCache.open(path, memory);
        Logger logger = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(tm,pc,logger);

        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm,logger,pc);
        }
        dm.fillPageIndex();
        Page pageOne = dm.getPageOne();
        PageOne.setVcOpen(pageOne);
        dm.getPageCache().flushPage(pageOne);
        return dm;


    }


}
