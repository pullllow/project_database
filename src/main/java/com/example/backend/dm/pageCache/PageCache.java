package com.example.backend.dm.pageCache;

import com.example.backend.dm.page.Page;
import com.example.backend.dm.pageCache.impl.PageCacheImpl;
import com.example.backend.utils.Panic;
import com.example.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author Chang Qi
 * @date 2022/5/26 17:00
 * @description
 * @Version V1.0
 */

public interface PageCache {


    public static final int MEM_MIN_LIM = 10;   //缓存存储资源数最小限制
    public static final String DB_SUFFIX = ".DB";


    int newPage(byte[] initData);

    Page getPage(int pageNo) throws Exception;

    void close();
    void release(Page page);

    void truncateByPageNo(int maxPageNo);
    int getPageNumber();
    void flushPage(Page page);



    public static PageCache create(String path, long memory) {
        File file = new File(path+DB_SUFFIX);

        try {
            if(!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotReadOrWriteException);
        }


        RandomAccessFile fac = null;
        FileChannel fc = null;

        try {
            fac = new RandomAccessFile(file,"rw");
            fc = fac.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new PageCacheImpl(fac,fc,(int) memory/Page.PAGE_SIZE);

    }


    public static PageCache open(String path, long memory) {
        File file = new File(path+DB_SUFFIX);

        if(!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }

        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotReadOrWriteException);
        }


        RandomAccessFile fac = null;
        FileChannel fc = null;

        try {
            fac = new RandomAccessFile(file,"rw");
            fc = fac.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new PageCacheImpl(fac,fc,(int) memory/Page.PAGE_SIZE);

    }



}
