package com.example.db.backend.dm.pageIndex;

import com.example.db.backend.dm.page.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Chang Qi
 * @date 2022/5/28 11:05
 * @description
 *  方便DataItem 快速定位插入，List+ArrayList实现形式 （相当于HashMap的数组+链表实现）
 *
 * @Version V1.0
 */

public class PageIndex {

    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = Page.PAGE_SIZE / INTERVALS_NO; //204


    private Lock lock;
    private List<PageInfo>[] lists;



    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];

        for(int i=0;i<INTERVALS_NO+1;i++) {
            lists[i] = new ArrayList<>();
        }

    }

    /**
     * 从PageIndex 中获取页面 算出区间号，直接获取
     * 被选择的页，会直接从PageIndex 中移除，同一页面不允许并发写
     * 上层模块使用完该页面后，需要将其重新加入PageIndex
     * @param spaceSize
     * @return com.example.db.backend.dm.pageIndex.PageInfo
     *
     **/
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize/THRESHOLD;

            if(number<INTERVALS_NO) number++;
            while (number<=INTERVALS_NO) {
                if(lists[number].size()==0) {
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    public void add(int pageNo,int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace/THRESHOLD;
            lists[number].add(new PageInfo(pageNo,freeSpace));
        } finally {
            lock.unlock();
        }
    }


}
