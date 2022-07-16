package com.example.db.backend.dm.pageIndex;

/**
 * @author Chang Qi
 * @date 2022/5/28 11:07
 * @description
 * 储存页剩余空间结构体
 *
 * @Version V1.0
 */

public class PageInfo {
    public int pageNo;
    public int freeSpace;

    public PageInfo(int pageNo, int freeSpace) {
        this.pageNo = pageNo;
        this.freeSpace = freeSpace;
    }
}
