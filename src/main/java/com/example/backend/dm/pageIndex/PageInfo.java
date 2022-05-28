package com.example.backend.dm.pageIndex;

/**
 * @author Chang Qi
 * @date 2022/5/28 11:07
 * @description
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
