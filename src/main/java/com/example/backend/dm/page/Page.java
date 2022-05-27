package com.example.backend.dm.page;

/**
 * @author Chang Qi
 * @date 2022/5/26 16:56
 * @description
 * @Version V1.0
 */

public interface Page {

    public static final int PAGE_SIZE = 1 << 13; //每页的字节大小

    void lock();

    void unlock();

    void release();

    void setDirty(boolean dirty);

    boolean isDirty();

    int getPageNo();

    byte[] getData();




}
