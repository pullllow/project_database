package com.example.backend.dm.page;

import com.example.backend.utils.Parser;

import java.util.Arrays;

/**
 * @author Chang Qi
 * @date 2022/5/26 17:32
 * @description
 *   PageX 管理普通页
 *   普通页结构
 *   [FreeSpaceOffset] [Data]
 *   FreeSpaceOffset: 2字节 空闲位置开始偏移
 *   页最大数据量 1<<13-2
 *
 * @Version V1.0
 */

public class PageX {

    // 用short
    private static final short OS_FREE = 0;
    private static final short OS_DATA = 2;

    public static final int MAX_FREE_SPACE = Page.PAGE_SIZE - OS_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[Page.PAGE_SIZE];
        setFSO(raw, OS_DATA);
        return raw;

    }

    private static void setFSO(byte[] raw, short offData) {
        // 设置初始化Free Offset Space = 2
        System.arraycopy(Parser.short2Byte(offData),0,raw, OS_FREE, OS_DATA);
    }

    /**
     * 获取Page的FreeOffsetSpace
     * @param page
     * @return short
     *
     **/
    public static short getFSO(Page page) {
        return getFSO(page.getData());
    }
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw,0,2));
    }

    /**
     * 返回page的空闲空间大小
     * @param page
     * @return int
     *
     **/
    public static int getFreeSpace(Page page) {
        return Page.PAGE_SIZE - (int)getFSO(page.getData());
    }

    /**
     * 将raw插入page中。并返回插入位置(不是更新后的FSO)
     * @param page
     * @param raw
     * @return short
     *
     **/
    public static short insert(Page page, byte[] raw) {
        page.setDirty(true);

        short offset = getFSO(page.getData());

        System.arraycopy(raw,0,page.getData(),offset,raw.length);
        setFSO(page.getData(), (short) (offset+raw.length));
        return offset;
    }


    /**
     * 数据库崩溃后重新打开，恢复例程直接插入数据
     * 将raw插入page中的offset位置，并将page的offset设置为较大的offset
     * @param page
     * @param raw
     * @param offset
     * @return void
     *
     **/
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw,0,page.getData(),offset,raw.length);

        short rawFSO = getFSO(page.getData());
        if(rawFSO<offset+raw.length) {
            setFSO(page.getData(), (short) (offset+raw.length));
        }
    }

    /**
     * 数据库崩溃后重新打开，修改数据不更新
     * @param page
     * @param raw
     * @param offset
     * @return void
     *
     **/
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw,0,page.getData(),offset,raw.length);
    }


}
