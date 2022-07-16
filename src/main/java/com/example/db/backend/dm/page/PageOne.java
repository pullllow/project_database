package com.example.db.backend.dm.page;

import com.example.db.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * @author Chang Qi
 * @date 2022/5/26 19:34
 * @description 特殊管理第一页
 * ValidCheck
 * DB启动时给100-107字节处插入一个随机字节，DB关闭时将其拷贝到108-115字节
 * 用于判断上次数据库是否是正常关闭
 * @Version V1.0
 */

public class PageOne {

    private static final int OS_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] initRaw() {
        byte[] raw = new byte[Page.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    /**
     * 数据库启动时设置初始字节
     *
     * @param page
     * @return void
     **/
    public static void setVcOpen(Page page) {
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OS_VC, LEN_VC);
    }

    /**
     * 数据库关闭时拷贝ValidCheck字节
     *
     * @param page
     * @return void
     **/
    public static void setVcClose(Page page) {
        page.setDirty(true);
        setVcClose(page.getData());

    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OS_VC, raw, OS_VC + LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page page) {
        return checkVc(page.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OS_VC, OS_VC + LEN_VC), Arrays.copyOfRange(raw, OS_VC + LEN_VC, OS_VC + 2 * LEN_VC));
    }


}
