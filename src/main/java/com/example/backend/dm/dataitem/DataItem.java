package com.example.backend.dm.dataitem;

import com.example.backend.common.SubArray;
import com.example.backend.dm.DataManager;
import com.example.backend.dm.dataitem.impl.DataItemImpl;
import com.example.backend.dm.impl.DataManagerImpl;
import com.example.backend.dm.page.Page;
import com.example.backend.utils.Parser;
import com.example.backend.utils.Types;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

/**
 * @author Chang Qi
 * @date 2022/5/28 9:59
 * @description DataItem 保存数据格式
 * <p>
 * [ValidFlag][DataSize][Data]
 * ValidFlag 表示该DataItem 是否有效（无效设置为0） 1字节
 * DataSize 标识Data的长度 2字节
 * @Version V1.0
 */

public interface DataItem {

    static final int DATAITEM_OS_VALID = 0;
    static final int DATAITEM_OS_SIZE = 1;
    static final int DATAITEM_OS_DATA = 3;

    SubArray data();

    void before();

    void unBefore();

    void after(long xid);

    void release();

    void wLock();

    void wUnlock();

    void rLock();

    void rUnlock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();


    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    public static DataItem parseDataItem(Page page, short offset, DataManagerImpl dm) {
        byte[] raw = page.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DATAITEM_OS_SIZE, offset + DATAITEM_OS_DATA));
        short length = (short) (size + DATAITEM_OS_DATA);
        long uid = Types.addressToUid(page.getPageNo(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], dm, uid, page);
    }


    public static void setDataItemRawInValid(byte[] raw) {
        raw[DataItem.DATAITEM_OS_VALID] = (byte) 1;
    }
}
