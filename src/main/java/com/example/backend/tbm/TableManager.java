package com.example.backend.tbm;

import com.example.backend.dm.DataManager;
import com.example.backend.parser.statement.*;
import com.example.backend.tbm.booter.Booter;
import com.example.backend.tbm.impl.TableManagerImpl;
import com.example.backend.utils.Parser;
import com.example.backend.vm.VersionManager;

/**
 * @author Chang Qi
 * @date 2022/6/5 20:47
 * @description
 * @Version V1.0
 */

public interface TableManager {
    BeginRes begin(Begin begin);

    byte[] commit(long xid) throws Exception;

    byte[] abort(long xid);

    byte[] show(long xid);

    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;

    byte[] read(long xid, Select select) throws Exception;

    byte[] update(long xid, Update update) throws Exception;

    byte[] delete(long xid, Delete delete) throws Exception;

    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }

}
