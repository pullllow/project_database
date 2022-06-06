package com.example.backend.tbm.impl;

import com.example.backend.dm.DataManager;
import com.example.backend.parser.statement.*;
import com.example.backend.tbm.BeginRes;
import com.example.backend.tbm.booter.Booter;
import com.example.backend.tbm.TableManager;
import com.example.backend.tbm.table.Table;
import com.example.backend.utils.Parser;
import com.example.backend.vm.VersionManager;



import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Chang Qi
 * @date 2022/6/5 20:57
 * @description
 * @Version V1.0
 */

public class TableManagerImpl implements TableManager {
    public VersionManager vm;
    public DataManager dm;

    private Booter booter;
    private Map<String, Table> tableCache;
    private Map<Long, List<Table>> xidTableCache;
    private Lock lock;


    public TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        this.lock = new ReentrantLock();
        loadTables();

    }

    private void loadTables() {
            long uid = firstTableUid();
            while(uid !=0) {
                Table tb = Table.loadTable(this,uid);
                uid = tb.nextUid;
                tableCache.put(tb.name, tb);
            }
    }

    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    @Override
    public BeginRes begin(Begin begin) {
        return null;
    }

    @Override
    public byte[] commit(long xid) throws Exception {
        return new byte[0];
    }

    @Override
    public byte[] abort(long xid) {
        return new byte[0];
    }

    @Override
    public byte[] show(long xid) {
        return new byte[0];
    }

    @Override
    public byte[] create(long xid, Create create) throws Exception {
        return new byte[0];
    }

    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        return new byte[0];
    }

    @Override
    public byte[] read(long xid, Select select) throws Exception {
        return new byte[0];
    }

    @Override
    public byte[] update(long xid, Update update) throws Exception {
        return new byte[0];
    }

    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        return new byte[0];
    }
}
