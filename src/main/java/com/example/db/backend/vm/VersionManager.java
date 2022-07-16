package com.example.db.backend.vm;

/**
 * @author Chang Qi
 * @date 2022/5/31 13:58
 * @description
 * @Version V1.0
 */

public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);

    void committed(long xid) throws Exception;

    void aborted(long xid);
}
