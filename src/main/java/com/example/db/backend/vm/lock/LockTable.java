package com.example.db.backend.vm.lock;

import com.example.db.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Chang Qi
 * @date 2022/5/31 15:25
 * @description 维护依赖等待图，用于死锁检测
 * @Version V1.0
 */

public class LockTable {

    private Map<Long, List<Long>> x2u;              // 某个XID已经获取的资源UID列表
    private Map<Long, Long> u2x;                    // UID被某个XID获取
    private Map<Long, List<Long>> waitX;            // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;               // 正在等待资源的XID锁
    private Map<Long, Long> waitU;                  // XID正在等待的UID
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        waitX = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 事务不需要等待返回null, 否则返回锁对象
     * 造成死锁则抛出异常
     *
     * @param xid
     * @param uid
     * @return java.util.concurrent.locks.Lock
     **/
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();

        try {
            if (isInList(x2u, xid, uid)) {
                return null;
            }
            if(!u2x.containsKey(uid)) {
                u2x.put(uid,xid);
                putIntoList(x2u,xid,uid);
                return null;
            }
            waitU.put(xid,uid);
            putIntoList(waitX,xid,uid);
            if(hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(waitX, uid, xid); ///
                throw Error.DeadLockException;
            }
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid,l);
            return l;

        } finally {
            lock.unlock();
        }
    }



    public void remove(long xid) {
        lock.lock();

        try {
            List<Long> list = x2u.get(xid);
            if(list!=null) {
                while(list.size()>0) {
                    Long uid = list.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }

    }


    /**
     * 从等待队列中选择一个xid来占用uid
     * @param uid
     * @return void
     *
     **/
    private void selectNewXID(Long uid) {
        u2x.remove(uid);
        List<Long> list = waitX.get(uid);
        if(list==null) return;
        assert  list.size()>0;

        while(list.size()>0) {
            long xid = list.remove(0);
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid,xid);
                Lock l = waitLock.remove(xid);
                waitU.remove(xid);
                l.unlock();
                break;
            }
        }

        if(list.size()==0) waitX.remove(uid);
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for(long xid: x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if(s!=null && s>0) {
                continue;
            }
            stamp++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp==stamp) {
            return true;
        }
        if(stp != null && stp<stamp) {
            return false;
        }

        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        if(uid==null) return false;
        Long x = u2x.get(uid);
        assert  x != null;
        return dfs(x);

    }

    private void removeFromList(Map<Long, List<Long>> listMap, long id0, long id1) {
        List<Long> list = listMap.get(id0);
        if(list==null) return;
        Iterator<Long> iterator = list.iterator();
        while(iterator.hasNext()) {
            long next = iterator.next();
            if(next==id1) {
                iterator.remove();
                break;
            }
        }
        if(list.isEmpty()) {
            listMap.remove(id0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long id0, long id1) {
        if(!listMap.containsKey(id0)) {
            listMap.put(id0, new ArrayList<>());
        }
        listMap.get(id0).add(0,id1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long id0, long id1) {
        List<Long> list = listMap.get(id0);
        if(list==null) return false;
        Iterator<Long> iterator = list.iterator();
        while(iterator.hasNext()) {
            long next = iterator.next();
            if(next==id1) return true;
        }
        return false;

    }

}
