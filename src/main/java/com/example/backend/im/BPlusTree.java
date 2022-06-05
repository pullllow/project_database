package com.example.backend.im;

import com.example.backend.common.SubArray;
import com.example.backend.dm.DataManager;
import com.example.backend.dm.dataitem.DataItem;
import com.example.backend.tm.TransactionManager;
import com.example.backend.utils.Parser;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Chang Qi
 * @date 2022/5/31 20:09
 * @description
 * @Version V1.0
 */

public class BPlusTree {
    public DataManager dm;
    public long bootUid;

    public DataItem bootDataItem;
    public Lock bootLock;

    public static long create(DataManager dm) throws  Exception {
        byte[] rawRoot = Node.newNullRootRaw();
        long rootUid = dm.insert(TransactionManager.SUPER_XID, rawRoot);
        return dm.insert(TransactionManager.SUPER_XID, Parser.long2Byte(rootUid));
    }

    public static BPlusTree load(long bootUid, DataManager dm) throws  Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.dm = dm;
        t.bootUid = bootUid;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw,sa.start,sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManager.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid),0,diRaw.raw,diRaw.start,8);
            bootDataItem.after(TransactionManager.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    private long searchLeaf(long nodeUid, long key) throws  Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();
        if(isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next,key);
        }
    }

    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this,nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid!=0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    public List<Long> search(long key) throws Exception {
        return searchRange(key,key);
    }

    public List<Long> searchRange(long leftKey, long rightKey) throws Exception{
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid,leftKey);
        List<Long> uidList = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this,leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uidList.addAll(res.uidList);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uidList;
    }

    public void insert(long key, long uid) throws  Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert  res !=null;
        if(res.newNode != 0) {
            updateRootUid(rootUid,res.newNode,res.newKey);
        }
    }

    class InsertRes {
        long newNode;
        long newKey;
    }

    private InsertRes insert(long nodeUid, long uid, long key) throws  Exception{
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if(isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid,key);
            if(ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid !=0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }

        }
    }

}
