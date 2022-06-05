package com.example.backend.im;

import com.example.backend.common.SubArray;
import com.example.backend.dm.dataitem.DataItem;
import com.example.backend.tm.TransactionManager;
import com.example.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author Chang Qi
 * @date 2022/5/31 20:06
 * @description [LeafFlag][KeyNumber][SiblingUId]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * <p>
 * LeafFlag 标记该店是否是叶子节点 1字节
 * KeyNumber 该节点中key个数 2字节
 * SiblingUid 其兄弟节点存储在DM中的UID 8字节
 * Soni 8字节
 * Keyi 8字节
 * <p>
 * KeyN 始终为MAX_VALUE
 * @Version V1.0
 */

public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NUM_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
    static final int SIBLING_OFFSET = NUM_KEYS_OFFSET + 2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);  // 1067

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    private static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    private static boolean getRawIsLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    private static void setRawNumKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.raw, NUM_KEYS_OFFSET, 2);
    }

    private static int getRawNumKeys(SubArray raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NUM_KEYS_OFFSET, raw.start + SIBLING_OFFSET));
    }

    private static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, SIBLING_OFFSET, 8);
    }

    private static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + NODE_HEADER_SIZE));
    }

    private static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    private static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    private static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    private static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }


    /**
     * 生成根节点数据
     * 根节点初始两个子节点 left和right，初始键值为key
     *
     * @param left
     * @param right
     * @param key
     * @return byte[]
     **/
    public static byte[] newRootRaw(long left, long right, long key) {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, false);
        setRawNumKeys(raw, 2);
        setRawSibling(raw, 2);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);
        return raw.raw;
    }


    public static byte[] newNullRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, true);
        setRawNumKeys(raw, 0);
        setRawSibling(raw, 0);
        return raw.raw;
    }


    public static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    public static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
        int end = raw.start + NODE_SIZE - 1;
        for (int i = end; i >= begin; i--) {
            raw.raw[i] = raw.raw[i - (8 * 2)];
        }
    }

    public static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIsLeaf(raw);
        } finally {
            dataItem.rUnlock();
        }
    }

    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    /**
     * 辅助B+树做插入操作,寻找对应key的UID，如果找不到，则返回兄弟节点的UID
     *
     * @param key
     * @return com.example.backend.im.Node.SearchNextRes
     **/
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int numKeys = getRawNumKeys(raw);
            for (int i = 0; i < numKeys; i++) {
                long kthKey = getRawKthKey(raw, i);
                if (key < kthKey) {
                    res.uid = getRawKthKey(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;
        } finally {
            dataItem.rUnlock();
        }

    }

    class LeafSearchRangeRes {
        List<Long> uidList;
        long siblingUid;
    }

    /**
     * 在当前节点进行范围查找
     * 如果rightKey 大于等于该节点的最大key，则同时返回兄弟节点的UID，方便继续搜索下一节点
     *
     * @param leftKey
     * @param rightKey
     * @return com.example.backend.im.Node.LeafSearchRangeRes
     **/
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();

        try {
            int numKeys = getRawNumKeys(raw);
            int kth = 0;
            while (kth < numKeys) {
                long kthKey = getRawKthKey(raw, kth);
                if (kthKey >= leftKey) {
                    break;
                }
                kth++;
            }
            List<Long> uidList = new ArrayList<>();
            while (kth < numKeys) {
                long kthKey = getRawKthKey(raw, kth);
                if (kthKey <= rightKey) {
                    uidList.add(getRawKthSon(raw, kth));
                    kth++;
                } else {
                    break;
                }
            }

            long siblingUid = 0;
            if (kth == rightKey) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uidList = uidList;
            res.siblingUid = siblingUid;
            return res;


        } finally {
            dataItem.rUnlock();
        }

    }

    class InsertAndSplitRes {
        long siblingUid;
        long newSon;
        long newKey;
    }

    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception error = null;

        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();

        try {
            success = insert(uid, key);
            if (!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if (needSpilt()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch (Exception e) {
                    error = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if (error == null && success) {
                dataItem.after(TransactionManager.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }

    }

    private boolean insert(long uid, long key) {
        int numKeys = getRawNumKeys(raw);
        int kth = 0;
        while (kth < numKeys) {
            long kthKey = getRawKthKey(raw, kth);
            if (kthKey < numKeys) {
                kth++;
            } else {
                break;
            }
        }

        if (kth == numKeys && getRawSibling(raw) != 0) return false;

        if (getRawIsLeaf(raw)) {
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNumKeys(raw, numKeys + 1);
        } else {
            long kthKey = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth + 1);
            setRawKthKey(raw, kthKey, kth + 1);
            setRawKthSon(raw, uid, kth + 1);
            setRawNumKeys(raw, numKeys + 1);
        }
        return true;
    }

    private boolean needSpilt() {
        return BALANCE_NUMBER*2 == getRawNumKeys(raw);
    }

    class SplitRes {
        long newSon;
        long newKey;
    }

    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIsLeaf(raw));
        setRawNumKeys(nodeRaw,BALANCE_NUMBER);
        setRawSibling(nodeRaw,getRawSibling(raw));
        copyRawFromKth(raw,nodeRaw,BALANCE_NUMBER);
        long son = tree.dm.insert(TransactionManager.SUPER_XID, nodeRaw.raw);
        setRawNumKeys(raw,BALANCE_NUMBER);
        setRawSibling(raw,son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }


}
