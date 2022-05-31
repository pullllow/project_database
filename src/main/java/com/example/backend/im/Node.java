package com.example.backend.im;

import com.example.backend.common.SubArray;
import com.example.backend.dm.dataitem.DataItem;

/**
 * @author Chang Qi
 * @date 2022/5/31 20:06
 * @description
 *
 * [LeafFlag][KeyNumber][SiblingUId]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 *
 * LeafFlag 标记该店是否是叶子节点 1字节
 * KeyNumber 该节点中key个数 2字节
 * SiblingUid 其兄弟节点存储在DM中的UID 8字节
 *
 * KeyN 始终为MAX_VALUE
 *
 * @Version V1.0
 */

public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8) * (BALANCE_NUMBER*2+2);  // 1067

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    /**
     * 生成根节点数据
     * 根节点初始两个子节点 left和right，初始键值为key
     * @param left
     * @param right
     * @param key
     * @return byte[]
     *
     **/
    static byte[] newRootRaw(long left, long right, long key) {
        SubArray raw = new SubArray(new byte[NODE_SIZE],0, NODE_SIZE);
        setRawIsLeaf(raw,false);
        setRawNoKeys(raw,2);
        setRawSibling(raw,2);
        setRawKthSon(raw,left,0);
        setRawKthKey(raw,key,0);
        setRawKthSon(raw,right,1);
        setRawKthKey(raw, Long.MAX_VALUE,1);
        return raw.raw;
    }


    static byte[] newNullRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE],0, NODE_SIZE);
        setRawIsLeaf(raw,true);
        setRawNoKeys(raw,0);
        setRawSibling(raw,0);
        return raw.raw;
    }

    private static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    private static boolean getRawIsLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET]==(byte)1;
    }

    private static void setRawNoKeys(SubArray raw, int noKeys) {
    }


    private static void setRawKthKey(SubArray raw, long key, int i) {

    }

    private static void setRawKthSon(SubArray raw, long left, int i) {

    }

    private static void setRawSibling(SubArray raw, int i) {

    }





}
