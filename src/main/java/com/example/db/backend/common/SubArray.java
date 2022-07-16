package com.example.db.backend.common;

/**
 * @author Chang Qi
 * @date 2022/5/26 16:32
 * @description
 * @Version V1.0
 */

public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
