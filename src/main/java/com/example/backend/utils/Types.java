package com.example.backend.utils;

public class Types {
    public static long addressToUid(int pageNo, short offset) {
        long u0 = (long)pageNo;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}
