package com.example.common;

/**
 * @author Chang Qi
 * @date 2022/5/25 19:57
 * @description
 * @Version V1.0
 */

public class Error {

    //common
    public static final Exception CacheFullException = new RuntimeException("Cache is full!");
    public static final Exception FileExistsException = new RuntimeException("File already exists!");
    public static final Exception FileNotExistsException = new RuntimeException("File not exists!");
    public static final Exception FileCannotReadOrWriteException = new RuntimeException("File cannot read or write!");


    //tm
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");

    //dm
    public static final Exception MemTooSmallException = new RuntimeException("Memory too small!");


}
