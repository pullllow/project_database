package com.example.db.common;

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
    public static final Exception BadLogFileErrorException = new RuntimeException("Bad log file!");
    public static final Exception DataTooLargeException = new RuntimeException("Data too lager!");
    public static final Exception DatabaseBusyException = new RuntimeException("Database busy!");

    //vm
    public static final Exception NullEntryException = new RuntimeException("Null Entry!");
    public static final Exception ConcurrentUpdateException = new RuntimeException("Concurrent update issue!");
    public static final Exception DeadLockException = new RuntimeException("Deadlock!");

    //parser
    public static final Exception InvalidCommandException = new RuntimeException("Invalid command!");

    //tbm
    public static final Exception FieldNotFoundException = new RuntimeException("Field not found!");
    public static final Exception InvalidValuesException = new RuntimeException("Invalid values!");
    public static final Exception FieldNotIndexedException = new RuntimeException("Filed not indexed!");
    public static final Exception InvalidLogOpException = new RuntimeException("Invalid logic operation!");
    public static final Exception InvalidFieldException = new RuntimeException("Invalid field type!");

}
