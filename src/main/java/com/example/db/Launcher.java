package com.example.db;

import com.example.db.backend.dm.DataManager;
import com.example.db.backend.server.Server;
import com.example.db.backend.tbm.TableManager;
import com.example.db.backend.tm.TransactionManager;
import com.example.db.backend.utils.Panic;
import com.example.db.backend.vm.VersionManager;
import com.example.db.backend.vm.impl.VersionManagerImpl;
import com.example.db.common.Error;
import org.apache.commons.cli.*;

import java.util.Date;



/**
 * @author Chang Qi
 * @date 2022/7/16 15:03
 * @description
 * @Version V1.0
 */

public class Launcher {

    public static final int port = 9999;

    public static final long DEFAULT_MEM = (1 << 20) * 64;
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create",true,"-create DBPath");
        options.addOption("mem",true,"-mem 64MB");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options,args);

        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"),parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
        //createDB("D:/Git-Space/MYDB/tmp/mydb");
        openDB("D:/Git-Space/MYDB/tmp/mydb",64*1024*1024);

    }

    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFAULT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path,vm,dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port,tbm).start();
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFAULT_MEM;
        }

        if(memStr.length()<2) {
            Panic.panic(Error.InvalidMemException);
        }

        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0,memStr.length()-2));
        switch (unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);

        }
        return DEFAULT_MEM;

    }

}
