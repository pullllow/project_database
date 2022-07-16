package com.example.db.backend.tm;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * @author Chang Qi
 * @date 2022/5/25 21:30
 * @description
 * @Version V1.0
 */

public class TransactionManagerTest {

    public static final String curTestPath = "D:/Develop/Code/IdeaProjects/project_database/test/";

    private Random random = new SecureRandom();

    private Lock lock = new ReentrantLock();
    private Map<Long, Byte> transMap;
    private CountDownLatch cdl;
    private TransactionManager tm;

    private int transCnt = 0;
    private int noWorkers = 50;
    private int noWorks = 3000;

    @Test
    public void testCreateTM() {

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String time = sdf.format(date);
        String path = "D:/Develop/Code/IdeaProjects/project_database/test/test_" + time;
        // System.out.println(path);
        // TransactionManager tm = TransactionManager.create(path);
        TransactionManager tm = TransactionManager.create(path);

    }


    @Test
    public void testOpenTM() {

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String time = sdf.format(date);
        String path = "D:/Develop/Code/IdeaProjects/project_database/test/test_" + time;
        //System.out.println(path);
        //TransactionManager tm = TransactionManager.create(path);
        TransactionManager tm = TransactionManager.open(path);

    }


    @Test
    public void testConcurrency() {

        this.tm = TransactionManager.create(curTestPath + "test_concurrency");

        transMap = new ConcurrentHashMap<>();
        cdl = new CountDownLatch(noWorkers);

        for (int i = 0; i < noWorkers; i++) {
            Runnable r = () -> worker();
            new Thread(r).run();
        }

        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //assert new File(curTestPath + "test_concurrency.xid").delete();

    }

    private void worker() {

        boolean inTrans = false;

        long transXID = 0;

        for (int i = 0; i < noWorks; i++) {
            int op = Math.abs(random.nextInt(6));

            if (op == 0) {
                lock.lock();
                if (inTrans == false) {
                    long xid = tm.begin();
                    transMap.put(xid, (byte) 0);
                    transCnt++;
                    transXID = xid;
                    inTrans = true;
                } else {
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch (status) {
                        case 1:
                            tm.commit(transXID);
                            break;
                        case 2:
                            tm.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte) status);
                    inTrans = false;
                }
                lock.unlock();

            } else {
                lock.lock();
                if (transCnt > 0) {
                    long xid = (long) ((random.nextInt(Integer.MAX_VALUE) % transCnt) + 1);
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = tm.isActive(xid);
                            break;
                        case 1:
                            ok = tm.isCommitted(xid);
                            break;
                        case 2:
                            ok = tm.isAborted(xid);
                            break;
                    }
                    assert ok;
                }
                lock.unlock();

            }

        }
        cdl.countDown();

    }

    @Test
    public void testFile() {

        File file = new File(curTestPath + "test_concurrency.xid");

        ByteBuffer buf = ByteBuffer.allocate((int)file.length());
        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
           raf = new RandomAccessFile(file,"r");
           fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(buf.array());

    }

}
