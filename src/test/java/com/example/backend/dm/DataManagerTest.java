package com.example.backend.dm;

import com.example.backend.dm.logger.Logger;
import com.example.backend.dm.page.Page;
import com.example.backend.dm.pageCache.PageCache;
import com.example.backend.tm.TransactionManager;
import com.example.backend.utils.Parser;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * @author Chang Qi
 * @date 2022/5/30 10:19
 * @description
 * @Version V1.0
 */

public class DataManagerTest {



    @Test
    public void testTM() throws IOException {

        String path = "D:/Develop/Code/IdeaProjects/project_database/test/test_tm";
        TransactionManager tm = TransactionManager.open(path);

        /*tm.begin();
        tm.begin();
        tm.begin();*/

        tm.abort(1);
        tm.commit(2);

        File file = new File(path+TransactionManager.XID_SUFFIX);

        RandomAccessFile raf = null;
        FileChannel fc = null;


        raf = new RandomAccessFile(file,"rw");
        fc = raf.getChannel();

        byte[] cnt = new byte[8];

        ByteBuffer header = ByteBuffer.wrap(cnt);

        fc.position(0);
        fc.read(header);
        long counter = Parser.parseLong(cnt);

        System.out.println(counter);

        byte[] xid = new byte[(int) (file.length()-8)];

        ByteBuffer buf = ByteBuffer.wrap(xid);

        fc.position(8);
        fc.read(buf);
        System.out.println(Arrays.toString(xid));

        fc.close();
        raf.close();

    }



    @Test
    public void testDm() throws Exception {

        String path = "D:/Develop/Code/IdeaProjects/project_database/test/test";
        TransactionManager tm = TransactionManager.create(path);

        DataManager dm = DataManager.create(path, Page.PAGE_SIZE * 10, tm);



        byte[] temp = new byte[60];
        Arrays.fill(temp,(byte)12);


        long uid = dm.insert(0, temp);
        System.out.println(uid);

        new File(path+PageCache.DB_SUFFIX).delete();
        new File(path+ Logger.LOG_SUFFIX).delete();
        new File(path+TransactionManager.XID_SUFFIX).delete();

    }

    @Test
    public void testReadFile() throws Exception {
        String path = "D:/Develop/Code/IdeaProjects/project_database/test/test";
        File file = new File(path+".log");

        RandomAccessFile raf = null;
        FileChannel fc = null;


        raf = new RandomAccessFile(file,"rw");
        fc = raf.getChannel();

        byte[] cnt = new byte[8];

       /* ByteBuffer header = ByteBuffer.wrap(cnt);

        fc.position(0);
        fc.read(header);
        long counter = Parser.parseLong(cnt);

        System.out.println(counter);

        byte[] xid = new byte[(int) (file.length()-8)];

        ByteBuffer buf = ByteBuffer.wrap(xid);

        fc.position(8);
        fc.read(buf);
        System.out.println(Arrays.toString(xid));*/

        ByteBuffer buf = ByteBuffer.allocate((int) file.length());

        fc.position(0);
        fc.read(buf);

        System.out.println(Arrays.toString(buf.array()));

        fc.close();
        raf.close();
    }


}
