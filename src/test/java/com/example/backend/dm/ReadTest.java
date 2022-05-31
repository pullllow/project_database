package com.example.backend.dm;

import com.example.backend.dm.logger.Logger;
import com.example.backend.dm.pageCache.PageCache;
import com.example.backend.utils.Parser;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * @author Chang Qi
 * @date 2022/5/31 10:07
 * @description
 * @Version V1.0
 */

public class ReadTest {


    @Test
    public void testRead() throws IOException {
        //String path = "D:/Develop/Code/IdeaProjects/project_database/test/test"+ PageCache.DB_SUFFIX;

        String path = "D:/Develop/Code/IdeaProjects/project_database/test/test" + Logger.LOG_SUFFIX;

        File file = new File(path);

        RandomAccessFile raf = new RandomAccessFile(file, "r");
        FileChannel fc = raf.getChannel();


        ByteBuffer buf = ByteBuffer.allocate((int) file.length());

        fc.position(0);
        fc.read(buf);
        System.out.println(Arrays.toString(buf.array()));


        System.out.println("Logger XCheckSum:" + Parser.parseInt(Arrays.copyOfRange(buf.array(), 0, 4)));
        System.out.println("Log Size:" + Parser.parseInt(Arrays.copyOfRange(buf.array(), 4, 8)));
        System.out.println("Log CheckSum:" + Parser.parseInt(Arrays.copyOfRange(buf.array(), 8, 12)));
        System.out.println("Log Type:" + buf.array()[12]);
        System.out.println("Log XID:" + Parser.parseLong(Arrays.copyOfRange(buf.array(), 13, 21)));
        System.out.println("Log PageNo:" + Parser.parseInt(Arrays.copyOfRange(buf.array(), 21, 25)));
        System.out.println("Log Offset:" + Parser.parseShort(Arrays.copyOfRange(buf.array(), 25, 27)));


    }
}
