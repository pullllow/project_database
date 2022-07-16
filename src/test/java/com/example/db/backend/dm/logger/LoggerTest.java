package com.example.db.backend.dm.logger;

import org.junit.Test;

import java.io.File;
import java.util.Arrays;

/**
 * @author Chang Qi
 * @date 2022/5/27 14:22
 * @description
 * @Version V1.0
 */

public class LoggerTest {

    @Test
    public void testLogger() {


        String path = "D:/Develop/Code/IdeaProjects/project_database/test/test_logger";
        assert new File(path+Logger.LOG_SUFFIX).delete();
        Logger lg = Logger.create(path);

        lg.log("aaa".getBytes());
        lg.log("bbb".getBytes());
        lg.log("cc".getBytes());
        lg.log("dd".getBytes());
        lg.close();

        lg = Logger.open(path);

        lg.rewind();
        byte[] next = null;
        while((next = lg.next()) != null) {
            System.out.println(Arrays.toString(next));
        }



    }

}
