package com.example.backend.common;

import com.example.backend.utils.Panic;
import com.example.common.Error;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author Chang Qi
 * @date 2022/5/26 16:34
 * @description
 * @Version V1.0
 */


public class CacheTest {

    static Random random = new SecureRandom();


    private CountDownLatch cdl;
    private TestCache cache;

    private int workers = 200;
    private int works = 1000;

    @Test
    public void testCache() {
        cache = new TestCache();
        cdl = new CountDownLatch(workers);

        for(int i=0;i<workers;i++) {
            Runnable r = () -> work();
            new Thread(r).run();
        }

        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    private void work() {

        for(int i=0;i<works;i++) {
            long uid = random.nextInt();
            long h = 0;
            try {
                h = cache.get(uid);
            } catch (Exception e) {
                if(e == Error.CacheFullException) continue;
                Panic.panic(e);
            }
            assert h==uid;
            cache.release(h);
        }
        cdl.countDown();

    }
}
