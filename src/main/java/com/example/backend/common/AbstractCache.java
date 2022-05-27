package com.example.backend.common;

import com.example.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Chang Qi
 * @date 2022/5/26 15:53
 * @description
 * @Version V1.0
 */

public abstract class AbstractCache<T> {

    private HashMap<Long, T> cache;                          // 实际缓存数据
    private HashMap<Long, Integer> references;               // 元素的引用个数
    private HashMap<Long, Boolean> getting;                  // 正在被获取的资源

    private int maxResource;                                // 缓存中最大资源数
    private int count = 0;                                  // 缓存中的元素个数
    private Lock lock;


    // 当资源不存在缓存时的获取行为
    protected abstract T getForCache(long key) throws Exception;

    // 当资源被驱逐时的回写行为
    protected abstract void releaseForCache(T t);


    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {

        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                //请求的资源正在被其他线程获取
                lock.unlock();

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if (cache.containsKey(key)) {
                // 请求资源在缓存中，且没有被其他线程获取
                T t = cache.get(key);
                references.put(key, references.get(key) + 1); // 引用+1
                lock.unlock();
                return t;
            }

            // 从数据源中获取请求资源
            if (maxResource > 0 && count == maxResource) {
                // 缓存容量超过最大容量
                lock.unlock();
                throw Error.CacheFullException;
            }
            count++;
            getting.put(key,true);
            lock.unlock();
            break;

        }

        // 从数据源中获取请求资源
        T t = null;

        try {
            t = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        // 将请求资源放入到内存中
        lock.lock();
        getting.remove(key);
        cache.put(key,t);
        references.put(key,1);
        lock.unlock();

        return t;

    }


    /**
     * 强行释放一个资源
     * @param key
     * @return void
     *
     **/
    protected void release(long key) {
        // 释放资源 references-1 如果ref==0 回源并删除所有相关结构
        lock.lock();

        try {
            int ref = references.get(key)-1;
            if(ref==0) {
                T t = cache.get(ref);
                releaseForCache(t);
                references.remove(key);
                cache.remove(key);
                count--;

            } else {
                references.put(key,ref);
            }
        } finally {
            lock.unlock();
        }

    }

    /**
     * 安全关闭功能，将缓存中的所有资源强行回源
     * @return void
     *
     **/
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                release(key);
                references.remove(key);
                cache.remove(key);

            }
        } finally {
            lock.unlock();
        }


    }



}
