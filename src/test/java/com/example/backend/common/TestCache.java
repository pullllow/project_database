package com.example.backend.common;

/**
 * @author Chang Qi
 * @date 2022/5/26 16:35
 * @description
 * @Version V1.0
 */

public class TestCache extends AbstractCache<Long>{

    public TestCache() {
        super(50);
    }

    @Override
    protected Long getForCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseForCache(Long obj) {

    }
}
