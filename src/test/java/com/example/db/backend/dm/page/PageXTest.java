package com.example.db.backend.dm.page;

import org.junit.Test;

/**
 * @author Chang Qi
 * @date 2022/5/26 20:19
 * @description
 * @Version V1.0
 */

public class PageXTest {


    @Test
    public void test() {

//        int[] arr = new int[]{1,2,3,4,5};
//        System.arraycopy(new int[2],0,arr,0,2);
//        System.out.println(Arrays.toString(arr));

        byte[] raw = PageX.initRaw();
        //System.out.println(Arrays.toString(raw));
        System.out.println(Page.PAGE_SIZE);


    }

}
