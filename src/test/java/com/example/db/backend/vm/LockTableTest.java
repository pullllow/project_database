package com.example.db.backend.vm;

import com.example.db.backend.utils.Panic;
import com.example.db.backend.vm.lock.LockTable;
import org.junit.Test;

/**
 * @author Chang Qi
 * @date 2022/5/31 19:42
 * @description
 * @Version V1.0
 */

public class LockTableTest {

    @Test
    public void testLockTable() {
        LockTable lockTable = new LockTable();

        try {
            lockTable.add(1,1);
        } catch (Exception e) {
            Panic.panic(e);
        }


    }
}
