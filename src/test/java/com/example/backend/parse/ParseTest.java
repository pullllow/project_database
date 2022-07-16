package com.example.backend.parse;

import com.example.backend.parser.Parser;
import com.example.backend.parser.statement.Create;
import com.example.backend.parser.statement.Drop;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author Chang Qi
 * @date 2022/6/7 15:10
 * @description
 * @Version V1.0
 */

public class ParseTest {


    @Test
    public void testCreate() throws Exception{
        String sql = "create table student id int32, name string, uid int64, (index name id uid)";
        Object res = Parser.parse(sql.getBytes());
        Create create = (Create)res;
        assert "student".equals(create.tableName);
        System.out.println("Create");
        for (int i = 0; i < create.fieldName.length; i++) {
            System.out.println(create.fieldName[i] + ":" + create.fieldType[i]);
        }
        System.out.println(Arrays.toString(create.index));
        System.out.println("======================");
    }

    @Test
    public void testDrop() throws Exception {
        String sql = "drop table student";
        Drop drop = (Drop) Parser.parse(sql.getBytes());
        System.out.println(drop.tableName);
    }

}
