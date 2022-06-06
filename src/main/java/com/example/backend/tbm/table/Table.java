package com.example.backend.tbm.table;

import com.example.backend.tbm.TableManager;
import com.example.backend.tbm.field.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Chang Qi
 * @date 2022/6/6 15:34
 * @description
 * @Version V1.0
 */

public class Table {
    public TableManager tbm;
    public long uid;
    public String name;
    public byte status;
    public long nextUid;
    public List<Field> fields = new ArrayList<>();

}
