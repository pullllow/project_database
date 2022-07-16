package com.example.db.backend.parser.statement;

/**
 * @author Chang Qi
 * @date 2022/6/5 20:49
 * @description
 * <create statement>
 *     sql： create table <table name>
 *              <field name><field type>
 *              <field name><field type>
 *            [index <field name list>]
 *
 * @Version V1.0
 */

public class Create {
    public String tableName;
    public String[] fieldName;
    public String[] fieldType;
    public String[] index;
}
