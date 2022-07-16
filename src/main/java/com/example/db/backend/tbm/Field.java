package com.example.db.backend.tbm;

import com.example.db.backend.im.BPlusTree;
import com.example.db.backend.parser.statement.SingleExpression;
import com.example.db.backend.tbm.impl.TableManagerImpl;
import com.example.db.backend.tbm.utils.FieldCalRes;
import com.example.db.backend.tbm.utils.ParseValueRes;
import com.example.db.backend.tm.TransactionManager;
import com.example.db.backend.utils.Panic;
import com.example.db.backend.utils.ParseStringRes;
import com.example.db.backend.utils.Parser;
import com.example.db.common.Error;
import com.google.common.primitives.Bytes;

import java.util.Arrays;
import java.util.List;

/**
 * @author Chang Qi
 * @date 2022/6/6 15:36
 * @description 表示字段信息
 * [FieldName][TypeName][IndexUid]
 *  无索引 IndexUid = 0
 * @Version V1.0
 */

public class Field {

    public long uid;

    private Table tb;

    public String fieldName;
    public String fieldType;

    private long index;
    private BPlusTree bt;


    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl) tb.tbm).vm.read(TransactionManager.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);

    }

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;
        position += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));

        if (index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            } catch (Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        if (indexed) {
            long index = BPlusTree.create(((TableManagerImpl) tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid);
        return f;
    }

    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        this.uid = ((TableManagerImpl) tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    private static void typeCheck(String fieldType) throws Exception {
        if (!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    public boolean isIndexed() {
        return index != 0;
    }

    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bt.insert(uKey, uid);
    }

    private long value2Uid(Object key) {
        long uid = 0;
        switch (fieldType) {
            case "string":
                uid = Parser.str2Uid((String) key);
                break;
            case "int32":
                int uint = (int) key;
                return (long) uint;
            case "int64":
                uid = (long) key;
                break;
        }
        return uid;
    }

    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    public Object string2Value(String str) {
        switch (fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    public byte[] value2Raw(Object value) {
        byte[] raw = null;
        switch (fieldType) {
            case "int32":
                raw = Parser.int2Byte((int) value);
                break;
            case "int64":
                raw = Parser.long2Byte((long) value);
                break;
            case "string":
                raw = Parser.string2Byte((String) value);
                break;
        }
        return raw;
    }

    public String printValue(Object value) {
        String str = null;
        switch (fieldType) {
            case "int32":
                str = String.valueOf((int) value);
                break;
            case "int64":
                str = String.valueOf((long) value);
                break;
            case "string":
                str = (String) value;
                break;
        }
        return str;
    }

    public ParseValueRes parseValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch (fieldType) {
            case "int32":
                res.value = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.value = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.value = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }


    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch (exp.compareOp) {
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                if (res.right > 0) {
                    res.right--;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                res.right = Long.MAX_VALUE;
                break;
        }
        return res;
    }
}
