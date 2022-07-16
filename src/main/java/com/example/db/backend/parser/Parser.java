package com.example.db.backend.parser;

import com.example.db.backend.parser.statement.*;
import com.example.db.common.Error;


import java.util.ArrayList;
import java.util.List;

/**
 * @author Chang Qi
 * @date 2022/6/6 9:39
 * @description
 *  改进为自动机
 * @Version V1.0
 */

public class Parser {

    public static Object parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();

        Object stmt = null;
        Exception stmtError = null;

        try {
            switch (token) {
                case "begin":
                    stmt = parseBegin(tokenizer);
                    break;
                case "commit":
                    stmt = parseCommit(tokenizer);
                    break;
                case "abort":
                    stmt = parseAbort(tokenizer);
                    break;
                case "create":
                    stmt = parseCreate(tokenizer);
                    break;
                case "drop":
                    stmt = parseDrop(tokenizer);
                    break;
                case "insert":
                    stmt = parseInsert(tokenizer);
                    break;
                case "select":
                    stmt = parseSelect(tokenizer);
                    break;
                case "delete":
                    stmt = parseDelete(tokenizer);
                    break;
                case "update":
                    stmt = parseUpdate(tokenizer);
                    break;
                case "show":
                    stmt = parseShow(tokenizer);
                    break;
                default:
                    throw Error.InvalidCommandException;
            }
        } catch (Exception e) {
            stmtError = e;
        }

        try {
            String next = tokenizer.peek();
            if (!"".equals(next)) {
                byte[] errorStmt = tokenizer.errorStmt();
                stmtError = new RuntimeException("Invalid statement:" + new String(errorStmt));
            }
        } catch (Exception e) {
            e.printStackTrace();
            byte[] errorStmt = tokenizer.errorStmt();
            stmtError = new RuntimeException("Invalid statement:" + new String(errorStmt));
        }
        if (stmtError != null) {
            throw stmtError;
        }
        return stmt;

    }

    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            return new Show();
        }
        throw Error.InvalidCommandException;
    }

    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        update.tableName = tokenizer.peek();
        tokenizer.pop();

        if (!"set".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        update.fieldName = tokenizer.peek();
        tokenizer.pop();

        update.value = tokenizer.peek();
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            update.where = null;
            return update;
        }

        update.where = parseWhere(tokenizer);
        return update;
    }

    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();

        if (!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        delete.tableName = tableName;
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);
        return delete;
    }

    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();

        if (!"into".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        tokenizer.pop();

        if (!"values".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        List<String> values = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if ("".equals(value)) {
                break;
            } else {
                values.add(value);
            }
        }

        insert.values = values.toArray(new String[values.size()]);

        return insert;


    }

    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();

        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        if ("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while (true) {
                String field = tokenizer.peek();
                if (!isName(field)) {
                    throw Error.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if (",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }

        read.fields = fields.toArray(new String[fields.size()]);

        if (!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        read.tableName = tableName;
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);
        return read;
    }

    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if (!"where".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        SingleExpression exp1 = parseSingleExpression(tokenizer);
        where.singleExp1 = exp1;

        String logicOp = tokenizer.peek();
        if ("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        if (!isLogicOption(logicOp)) {
            throw Error.InvalidCommandException;
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExpression(tokenizer);
        where.singleExp2 = exp2;
        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return where;

    }

    private static SingleExpression parseSingleExpression(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();

        String field = tokenizer.peek();
        if (!isName(field)) {
            throw Error.InvalidCommandException;
        }
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if (!isCompareOption(op)) {
            throw Error.InvalidCommandException;
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;

    }

    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        if (!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        if (!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        Create create = new Create();
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        create.tableName = tableName;

        List<String> fieldNames = new ArrayList<>();
        List<String> fieldTypes = new ArrayList<>();

        while (true) {
            tokenizer.pop();
            String fieldName = tokenizer.peek();
            if ("(".equals(fieldName)) {
                break;
            }
            if (!isName(fieldName)) {
                throw Error.InvalidCommandException;
            }

            tokenizer.pop();
            String filedType = tokenizer.peek();
            if (!isType(filedType)) {
                throw Error.InvalidCommandException;
            }
            fieldNames.add(fieldName);
            fieldTypes.add(filedType);
            tokenizer.pop();

            String next = tokenizer.peek();
            if (",".equals(next)) {
                continue;
            } else if ("".equals(next)) {
                throw Error.InvalidCommandException;
            } else if ("(".equals(next)) {
                break;
            } else {
                throw Error.InvalidCommandException;
            }
        }

        create.fieldName = fieldNames.toArray(new String[fieldNames.size()]);
        create.fieldType = fieldTypes.toArray(new String[fieldTypes.size()]);

        tokenizer.pop();
        if (!"index".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        List<String> indexes = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if (")".equals(field)) {
                break;
            }
            if (!isName(field)) {
                throw Error.InvalidCommandException;
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[indexes.size()]);
        tokenizer.pop();

        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return create;
    }

    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return new Abort();
    }


    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return new Commit();
    }

    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek(); //隔离级别
        Begin begin = new Begin();
        if ("".equals(isolation)) {
            return begin;
        }

        if (!"isolation".equals(isolation)) {
            throw Error.InvalidCommandException;
        }

        tokenizer.pop();

        String tmp1 = tokenizer.peek();
        if ("read".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("committed".equals(tmp2)) {
                tokenizer.pop();
                if (!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else if ("repeatable".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("read".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if (!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else {
            throw Error.InvalidCommandException;
        }

    }

    private static boolean isName(String name) {
        return !(name.length() == 1 && Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }

    private static boolean isType(String type) {
        return ("int32".equals(type) || "int64".equals(type) || "string".equals(type));
    }

    private static boolean isCompareOption(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    private static boolean isLogicOption(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

}
