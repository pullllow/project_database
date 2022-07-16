package com.example.backend.parser;

import com.example.common.Error;

import static java.lang.Character.isDigit;

/**
 * @author Chang Qi
 * @date 2022/6/6 9:40
 * @description
 * @Version V1.0
 */

public class Tokenizer {

    private byte[] stmt;
    private int pos;
    private String currentToken;
    private boolean flushToken;
    private Exception error;

    public Tokenizer(byte[] stmt) {
        this.stmt = stmt;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    public String peek() throws Exception {
        if (error != null) {
            throw error;
        }
        if (flushToken) {
            String token = null;
            try {
                token = next();
            } catch (Exception e) {
                error = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }


    /**
     * pop可以优化
     **/
    public String pop() throws Exception {
        flushToken = true;
        return peek();
    }

    public byte[] errorStmt() {
        byte[] res = new byte[stmt.length + 3];
        System.arraycopy(stmt, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stmt, pos, res, pos + 3, stmt.length - pos);
        return res;
    }

    private void popByte() {
        pos++;
        if (pos > stmt.length) {
            pos = stmt.length;
        }
    }

    private Byte peekByte() {
        if (pos == stmt.length) {
            return null;
        }
        return stmt[pos];
    }

    private String next() throws Exception {
        if (error != null) {
            throw error;
        }
        return nextMetaState();
    }

    private String nextMetaState() throws Exception {
        while (true) {
            Byte b = peekByte();
            if (b == null) {
                return "";
            }
            if (!isBlank(b)) {
                break;
            }
            popByte();
        }
        byte b = peekByte();
        if (isSymbol(b)) {
            popByte();
            return new String(new byte[]{b});
        } else if (b == '"' || b == '\'') {
            return nextQuoteState();
        } else if (isAlphaBeta(b) || isDigit(b)) {
            return nextTokenState();
        } else {
            error = Error.InvalidCommandException;
            throw error;
        }
    }

    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while (true) {
            Byte b = peekByte();
            if (b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if (b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }


    }

    private String nextQuoteState() throws Exception {
        byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b==null) {
                error = Error.InvalidCommandException;
                throw error;
            }
            if(b == quote) {
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' || b == ',' || b == '(' || b == ')');
    }

    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }

}
