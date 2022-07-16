package com.example.db.backend.server;

import com.example.db.backend.parser.Parser;
import com.example.db.backend.parser.statement.*;
import com.example.db.backend.tbm.TableManager;
import com.example.db.backend.tbm.utils.BeginRes;
import com.example.db.common.Error;

/**
 * @author Chang Qi
 * @date 2022/7/16 15:40
 * @description
 * @Version V1.0
 */

public class Executor {
    private long xid;
    TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        xid = 0;
    }

    public void close() {
        if (xid != 0) {
            System.out.println("Abnormal Abort" + xid);
            tbm.abort(xid);
        }
    }

    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        Object stat = Parser.parse(sql);
        if (Begin.class.isInstance(stat)) {
            if (xid != 0) {
                throw Error.NestedTransactionException;
            }
            BeginRes res = tbm.begin((Begin) stat);
            xid = res.xid;
            return res.res;
        } else if (Commit.class.isInstance(stat)) {
            if (xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if (Abort.class.isInstance(stat)) {
            if (xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            boolean tmpTransaction = false;
            Exception e = null;
            if (xid == 0) {
                tmpTransaction = true;
                BeginRes res = tbm.begin(new Begin());
                xid = res.xid;
            }
            try {
                byte[] res = null;
                if (Show.class.isInstance(stat)) {
                    res = tbm.show(xid);
                } else if (Create.class.isInstance(stat)) {
                    res = tbm.create(xid, (Create) stat);
                } else if (Select.class.isInstance(stat)) {
                    res = tbm.read(xid, (Select) stat);
                } else if (Insert.class.isInstance(stat)) {
                    res = tbm.insert(xid, (Insert) stat);
                } else if (Delete.class.isInstance(stat)) {
                    res = tbm.delete(xid, (Delete) stat);
                } else if (Update.class.isInstance(stat)) {
                    res = tbm.update(xid, (Update) stat);
                }
                return res;
            } catch (Exception ex) {
                e = ex;
                throw e;
            } finally {
                if(tmpTransaction) {
                    if(e!=null) {
                        tbm.abort(xid);
                    } else {
                        tbm.commit(xid);
                    }
                    xid = 0;
                }
            }
        }
    }
}
