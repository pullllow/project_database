package com.example.backend.tbm.booter;

import com.example.backend.utils.Panic;
import com.example.common.Error;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * @author Chang Qi
 * @date 2022/6/5 20:57
 * @description
 * @Version V1.0
 */

public class Booter {

    public static final String BOOTER_SUFFIX = ".bt";
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    public Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }


    public static Booter create(String path) {
        removeBadTmp(path);
        File file = new File(path + BOOTER_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotReadOrWriteException);
        }
        return new Booter(path, file);
    }

    public static Booter open(String path) {
        removeBadTmp(path);
        File file = new File(path + BOOTER_SUFFIX);
        if (!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotReadOrWriteException);
        }
        return new Booter(path, file);
    }


    private static void removeBadTmp(String path) {
        new File(path + BOOTER_TMP_SUFFIX).delete();
    }

    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }


    public void update(byte[] data) {
        File tmp = new File(path+BOOTER_TMP_SUFFIX);

        try {
            tmp.createNewFile();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotReadOrWriteException);
        }

        try(FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            Files.move(tmp.toPath(), new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Panic.panic(e);
        }
        file = new File(path+BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotReadOrWriteException);
        }

    }
}
