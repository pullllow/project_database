package com.example.backend.utils;

/**
 * @author Chang Qi
 * @date 2022/5/25 19:40
 * @description
 * @Version V1.0
 */

public class Panic {
    public static void panic(Exception error) {
        error.printStackTrace();
        System.exit(1);
    }
}
