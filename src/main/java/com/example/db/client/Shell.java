package com.example.db.client;

import java.util.Scanner;

/**
 * @author Chang Qi
 * @date 2022/7/16 15:00
 * @description
 * @Version V1.0
 */

public class Shell {
    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    public void run() {
        Scanner sc = new Scanner(System.in);
        try {
            while(true) {
                System.out.println(":>");
                String statStr = sc.nextLine();
                if("exist".equals(statStr) || "quit".equals(statStr)) {
                    break;
                }
                try {
                    byte[] res = client.execute(statStr.getBytes());
                    System.out.println(new String(res));
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        } finally {
            sc.close();
            client.close();
        }
    }
}
