package com.ddstar.report.msg;

import org.jcp.xml.dsig.internal.SignerOutputStream;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by Liutao on 2019/5/7 14:25
 */
public class SendMessage {
    public static void send(String address, String message) {
        try {
            URL url = new URL(address);
            HttpURLConnection conn =
                    (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setAllowUserInteraction(true);
            conn.setUseCaches(false);
            conn.setReadTimeout(10 * 1000);
            conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.connect();
            OutputStream outputStream = conn.getOutputStream();
            BufferedOutputStream out = new
                    BufferedOutputStream(outputStream);
            out.write(message.getBytes());
            out.flush();

            String temp = "";
            InputStream in = conn.getInputStream();
            byte[] tempbytes = new byte[1024];
            while (in.read(tempbytes, 0, 1024) != -1) {
                temp += new String(tempbytes);
            }
            System.out.println(conn.getResponseCode());
            System.out.println(temp);


        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
