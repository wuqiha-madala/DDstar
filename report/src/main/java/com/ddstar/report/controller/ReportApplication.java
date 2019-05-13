package com.ddstar.report.controller;

import com.alibaba.fastjson.JSON;
import com.ddstar.report.msg.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Date;

/**
 * Created by Liutao on 2019/5/7 11:20
 */
@SpringBootApplication
@Controller
@RequestMapping("ReportApplication")
public class ReportApplication {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping(value = "receiveData", method = RequestMethod.POST)
    public void receiveData(@RequestBody String json, HttpServletRequest request, HttpServletResponse response) {
        Message msg = new Message();
        msg.setCount(1);
        msg.setTimeStamp(new Date().getTime());
        msg.setMessage(json);

        String jsonstring = JSON.toJSONString(msg);
        System.out.println(jsonstring);
        //业务开始
        kafkaTemplate.send("test1","key", jsonstring);
        //业务结束
        PrintWriter printWriter = getWrite(response);
        response.setStatus(HttpStatus.OK.value());
        printWriter.write("success");
        close(printWriter);

    }

    private PrintWriter getWrite(HttpServletResponse response) {
        response.setCharacterEncoding("utf-8");
        response.setContentType("application/json");
        //回显结果：IO流的操作
        OutputStream out = null;
        PrintWriter printWriter = null;
        try {
            out = response.getOutputStream();
            printWriter = new PrintWriter(out);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return printWriter;
    }

    //关闭IO流
    public void close(PrintWriter printWriter) {
        printWriter.flush();
        printWriter.close();
    }


}
