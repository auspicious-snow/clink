package com.auspicious.snow.client;

import java.lang.reflect.Method;
import org.apache.flink.client.cli.CliFrontend;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/2/20 14:54
 */
public class ClinkClient {

    public static void main(String[] args) {
        try {
            //通过反射，获取flink得CliFrontend 客户端
            Method method = CliFrontend.class.getMethod("main", String[].class);
            //通过反射将任务提交到远程节点
            method.invoke(null,(Object) args);
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

}
