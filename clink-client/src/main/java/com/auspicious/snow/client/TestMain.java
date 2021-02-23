package com.auspicious.snow.client;

import java.lang.reflect.Method;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/2/20 14:59
 */
public class TestMain {

    public static void main(String[] args) {
        String[] a = {"abc"};
        try {
            Method main = ClinkClient.class.getMethod("main", String[].class);
            main.invoke(null,(Object)a);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
