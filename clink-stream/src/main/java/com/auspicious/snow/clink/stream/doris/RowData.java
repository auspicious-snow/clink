package com.auspicious.snow.clink.stream.doris;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/4/12 17:11
 */

public class RowData {
    public int id;
    public Double amount;
    public String name;
    public RowData(int id,Double amount, String name) {
        this.id = id;
        this.name = name;
        this.amount = amount;
    }
}
