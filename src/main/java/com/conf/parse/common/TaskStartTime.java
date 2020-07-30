package com.conf.parse.common;

import java.util.Date;

public class TaskStartTime {

    /**
     * 日期小时字符串，格式: yyyyMMddHH，例如: 2017111605
     */
    public final String dateHourStr;
    public final Date dateHour;

    /**
     * 日期字符串，格式: yyyyMMdd，例如: 20171116
     */
    public final String dateStr;
    public final Date date;

    public final int hour;

    /**
     * 
     * @param dateHourStr 日期小时字符串，格式: yyyyMMddHH
     * @throws Exception
     */
    public TaskStartTime(String dateHourStr) throws Exception {
        if (dateHourStr == null) {
            throwException(dateHourStr);
        }
        dateHourStr = dateHourStr.trim();
        if (dateHourStr.isEmpty() || dateHourStr.length() != 10 || !MiscUtils.isNumber(dateHourStr)) {
            throwException(dateHourStr);
        }
        this.dateHourStr = dateHourStr;
        this.dateHour = DateTimeUtils.parseDateTime(dateHourStr);
        dateStr = dateHourStr.substring(0, 8);
        // 不能拿dateHourStr来解析，用yyyyMMdd来解析 yyyyMMddHH格式的字符串会得到错误的Date对象
        date = DateTimeUtils.parseDate(dateStr);
        hour = Integer.parseInt(dateHourStr.substring(8, 10));
    }

    private static void throwException(String dateHourStr) {
        throw new RuntimeException("Invalid dateHourStr: " + dateHourStr + ", format: yyyyMMddHH");
    }

    @Override
    public String toString() {
        return dateHourStr;
    }
}
