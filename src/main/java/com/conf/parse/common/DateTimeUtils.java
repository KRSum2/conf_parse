package com.conf.parse.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTimeUtils {

    public static final String DATE_TIME_FORMAT = "yyyyMMddHH";
    public static final String DATE_FORMAT = "yyyyMMdd";

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    public static final SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT);

    public static String formatDateTime(Date date) {
        synchronized (dateTimeFormat) {
            return dateTimeFormat.format(date);
        }
    }

    public static String formatDateTime(long time) {
        return formatDateTime(new Date(time));
    }

    public static Date parseDateTime(String dateTime) throws ParseException {
        synchronized (dateTimeFormat) {
            return dateTimeFormat.parse(dateTime);
        }
    }

    public static long parseDateTimeAsLong(String dateTime) throws ParseException {
        synchronized (dateTimeFormat) {
            return dateTimeFormat.parse(dateTime).getTime();
        }
    }

    public static Date parseDate(String dateStr) throws ParseException {
        synchronized (dateFormat) {
            return dateFormat.parse(dateStr);
        }
    }

    public static Date parseDate(String dateStr, SimpleDateFormat dateFormat) throws ParseException {
        synchronized (dateFormat) {
            return dateFormat.parse(dateStr);
        }
    }

    public static String formatDate(Date date) {
        synchronized (dateFormat) {
            return dateFormat.format(date);
        }
    }

    public static String formatDate(long time) {
        return formatDate(new Date(time));
    }

    public static String formatDate(Date date, SimpleDateFormat dateFormat) {
        synchronized (dateFormat) {
            return dateFormat.format(date);
        }
    }

}
