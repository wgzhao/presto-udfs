package com.wgzhao.presto.udfs.utils;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class CloseDateUtil
{
    public static final String closePath = "/closedate.dat";

    private static final io.airlift.log.Logger LOG = io.airlift.log.Logger.get(CloseDateUtil.class);

    private static final List<String> closeDateList = new ArrayList<>();

    static {
        InputStream in = null;
        try {
            in = CloseDateUtil.class.getResourceAsStream(closePath);
            closeDateList.addAll(IOUtils.readLines(in, StandardCharsets.UTF_8));
        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("CloseDateUtil init failed", e);
        }
        finally {
            if (in != null) {
                IOUtils.closeQuietly(in, null);
            }
        }
    }

    public static boolean isCloseDate(final String date)
    {
        return closeDateList.contains(date);
    }

    public static String getNextExchangeDay(final String date)
    {
        String nextDate = nextDay(date);
        while (!isCloseDate(nextDate)) {
            nextDate = nextDay(nextDate);
            if (nextDate.compareTo(getMaxExchangeDay()) >0) {
                return null;
            }
        }
        return nextDate;
    }

    public static String getLastExchangeDay(final String date)
    {
        // 如果传递的日期恰好是交易日列表的第一天，则需要直接返回null，否则陷入死循环
        if (date.equals(closeDateList.get(0))) {
            return null;
        }
        String lastDate = lastDay(date);
        if (lastDate == null) {
            return null;
        }
        while (!isCloseDate(lastDate)) {
            if (lastDate.compareTo(closeDateList.get(0)) < 0) {
                return null;
            }
            lastDate = lastDay(lastDate);
            if (lastDate == null) {
                return null;
            }
        }
        return lastDate;
    }

    public static String getPeriodExchangeDay(final String date, final int day)
    {
        String lastDate = date;
        while (!closeDateList.contains(lastDate)) {
            lastDate = lastDay(lastDate);
            if (lastDate == null) {
                return null;
            }
            if (lastDate.compareTo(getMinExchangeDay()) < 0 || lastDate.compareTo(getMaxExchangeDay()) > 0) {
                return null;
            }
        }
        final int i = closeDateList.indexOf(lastDate);

        return closeDateList.get(i + day);
    }

    public static String getFirstPeriodExchangeDay(final String date, final int day)
    {
        String lastDate = addDate(date, day);
        if (lastDate == null) {
            return null;
        }
        while (!closeDateList.contains(lastDate)) {
            if (day < 0) {
                if (closeDateList.isEmpty() || closeDateList.get(closeDateList.size() - 1).compareTo(lastDate) < 0) {
                    return null;
                }
                lastDate = nextDay(lastDate);
            }
            else {
                if (closeDateList.isEmpty() || closeDateList.get(0).compareTo(lastDate) > 0) {
                    return null;
                }
                lastDate = lastDay(lastDate);
            }
        }
        return lastDate;
    }


    public static String addDate(final String date, final int days)
    {
        final Calendar c = Calendar.getInstance();
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        try {
            c.setTime(sdf.parse(date));
        }
        catch (final ParseException e) {
            e.printStackTrace();
            return null;
        }
        c.add(Calendar.DAY_OF_MONTH, days);
        return sdf.format(c.getTime());
    }

    public static String nextDay(final String date)
    {
        return addDate(date, 1);
    }

    public static String lastDay(final String date)
    {
        return addDate(date, -1);
    }

    public static String getPrevCloseDay(final String date, final String type)
    {
        String newDate;
        final Calendar c = Calendar.getInstance();
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        try {
            c.setTime(sdf.parse(date));
        }
        catch (final ParseException e) {
            e.printStackTrace();
            return null;
        }

        switch (type) {
            case "month":
                c.add(Calendar.MONTH, -1);
                break;
            case "quoter":
                c.add(Calendar.MONTH, -3);
                break;
            case "halfyear":
                c.add(Calendar.MONTH, -6);
                break;
            case "year":
                c.add(Calendar.YEAR, -1);
                break;
            default:
                c.add(Calendar.DATE, -1);
        }
        newDate = sdf.format(c.getTime());
        return CloseDateUtil.getLastExchangeDay(newDate);
    }

    public static int getCountExchangeDay(final String date1, final String date2)
    {
        if (date1 == null || date2 == null) {
            return 0;
        }
        String lastDate = date1;
        int count = 0;
        while (lastDate.compareTo(date2) <= 0) {
            if (closeDateList.contains(lastDate)) {
                count++;
            }
            lastDate = nextDay(lastDate);
            if (lastDate == null) {
                return 0;
            }
        }

        return count;
    }

    /**
     * 获取当前交易日文件的最大日期
     *
     * @return the max trade date
     */
    public static String getMaxExchangeDay()
    {
        return closeDateList.get(closeDateList.size() - 1);
    }

    /**
     * 获取当前交易文件的最小交易日
     *
     * @return the min trade date
     */
    public static String getMinExchangeDay()
    {
        return closeDateList.get(0);
    }
}
