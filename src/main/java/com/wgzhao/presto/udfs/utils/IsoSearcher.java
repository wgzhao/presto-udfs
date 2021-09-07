package com.wgzhao.presto.udfs.utils;

import com.wgzhao.presto.udfs.dto.IsoDto;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2020/3/23.
 */
public class IsoSearcher
{

    public static Map<String, IsoDto> isoMap = new HashMap<>();

    private static volatile IsoSearcher instance;

    public static IsoSearcher getInstance()
    {
        if (instance == null) {
            synchronized (IsoSearcher.class) {
                if (instance == null) {
                    instance = new IsoSearcher();
                }
            }
        }
        return instance;
    }

    private IsoSearcher()
    {

        this.readIsoCsv();
    }

    public void readIsoCsv()
    {
        InputStream inputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader reader = null;

        try {
            inputStream = IsoSearcher.class.getResourceAsStream("/iso3166.csv");

            inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            reader = new BufferedReader(inputStreamReader);//GBK
            reader.readLine();//显示标题行

            String line = null;
            while ((line = reader.readLine()) != null) {
                String item[] = line.split(","); //CSV格式文件时候的分割符

                IsoDto iso = new IsoDto();

                iso.setCn(item[0].replace("\"", ""));
                iso.setEn(item[1].replace("\"", ""));
                iso.setM2(item[2].replace("\"", ""));
                iso.setM3(item[3].replace("\"", ""));
                iso.setDigit(item[4].replace("\"", ""));

                isoMap.put(item[0].replace("\"", ""), iso);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
                if (inputStreamReader != null) {
                    inputStreamReader.close();
                }
                if (reader != null) {
                    reader.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String searcher(String country, String fieldName)
    {

        String value = "";

        if ("".equals(country) || country == null) {
            return value;
        }

        if (isoMap.containsKey(country)) {
            IsoDto isoDto = isoMap.get(country);

            value = (String) this.getFieldValueByName(fieldName, isoDto);
        }

        return value;
    }

    private Object getFieldValueByName(String fieldName, Object o)
    {
        try {
            String firstLetter = fieldName.substring(0, 1).toUpperCase();
            String getter = "get" + firstLetter + fieldName.substring(1);
            Method method = o.getClass().getMethod(getter);
            return method.invoke(o);
        }
        catch (Exception e) {
            return "";
        }
    }
}
