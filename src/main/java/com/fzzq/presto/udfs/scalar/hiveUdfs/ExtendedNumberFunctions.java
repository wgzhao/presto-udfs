/*
 * Copyright 2013-2016 fzzq
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fzzq.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import static io.airlift.slice.Slices.utf8Slice;
import java.util.HashMap;
import java.util.Map;

/**
 * 这是一个中文数字和阿拉伯数字转换算法的测试
 * @author Haibo Yu on 09/27/2017.
 */
public class ExtendedNumberFunctions
{
    private ExtendedNumberFunctions() {}
    //存放数量级中文数字信息 {十，百。。。亿。。。}
    private static Map<String, Long> magnitudeMap = getMagnitudeMap();
    //存放0~9基本中文数字信息, {一，二。。。九，零}
    private static Map<String, Long> dataMap = getDataMap();

    /**
     * 从后往前遍历字符串的方式将中文数字转换为阿拉伯数字
     * @param inputStr 源字符串
     * @return 转换后的阿拉伯数字
     * @throws Exception 如果字符串中有不能识别的（不在dataMap和operatorMap）字符，抛出异常
     */
    @Description("convert chinese number to aribia number")
    @ScalarFunction("ch2num")
    @SqlType(StandardTypes.BIGINT)
    public static long convertToLongFromEnd(@SqlType(StandardTypes.VARCHAR) Slice chStr)
    {
        String inputStr = chStr.toStringUtf8();
        //存储遇到该数字前的最大一个数量值，这个值是累乘之前所有数量级，
        //比如二百万，到二的时候最高数量级就是100*10000
        long currentMaxLevel = 1L;
        //存储之前一次执行过乘操作的数量级
        long previousOpeMagnitude = 1L;
        //存储当前字符所对应的数量级
        long currentMagnitude = 1L;
        //存储当前所有字符仲最大的单个字符的数量级，
        long maxMagnitude = 1L;

        long sumVal = 0L;

        int len = inputStr.length();
        //倒序循环整个字符串，从最低位开始计算整个数值
        for (int i = len - 1; i >= 0; i--) {
            String currentTxt = String.valueOf(inputStr.charAt(i));

            //如果当前值是数量级
            if (magnitudeMap.containsKey(currentTxt)) {
                currentMagnitude = magnitudeMap.get(currentTxt);
                //如果第一位是一个数量级（比如十二）, 将当前值相加
                if (i == 0) {
                    sumVal = sumVal + currentMagnitude;
                    return  sumVal;
                }
                //比较当前数量级与当前最大数量值，如果大于当前最大值，将当然最大数量值更新为当前数量级
                if (currentMagnitude > currentMaxLevel) {
                    currentMaxLevel = currentMagnitude;
                }
                else {
                    if (currentMagnitude < maxMagnitude && currentMagnitude > previousOpeMagnitude) {
                        //如果当前数量级小于当前最大数量级并且大于之前的数量级,比如二十五万五百亿，抵达"万"的时候因为之前的百
                        //已经与亿相乘，所以应该除以之前的百才能得到当前真正的最大数量值
                        currentMaxLevel = currentMaxLevel * currentMagnitude / previousOpeMagnitude;
                    }
                    else {
                        currentMaxLevel = currentMaxLevel * currentMagnitude;
                    }
                    previousOpeMagnitude = currentMagnitude;
                }
                //将当前最大单数量级更新为当前数量级
                if (currentMagnitude > maxMagnitude) {
                    maxMagnitude = currentMagnitude;
                }
            }
            else if (dataMap.containsKey(currentTxt)) {
                //如果是0~9之间的数字，与前面一位数量级相乘，并累加到当前sumVal
                long data = dataMap.get(currentTxt);
                if (data == 0) {
                    //跳过0
                    continue;
                }
                else {
                    sumVal = sumVal + data * currentMaxLevel;
                }
            }
        }
        return sumVal;
    }

    @Description("convert Chinese number to Arabia number")
    @ScalarFunction("num2ch")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice  convertArabiaNumerToChineseNumber(@SqlType(StandardTypes.VARCHAR) Slice value)
    {
        long inputValue = Long.valueOf(value.toStringUtf8());
        return convertArabiaNumerToChineseNumber(inputValue);
    }

    @Description("convert Chinese number to Arabia number")
    @ScalarFunction("num2ch")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice  convertArabiaNumerToChineseNumber(@SqlType(StandardTypes.BIGINT) long value)
    {
        // private static final String[] units = { "", "十", "百", "千", "万", "十", "百", "千", "亿", "十", "百", "千", };
        final String[] units = { "", "拾", "佰", "仟", "万", "拾", "佰", "仟", "亿", "拾", "佰", "仟", };
        // final String[] nums = { "零", "一", "二", "三", "四", "五", "六", "七", "八", "九", };
        final String[] nums = { "零", "壹", "贰", "叁", "肆", "伍", "陆", "柒", "捌", "玖", };
        String result = ""; //转译结果

        for (int i = String.valueOf(value).length() - 1; i >= 0; i--) {
            //String.valueOf(value) 转换成String型得到其长度 并排除个位,因为个位不需要单位
            //value / Math.pow(10, i) 截位匹配单位
            int r = (int) (value / Math.pow(10, i));
            result += nums[r % 10] + units[i];
        }

        // result = result.replaceAll("零[十, 百, 千]", "零");
        //匹配字符串中的 "零[十, 百, 千]" 替换为 "零"
        result = result.replaceAll("零[拾, 佰, 仟]", "零"); //匹配字符串中的 "零[十, 百, 千]" 替换为 "零"
        result = result.replaceAll("零+", "零"); //匹配字符串中的1或多个 "零" 替换为 "零"
        result = result.replaceAll("零([万, 亿])", "$1");
        result = result.replaceAll("亿万", "亿"); //亿万位拼接时发生的特殊情况

        // if (result.startsWith("一十")) { //判断是否以 "一十" 开头 如果是截取第一个字符
        if (result.startsWith("壹拾")) { //判断是否以 "一十" 开头 如果是截取第一个字符
            result = result.substring(1);
        }

        if (result.endsWith("零")) { //判断是否以 "零" 结尾 如果是截取除 "零" 外的字符
            result = result.substring(0, result.length() - 1);
        }
        return utf8Slice(result);
    }
    /**
     * 数量级map，存储对应的数量级文字和对应的阿拉伯数字量值
     * @return The magnitude map
     */
    private static Map<String, Long> getMagnitudeMap()
    {
        Map<String, Long> magnitudeMap = new HashMap();
        magnitudeMap.put("十", 10L);
        magnitudeMap.put("拾", 10L);
        magnitudeMap.put("百", 100L);
        magnitudeMap.put("佰", 100L);
        magnitudeMap.put("千", 1000L);
        magnitudeMap.put("仟", 1000L);
        magnitudeMap.put("万", 10000L);
        magnitudeMap.put("亿", 100000000L);
        magnitudeMap.put("兆", 1000000000000L);
        magnitudeMap.put("京", 10000000000000000L);
        return magnitudeMap;
    }

    /**
     * 基本数据map，存储对应的基本数据及对应的阿拉伯数字量值
     * @return
     */
    private static Map<String, Long> getDataMap()
    {
        Map<String, Long> dataMap = new HashMap<>();
        dataMap.put("一", 1L);
        dataMap.put("二", 2L);
        dataMap.put("三", 3L);
        dataMap.put("四", 4L);
        dataMap.put("五", 5L);
        dataMap.put("六", 6L);
        dataMap.put("七", 7L);
        dataMap.put("八", 8L);
        dataMap.put("九", 9L);
        dataMap.put("零", 0L);
        dataMap.put("壹", 1L);
        dataMap.put("贰", 2L);
        dataMap.put("叁", 3L);
        dataMap.put("肆", 4L);
        dataMap.put("伍", 5L);
        dataMap.put("陆", 6L);
        dataMap.put("柒", 7L);
        dataMap.put("捌", 8L);
        dataMap.put("玖", 9L);
        return dataMap;
    }
}
