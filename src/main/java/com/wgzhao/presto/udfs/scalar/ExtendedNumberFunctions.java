/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wgzhao.presto.udfs.scalar;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Map.entry;

/**
 * 阿拉伯数字和中文数字互转
 */
public class ExtendedNumberFunctions
{
    //存放数量级中文数字信息 {十，百。。。亿。。。}
    private static final Map<String, Long> magnitudeMap = Map.ofEntries(
            entry("十", 10L),
            entry("拾", 10L),
            entry("百", 100L),
            entry("佰", 100L),
            entry("千", 1000L),
            entry("仟", 1000L),
            entry("万", 10000L),
            entry("亿", 100000000L),
            entry("兆", 1000000000000L),
            entry("京", 10000000000000000L)
    );
    //存放0~9基本中文数字信息, {一，二。。。九，零}
    private static final Map<String, Long> dataMap = Map.ofEntries(
            entry("一", 1L),
            entry("二", 2L),
            entry("三", 3L),
            entry("四", 4L),
            entry("五", 5L),
            entry("六", 6L),
            entry("七", 7L),
            entry("八", 8L),
            entry("九", 9L),
            entry("零", 0L),
            entry("壹", 1L),
            entry("贰", 2L),
            entry("叁", 3L),
            entry("肆", 4L),
            entry("伍", 5L),
            entry("陆", 6L),
            entry("柒", 7L),
            entry("捌", 8L),
            entry("玖", 9L));

    private ExtendedNumberFunctions() {}

    /**
     * 从后往前遍历字符串的方式将中文数字转换为阿拉伯数字
     *
     * @param chStr 源字符串
     * @return 转换后的阿拉伯数字
     */
    @Description("convert chinese number to Arabia number")
    @ScalarFunction("udf_ch2num")
    @SqlType(StandardTypes.BIGINT)
    public @SqlNullable
    static Long convertChineseNumberToArabiaNumber(@SqlType(StandardTypes.VARCHAR) Slice chStr)
    {
        if (chStr == null || "".equals(chStr.toStringUtf8())) {
            return null;
        }
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
                    return sumVal;
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
                if (data != 0) {
                    sumVal = sumVal + data * currentMaxLevel;
                }
            }
            else {
                // illegal value
                return null;
            }
        }
        return sumVal;
    }

    @Description("convert Chinese number to Arabia number")
    @ScalarFunction("udf_num2ch")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice convertArabiaNumberToChineseNumber(@SqlType(StandardTypes.VARCHAR) Slice value)
    {
        if (value == null || "".equals(value.toStringUtf8())) {
            return null;
        }
        long inputValue = Long.parseLong(value.toStringUtf8());
        return numberToChinese(inputValue, false);
    }

    @Description("convert Chinese number to Arabia number")
    @ScalarFunction("udf_num2ch")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice convertArabiaNumberToChineseNumber(@SqlType(StandardTypes.BIGINT) long value)
    {
        return numberToChinese(value, false);
    }

    @Description("convert Chinese number to Arabia number")
    @ScalarFunction("udf_num2ch")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice convertArabiaNumberToChineseNumber(@SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.BOOLEAN) boolean flag)
    {
        return numberToChinese(value, flag);
    }

    @Description("convert Chinese number to Arabia number")
    @ScalarFunction("udf_num2ch")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice convertArabiaNumberToChineseNumber(@SqlType(StandardTypes.VARCHAR) Slice value,
            @SqlType(StandardTypes.BOOLEAN) boolean flag)
    {
        if (value == null || "".equals(value.toStringUtf8())) {
            return null;
        }

        long inputValue = Long.parseLong(value.toStringUtf8());
        return numberToChinese(inputValue, flag);
    }

    private static Slice numberToChinese(long value, boolean flag)
    {
        int pos = flag ? 1 : 0;
        final String[][] units = {
                {"", "拾", "佰", "仟", "万", "拾", "佰", "仟", "亿", "拾", "佰", "仟",},
                {"", "十", "百", "千", "万", "十", "百", "千", "亿", "十", "百", "千",}};
        final String[][] nums = {
                {"零", "壹", "贰", "叁", "肆", "伍", "陆", "柒", "捌", "玖",},
                {"零", "一", "二", "三", "四", "五", "六", "七", "八", "九",}};
        StringBuilder result = new StringBuilder(); //转译结果

        for (int i = String.valueOf(value).length() - 1; i >= 0; i--) {
            //value / Math.pow(10, i) 截位匹配单位
            int r = (int) (value / Math.pow(10, i));
            result.append(nums[pos][r % 10]).append(units[pos][i]);
        }

        //匹配字符串中的 "零[十, 百, 千]" 替换为 "零"
        result = new StringBuilder(result.toString().replaceAll("零[拾, 佰, 仟]", "零")); //匹配字符串中的 "零[十, 百, 千]" 替换为 "零"
        result = new StringBuilder(result.toString().replaceAll("零[十, 百, 千]", "零")); //匹配字符串中的 "零[十, 百, 千]" 替换为 "零"
        result = new StringBuilder(result.toString().replaceAll("零+", "零")); //匹配字符串中的1或多个 "零" 替换为 "零"
        result = new StringBuilder(result.toString().replaceAll("零([万, 亿])", "$1"));
        result = new StringBuilder(result.toString().replaceAll("亿万", "亿")); //亿万位拼接时发生的特殊情况

        if (result.toString().startsWith("壹拾") || result.toString().startsWith("一十")) {
            //判断是否以 "一十" 开头 如果是截取第一个字符
            result = new StringBuilder(result.substring(1));
        }

        if (result.toString().endsWith("零")) {
            //判断是否以 "零" 结尾 如果是截取除 "零" 外的字符
            result = new StringBuilder(result.substring(0, result.length() - 1));
        }
        return utf8Slice(result.toString());
    }
}
