/*
 *
 *  Copyright 2021
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.wgzhao.presto.udfs.scalar;

import com.wgzhao.presto.udfs.utils.CloseDateUtil;
import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import static com.alibaba.fastjson.JSON.toJSONString;
import static io.airlift.slice.Slices.utf8Slice;

/**
 * common functions about stock exchange date
 */
public class TradeDateFunctions
{
    private TradeDateFunctions()
    {
    }

    /**
     * 计算在给定日期后的N天内的第一个交易日
     * <p>
     * 如果N是正数，则往后计算；如果是负数，则往前计算
     * <p>
     * 示例：
     * <pre>{@code
     *  select udf_add_normal_days('20210903', 4); -- 20210907
     *  select udf_add_normal_days('20210906', -1) -- 20210903
     * }</pre>
     *
     * @param baseDate 给定的日期, 格式为 yyyyMMdd
     * @param days 指定的天数，正数表示往后计算，负数表示往前计算
     * @return 交易日，如果没有找到，则返回为 null
     */
    @Description("add days from base date")
    @ScalarFunction("udf_add_normal_days")
    @SqlType(StandardTypes.VARCHAR)
    public static @SqlNullable
    Slice udfAddNormalDays(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice baseDate,
            @SqlNullable @SqlType(StandardTypes.INTEGER) Long days)
    {
        if (baseDate == null || "".equals(baseDate.toStringUtf8())) {
            return null;
        }
        String res;
        res = CloseDateUtil.getFirstPeriodExchangeDay(baseDate.toStringUtf8(), days.intValue());
        if (res == null) {
            return null;
        }
        return utf8Slice(res);
    }

    /**
     * 返回两个给定的第一个日期（包括）和第二个日期（包括）之间有多少个交易日
     * 给定的两个日期有任意一个为 null 或为空，均返回为 null
     *
     * @param d1 第一个日期
     * @param d2 第二个日期
     * @return 交易日数量
     */
    @Description("count the number of trade date between given two dates")
    @ScalarFunction("udf_count_trade_days")
    @SqlType(StandardTypes.INTEGER)
    public static @SqlNullable
    Long udfCountTradeDays(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice d1,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice d2)
    {
        if (d1 == null || d2 == null || "".equals(d1.toStringUtf8()) || "".equals(d2.toStringUtf8())) {
            return null;
        }

        return (long) CloseDateUtil.getCountExchangeDay(d1.toStringUtf8(), d2.toStringUtf8());
    }

    /*
    -- yyyyMMdd格式，加减days个交易日
    */
    @Description("add days from base date with yyyyMMdd format")
    @ScalarFunction("udf_add_trade_days")
    @SqlType(StandardTypes.VARCHAR)
    public static @SqlNullable
    Slice udfAddTradeDays(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice s, @SqlNullable @SqlType(StandardTypes.INTEGER) Long days)
    {
        if (s == null || "".equals(s.toStringUtf8())) {
            return null;
        }
        String res;
        res = CloseDateUtil.getPeriodExchangeDay(s.toStringUtf8(), days.intValue());
        if (res == null) {
            return null;
        }
        return utf8Slice(res);
    }

    /*
    传入以逗号分隔的数据数组，返回这段数据的最大回撤率。
    */
    @Description("get the max drawdown rate")
    @ScalarFunction("udf_max_draw_down")
    @SqlType(StandardTypes.DOUBLE)
    public static @SqlNullable
    Double udfMaxDrawDown(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice s)
            throws IOException
    {
        if (s == null || s.toStringUtf8().trim().isEmpty()) {
            return null;
        }
        String[] arry = s.toStringUtf8().split(",");
        if (arry.length == 0) {
            return 0.0;
        }
        double[] da;
        try {
            da = Stream.of(arry).map(
                    x -> {
                        if (x == null || x.trim().isEmpty()) {
                            return "0.0";
                        }
                        else {
                            return x;
                        }
                    })
                    .mapToDouble(Double::parseDouble).toArray();
        }
        catch (Exception e) {
            throw new IOException("参数中包含非数字", e);
        }
        double mdd = 0.0;
        double peak = da[0];
        double dd;
        for (double v : da) {
            if (v > peak) {
                peak = v;
            }
            dd = (peak - v) / peak;
            if (dd > mdd) {
                mdd = dd;
            }
        }
        return mdd;
    }

    /*
        -- 获取日期的上一个交易日
    */
    @Description("get the last exchange day before specified date")
    @ScalarFunction("udf_last_trade_date")
    @SqlType(StandardTypes.VARCHAR)
    public static @SqlNullable
    Slice udfLastTradeDate(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice s)
    {
        if (s == null || "".equals(s.toStringUtf8())) {
            return null;
        }

        String res = CloseDateUtil.getLastExchangeDay(s.toStringUtf8());
        if (res == null) {
            return null;
        }
        return utf8Slice(res);
    }

    @Description("is close day or not")
    @ScalarFunction("udf_is_trade_date")
    @SqlType(StandardTypes.BOOLEAN)
    public static @SqlNullable
    Boolean udfIsTradeDate(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice s)
    {
        if (s == null || "".equals(s.toStringUtf8())) {
            return false;
        }
        return CloseDateUtil.isCloseDate(s.toStringUtf8());
    }

    @Description("convert map to json")
    @ScalarFunction("map_to_json")
    @SqlType(StandardTypes.VARCHAR)
    public static @SqlNullable
    Slice udfMapToJson(@SqlNullable @SqlType(StandardTypes.VARCHAR) Map<String, String> smap)
    {
        String res = toJSONString(smap);
        return utf8Slice(res);
    }
}