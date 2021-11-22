/*
 * Copyright 2020
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

package com.wgzhao.presto.udfs.scalar;

import com.wgzhao.presto.udfs.utils.IPSearcher;
import com.wgzhao.presto.udfs.utils.IsoSearcher;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.lionsoul.ip2region.DataBlock;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Map.entry;

public class IP2RegionFunction
{
    private static final String FILE_NAME = "/ip2region.db";
    private static final Map<String, Integer> gMap = Map.ofEntries(
        entry("country", 0),
        entry("g",0),
        entry("province",2),
        entry("p",2),
        entry("city",3),
        entry("c",3),
        entry("isp",4),
        entry("i",4)
    );

    private static final Map<String, Integer> codeMap = Map.ofEntries(
        entry("en", 0),
        entry("m2", 1),
        entry("m3", 2),
        entry("digit", 3)
    );

    private static final io.airlift.log.Logger logger = io.airlift.log.Logger.get(IP2RegionFunction.class);

    private IP2RegionFunction() {}

    private static String ipSearch(String ip, String segment)
    {
        String result = null;
        IPSearcher searcher = IPSearcher.getInstance(FILE_NAME);

        DataBlock dataBlock = searcher.lookup(ip);
        Map<String, String> ipInfo = new HashMap<>();
        if (dataBlock != null) {
            result = dataBlock.getRegion();
        }

        if (result == null) {
            return null;
        }
        //如果不指定返回部分，则返回全部
        if (segment == null) {
            return result.replace("0", "");
        }
        // 城市Id|国家|区域|省份|城市|ISP
        String[] arrInfo = result.split("\\|");
        gMap.forEach((k, v) -> ipInfo.put(k, arrInfo[v].equals("0") ? null : arrInfo[v]));

        //检查是否是需要返回区域信息
        if (gMap.containsKey(segment)) {
            return ipInfo.getOrDefault(segment, null);
        }

        //检查是否是需要返回国际编码信息
        if (codeMap.containsKey(segment)) {
            return IsoSearcher.getInstance().searcher(ipInfo.get("g"), segment);
        }
        //都不是，说明指定的返回编码出现未知情况，直接返回为空
        logger.warn("invalid segment values:" + segment);
        return null;
    }

    @Description("get region from IP Address")
    @ScalarFunction("udf_ip2region")
    @SqlType(StandardTypes.VARCHAR)
    public static @SqlNullable
    Slice ip2region(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice ip)
    {
        if (ip == null || ip.toStringUtf8().trim().isEmpty()) {
            return null;
        }
        if (!isIpAddress(ip.toStringUtf8())) {
            logger.warn("invalid IP address: " + ip.toStringUtf8());
            return null;
        }
        String res = ipSearch(ip.toStringUtf8(), null);
        if (res == null) {
            return Slices.EMPTY_SLICE;
        }
        return utf8Slice(res);
    }

    @Description("get region from IP Address")
    @ScalarFunction("udf_ip2region")
    @SqlType(StandardTypes.VARCHAR)
    public static @SqlNullable
    Slice ip2region(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice ip, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice segment)
    {
        if (ip == null || segment == null) {
            return null;
        }
        if (ip.toStringUtf8().trim().isEmpty() || segment.toStringUtf8().trim().isEmpty()) {
            return null;
        }
        if (! isIpAddress(ip.toStringUtf8())) {
            logger.warn("invalid IP address: " + ip.toStringUtf8());
            return null;
        }
        String result;
        result = ipSearch(ip.toStringUtf8(), segment.toStringUtf8());
        if (result == null) {
            return Slices.EMPTY_SLICE;
        }
        return utf8Slice(result);
    }

    private static boolean isIpAddress(String ip)
    {
        try {
            for (String part : ip.split("\\.")) {
                Integer.parseInt(part);
            }
            return true;
        }
        catch (NumberFormatException ignored) {
            return false;
        }
    }
}
