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

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static io.airlift.slice.Slices.utf8Slice;

public class ExtendedStringFunctions
{
    private static final String[][] nums = {
            {"零", "壹", "贰", "叁", "肆", "伍", "陆", "柒", "捌", "玖",},
            {"〇", "一", "二", "三", "四", "五", "六", "七", "八", "九",}};

    private ExtendedStringFunctions() {}

    @Description("the implement of javascript eval function")
    @ScalarFunction("udf_eval")
    @SqlType(StandardTypes.DOUBLE)
    public @SqlNullable
    static Double eval(@SqlType(StandardTypes.VARCHAR) Slice estr)
    {
        if (estr == null || "".equals(estr.toStringUtf8())) {
            return null;
        }

        String evalStr = estr.toStringUtf8();

        ScriptEngineManager manager = new ScriptEngineManager();
        try {
            ScriptEngine engine = manager.getEngineByName("js");
            return Double.parseDouble(engine.eval(evalStr).toString());
        }
        catch (ScriptException ignored) {
            return null;
        }

    }

    @Description("convert number string to chinese number string")
    @ScalarFunction("udf_to_chinese")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice convertDigitalToChinese(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        if (slice == null || "".equals(slice.toStringUtf8())) {
            return null;
        }
        String res = toChinese(slice.toStringUtf8(), false);
        if (res == null) {
            return null;
        }
        return utf8Slice(res);
    }

    @Description("convert number string to chinese number string")
    @ScalarFunction("udf_to_chinese")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice convertDigitalToChinese(@SqlType(StandardTypes.VARCHAR) Slice slice, @SqlType(StandardTypes.BOOLEAN) boolean flag)
    {
        if (slice == null || "".equals(slice.toStringUtf8())) {
            return null;
        }

        String res = toChinese(slice.toStringUtf8(), flag);

        if (res == null) {
            return null;
        }
        return utf8Slice(res);
    }

    private static String toChinese(String str, boolean flag)
    {
        int pos = flag ? 1 : 0;
        int curDigit;
        StringBuilder sb = new StringBuilder();
        try {
            for (int i = 0; i < str.length(); i++) {
                curDigit = Integer.parseInt(str.charAt(i) + "");
                sb.append(nums[pos][curDigit]);
            }
            return sb.toString();
        }
        catch (NumberFormatException e) {
            return null;
        }
    }
}
