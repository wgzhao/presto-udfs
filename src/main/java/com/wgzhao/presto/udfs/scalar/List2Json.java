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

import com.alibaba.fastjson2.JSON;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.io.IOException;

import static io.airlift.slice.Slices.utf8Slice;

public class List2Json
{

    private List2Json() {}

    @Description("convert List to Json")
    @ScalarFunction("list_json")
    @SqlType(StandardTypes.JSON)
    public static @SqlNullable
    Slice ListToJson(@SqlNullable @SqlType(StandardTypes.ARRAY) Slice aList)
            throws IOException
    {
        String res = JSON.parseArray(aList.toString()).toString();
        if (res == null) {
            return Slices.EMPTY_SLICE;
        }
        return utf8Slice(res);
    }
}
