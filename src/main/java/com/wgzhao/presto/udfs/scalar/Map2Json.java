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

import static com.alibaba.fastjson.JSON.toJSONString;
import static io.airlift.slice.Slices.utf8Slice;

public class Map2Json
{
    private Map2Json() {}

    @Description("convert Map to Json")
    @ScalarFunction("map_json")
    @SqlType(StandardTypes.JSON)
    public static Slice MapToJson(@SqlNullable @SqlType(StandardTypes.MAP) Slice map)
    {
        String res;
        res = toJSONString(map);
        return utf8Slice(res);
    }
}
