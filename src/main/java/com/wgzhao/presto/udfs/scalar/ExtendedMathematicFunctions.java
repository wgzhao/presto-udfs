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

import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ExtendedMathematicFunctions
{
    private ExtendedMathematicFunctions() {}

    @SqlNullable
    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.BIGINT)
    public static Long positiveModulus(@SqlType(StandardTypes.BIGINT) long a, @SqlType(StandardTypes.BIGINT) long b)
    {
        if (b == 0) {
            return null;
        }
        return java.lang.Math.floorMod(a, b);
    }

    @SqlNullable
    @Description("Returns the positive value of a mod b ")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.DOUBLE)
    public static Double positiveModulusDouble(@SqlType(StandardTypes.DOUBLE) double a, @SqlType(StandardTypes.DOUBLE) double b)
    {
        if (b == 0) {
            return null;
        }

        if ((a >= 0 && b > 0) || (a <= 0 && b < 0)) {
            return a % b;
        }
        else {
            return (a % b) + b;
        }
    }
}
