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

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.io.IOException;
import java.net.InetAddress;

import static io.airlift.slice.Slices.utf8Slice;

public class IPFunction
{
    private IPFunction() {}

    @Description("get region from IP Address")
    @ScalarFunction("udf_ip2int")
    @SqlType(StandardTypes.BIGINT)
    public @SqlNullable
    static Long ip2int(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice ipStr)
    {
        if (ipStr == null || ipStr.toStringUtf8().equals("")) {
            return null;
        }
        String[] ip = ipStr.toStringUtf8().split("\\.");
        if (ip.length != 4) {
            return null;
        }
        long result = 0;
        try {
            for (String part : ip) {
                result = result << 8;
                result |= Integer.parseInt(part);
            }
            return result;
        }
        catch (NumberFormatException ignored) {
            return null;
        }
    }

    @Description("get region from IP Address")
    @ScalarFunction("udf_int2ip")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice int2ip(@SqlNullable @SqlType(StandardTypes.INTEGER) Long ip)
    {
        if (ip == null || ip < 0) {
            return null;
        }

        try {
            InetAddress i = InetAddress.getByName(String.valueOf(ip));
            String ipStr = i.getHostAddress();
            return utf8Slice(ipStr);
        }
        catch (IOException ignored) {
            return null;
        }
    }

    public static void main(String[] args)
    {
        int a = 1;
        a |= 0;
        System.out.println(a);
    }
}
