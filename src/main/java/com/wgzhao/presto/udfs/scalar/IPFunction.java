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
import java.nio.ByteBuffer;

import static io.airlift.slice.Slices.utf8Slice;

public class IPFunction {
    private IPFunction() {}

    @Description("get region from IP Address")
    @ScalarFunction("ip_to_int")
    @SqlType(StandardTypes.INTEGER)
    public static long ip2int(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice ipStr ) throws IOException
    {
        if (ipStr == null || ipStr.toStringUtf8().equals("")) {
            return 0L;
        }
        InetAddress i= InetAddress.getByName(ipStr.toStringUtf8());
        return ByteBuffer.wrap(i.getAddress()).getLong();
    }

    @Description("get region from IP Address")
    @ScalarFunction("int_to_ip")
    @SqlType(StandardTypes.VARCHAR)
    public static @SqlNullable Slice int2ip(@SqlType(StandardTypes.INTEGER) long ip) throws IOException
    {
        if (ip < 0) {
            return null;
        }
        InetAddress i = InetAddress.getByName(String.valueOf(ip));
        String ipStr= i.getHostAddress();
        return  utf8Slice(ipStr);
    }

}
