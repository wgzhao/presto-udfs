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
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.sql.Timestamp;
import java.time.ZoneId;

import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;

public class ExtendedDateTimeFunctions
{
    private ExtendedDateTimeFunctions() {}

    @Description("given timestamp in UTC and converts to given timezone")
    @ScalarFunction("from_utc_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long fromUtcTimestamp(@SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.VARCHAR) Slice inputZoneId)
    {
        ZoneId zoneId = ZoneId.of(inputZoneId.toStringUtf8(), ZoneId.SHORT_IDS);
        long offsetTimestamp = packDateTimeWithZone(timestamp, zoneId.toString());
        return  timestamp + ((PrestoDateTimeFunctions.timeZoneHourFromTimestampWithTimeZone(offsetTimestamp) * 60
                + PrestoDateTimeFunctions.timeZoneMinuteFromTimestampWithTimeZone(offsetTimestamp)) * 60) * 1000;
    }

    @Description("given timestamp (in varchar) in UTC and converts to given timezone")
    @ScalarFunction("from_utc_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long fromUtcTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp, @SqlType(StandardTypes.VARCHAR) Slice inputZoneId)
    {
        Timestamp javaTimestamp = Timestamp.valueOf(inputTimestamp.toStringUtf8());
        ZoneId zoneId = ZoneId.of(inputZoneId.toStringUtf8(), ZoneId.SHORT_IDS);
        long offsetTimestamp = packDateTimeWithZone(javaTimestamp.getTime(), zoneId.toString());
        return  javaTimestamp.getTime() + ((PrestoDateTimeFunctions.timeZoneHourFromTimestampWithTimeZone(offsetTimestamp) * 60
                + PrestoDateTimeFunctions.timeZoneMinuteFromTimestampWithTimeZone(offsetTimestamp)) * 60) * 1000;
    }

    @Description("given timestamp in a timezone convert it to UTC")
    @ScalarFunction("to_utc_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long toUtcTimestamp(@SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.VARCHAR) Slice inputZoneId)
    {
        ZoneId zoneId = ZoneId.of(inputZoneId.toStringUtf8(), ZoneId.SHORT_IDS);
        long offsetTimestamp =  packDateTimeWithZone(timestamp, zoneId.toString());
        return timestamp - ((PrestoDateTimeFunctions.timeZoneHourFromTimestampWithTimeZone(offsetTimestamp) * 60
                + PrestoDateTimeFunctions.timeZoneMinuteFromTimestampWithTimeZone(offsetTimestamp)) * 60) * 1000;
    }

    @Description("given timestamp (in varchar) in a timezone convert it to UTC")
    @ScalarFunction("to_utc_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long toUtcTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp, @SqlType(StandardTypes.VARCHAR) Slice inputZoneId)
    {
        Timestamp javaTimestamp = Timestamp.valueOf(inputTimestamp.toStringUtf8());
        ZoneId zoneId = ZoneId.of(inputZoneId.toStringUtf8(), ZoneId.SHORT_IDS);
        long offsetTimestamp = packDateTimeWithZone(javaTimestamp.getTime(), zoneId.toString());
        return  javaTimestamp.getTime() - ((PrestoDateTimeFunctions.timeZoneHourFromTimestampWithTimeZone(offsetTimestamp) * 60
                + PrestoDateTimeFunctions.timeZoneMinuteFromTimestampWithTimeZone(offsetTimestamp)) * 60) * 1000;
    }
}
