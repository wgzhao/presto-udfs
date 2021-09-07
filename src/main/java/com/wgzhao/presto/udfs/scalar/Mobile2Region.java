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

import com.wgzhao.presto.udfs.utils.MobileSearcher;
import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import me.ihxq.projects.pna.PhoneNumberInfo;

import static io.airlift.slice.Slices.utf8Slice;

public class Mobile2Region
{

    private Mobile2Region()
    {
    }

    private static String mobileSearch(String phone, String segment)
    {
        MobileSearcher mobileSearcher = MobileSearcher.getInstance();
        try {
            PhoneNumberInfo phoneInfo = mobileSearcher.lookup(phone);

            if (phoneInfo == null) {
                return null;
            }

            if (segment == null) {
                return phoneInfo.getAttribution().getProvince() + "|"
                        + phoneInfo.getAttribution().getCity() + "|"
                        + phoneInfo.getIsp().getCnName();
            }
            String res;
            switch (segment) {
                case "province":
                case "p":
                    res = phoneInfo.getAttribution().getProvince();
                    break;
                case "city":
                case "c":
                    res = phoneInfo.getAttribution().getCity();
                    break;
                case "isp":
                case "i":
                    res = phoneInfo.getIsp().getCnName();
                    break;
                default:
                    res = "";
                    break;
            }
            return res;
        }
        catch (Exception e) {
            return null;
        }
    }

    @Description("get region from mobile phone")
    @ScalarFunction("mobile2region")
    @SqlType(StandardTypes.VARCHAR)
    public static @SqlNullable
    Slice mobile2region(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice phone)
    {
        String result;
        if (phone == null || "".equals(phone.toStringUtf8().trim())) {
            return null;
        }
        result = mobileSearch(phone.toStringUtf8().trim(), null);
        if (result == null) {
            return null;
        }
        return utf8Slice(result);
    }

    @Description("get region from mobile phone")
    @ScalarFunction("mobile2region")
    @SqlType(StandardTypes.VARCHAR)
    public static @SqlNullable
    Slice mobile2region(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice phone,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice segment)
    {
        String result;
        if (phone == null
                || "".equals(phone.toStringUtf8().trim())
                || segment == null
                || "".equals(segment.toStringUtf8().trim())) {
            return null;
        }
        result = mobileSearch(phone.toStringUtf8().trim(), segment.toStringUtf8().trim());
        if (result == null) {
            return null;
        }
        return utf8Slice(result);
    }
}
