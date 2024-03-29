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

import com.wgzhao.presto.udfs.utils.IdCardUtil;
import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

/**
 * 身份证校验，对于合法的身份证返回true，否则返回false，对于空身份证，则返回null
 */
public class IdCardFunctions
{
    private IdCardFunctions() {}

    @Description("Check IdCard is valid or not")
    @ScalarFunction("udf_is_idcard")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean idCheck(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice idNo)
    {
        if (idNo == null) {
            return false;
        }
        return IdCardUtil.isValidCard(idNo.toStringUtf8());
    }

    @Description("Check ID Card is valid or not, return the birthday if valid else 0")
    @ScalarFunction("id_check")
    @SqlType(StandardTypes.BIGINT)
    public static long idCheckWithBirthday(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice idNo)
    {
        if (idNo == null ) {
            return 0L;
        }
        String prettyId = idNo.toStringUtf8().trim();
        if (prettyId.isEmpty()) {
            return 0L;
        }
        if (!IdCardUtil.isValidCard(idNo.toStringUtf8())) {
            return 0L;
        }
        String birthday = IdCardUtil.getBirth(idNo.toStringUtf8());
        return birthday == null ? 0L : Long.parseLong(birthday);
    }

    /**
     * 提取身份证号码里的生日，格式为 <code>yyyyMMdd</code>, 如果给定的身份证为空或不合法，则返回为 NULL
     *
     * @param idNo 身份证号码，支持中国大陆身份证（15位和18位）以及港澳台的10位证件号码
     * @return 生日（code>yyyyMMdd</code>）
     */
    @Description("Extract birthday from valid ID card")
    @ScalarFunction("udf_get_birthday")
    @SqlType(StandardTypes.INTEGER)
    public @SqlNullable
    static Long getBirthFromIdCard(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice idNo)
    {
        if (idNo == null) {
            return null;
        }
        String prettyId = idNo.toStringUtf8().trim();
        if (!IdCardUtil.isValidCard(prettyId)) {
            return null;
        }
        String birthday = IdCardUtil.getBirth(prettyId);
        if (birthday == null) {
            return null;
        }
        else {
            return Long.parseLong(birthday);
        }
    }
}
