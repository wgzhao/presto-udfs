package com.wgzhao.presto.udfs.scalar;

import com.github.promeg.pinyinhelper.Pinyin;
import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.airlift.slice.Slices.utf8Slice;

public class PinyinFunction
{
    private PinyinFunction() {}

    @Description("convert chinese to pinyin")
    @ScalarFunction("udf_pinyin")
    @SqlType(StandardTypes.VARCHAR)
    public @SqlNullable static Slice pinyin(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice chinese)
    {
        if (chinese == null) {
            return null;
        }
        String chStr = chinese.toStringUtf8();
        if (chStr.equals("")) {
            return utf8Slice("");
        }

        return utf8Slice(Pinyin.toPinyin(chStr, "").toLowerCase());

    }

}
