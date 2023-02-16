package com.wgzhao.presto.udfs;

import com.wgzhao.presto.udfs.scalar.PinyinFunction;
import io.airlift.slice.Slice;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static io.airlift.slice.Slices.utf8Slice;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPinyinFunction
{

    @Test
    public void testPinyin()
    {
        Slice result;
        result = PinyinFunction.pinyin(utf8Slice("中"));
        assertEquals( "zhong", result.toStringUtf8());

        result =  PinyinFunction.pinyin(null);
        assertNull(result);

        result = PinyinFunction.pinyin(utf8Slice(""));
        assertEquals("", result.toStringUtf8());

        result = PinyinFunction.pinyin(utf8Slice("english"));
        assertEquals("english", result.toStringUtf8());

    }
}
