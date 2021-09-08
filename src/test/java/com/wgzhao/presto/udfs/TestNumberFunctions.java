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

package com.wgzhao.presto.udfs;

import com.wgzhao.presto.udfs.scalar.ExtendedNumberFunctions;
import io.airlift.slice.Slice;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestNumberFunctions
{
    long num = 103543;
    String numStr = String.valueOf(num);
    String chineseNum = "十万三千五百四十三";
    String chineseCapNum = "拾万叁仟伍佰肆拾叁";

    @Test
    public void testNumToCh()
    {
        Slice res;

        res = ExtendedNumberFunctions.convertArabiaNumberToChineseNumber(num, true);
        assertEquals(chineseNum, res.toStringUtf8());
        res = ExtendedNumberFunctions.convertArabiaNumberToChineseNumber(utf8Slice(numStr), true);
        assertEquals(chineseNum, res.toStringUtf8());

        res = ExtendedNumberFunctions.convertArabiaNumberToChineseNumber(num);
        assertEquals(chineseCapNum, res.toStringUtf8());
        res = ExtendedNumberFunctions.convertArabiaNumberToChineseNumber(utf8Slice(numStr));
        assertEquals(chineseCapNum, res.toStringUtf8());

    }

    @Test
    public void testChToNum()
    {
        long res;
        assertNull(ExtendedNumberFunctions.convertChineseNumberToArabiaNumber(null));
        assertNull(ExtendedNumberFunctions.convertChineseNumberToArabiaNumber(utf8Slice("abc")));
        assertNull(ExtendedNumberFunctions.convertChineseNumberToArabiaNumber(utf8Slice("")));
        res = ExtendedNumberFunctions.convertChineseNumberToArabiaNumber(utf8Slice("壹" + chineseCapNum));
        assertEquals(num, res);
        res = ExtendedNumberFunctions.convertChineseNumberToArabiaNumber(utf8Slice("一" + chineseNum));
        assertEquals(num, res);

    }
}
