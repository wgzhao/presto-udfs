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

import io.airlift.slice.Slice;
import org.junit.jupiter.api.Test;

import static com.wgzhao.presto.udfs.scalar.TradeDateFunctions.udfAddNormalDays;
import static com.wgzhao.presto.udfs.scalar.TradeDateFunctions.udfAddTradeDays;
import static com.wgzhao.presto.udfs.scalar.TradeDateFunctions.udfIsTradeDate;
import static com.wgzhao.presto.udfs.scalar.TradeDateFunctions.udfLastTradeDate;
import static io.airlift.slice.Slices.utf8Slice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTradeDateFunctions
{
    private Slice res;
    private final Slice tradeDate = utf8Slice("20210906");
    private final Slice nonTradeDate = utf8Slice("20210904");
    @Test
    public void testLastTradeDate()
    {
        res = udfLastTradeDate(tradeDate);
        assertEquals("20210903", res.toStringUtf8());
    }

    @Test
    public void testIsTradeDate()
    {
        assertTrue(udfIsTradeDate(tradeDate));
        assertFalse(udfIsTradeDate(utf8Slice("20210904")));
        assertFalse(udfIsTradeDate(null));
    }

    @Test
    public void testAddTradeDays()
    {
        res = udfAddTradeDays(tradeDate, 4L);
        assertEquals("20210910", res.toStringUtf8());

        res = udfAddTradeDays(nonTradeDate, 1L);
        assertEquals("20210906", res.toStringUtf8());

        assertNull(udfAddTradeDays(null, 1L));
        assertNull(udfAddTradeDays(utf8Slice(""), 1L));

        assertNull(udfAddTradeDays(utf8Slice("19900101"), 1L));
        assertNull(udfAddTradeDays(utf8Slice("20460104"), 1L));
    }

    @Test
    public void testAddNormalDays()
    {
        res = udfAddNormalDays(tradeDate, 1L);
        assertEquals("20210907", res.toStringUtf8());

        res = udfAddNormalDays(nonTradeDate, 1L);
        assertEquals("20210903", res.toStringUtf8());

        assertNull(udfAddNormalDays(null, 1L));
        assertNull(udfAddNormalDays(utf8Slice(""), 1L));
    }
}
