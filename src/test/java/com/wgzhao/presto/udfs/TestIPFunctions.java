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

import com.wgzhao.presto.udfs.scalar.IPFunction;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestIPFunctions
{
    @Test
    public void testIPToInt()
    {
        assertEquals(3232235777L, IPFunction.ip2int(utf8Slice("192.168.1.1")));
        assertEquals(16843008L , IPFunction.ip2int(utf8Slice("1.1.1.0")));
        assertEquals(16777217L, IPFunction.ip2int(utf8Slice("1.0.0.1")));
        assertNull(IPFunction.ip2int(null));
        assertNull(IPFunction.ip2int(utf8Slice("abc")));
        assertNull(IPFunction.ip2int(utf8Slice("a.b.c.d")));
        assertNull(IPFunction.ip2int(utf8Slice("1.1.1.a")));
    }

    @Test
    public void testIntToIP()
    {
        assertNull(IPFunction.int2ip(null));
        assertNull(IPFunction.int2ip(-12L));
        assertEquals("192.168.1.1", IPFunction.int2ip(3232235777L).toStringUtf8());
        assertEquals("1.1.1.0", IPFunction.int2ip(16843008L).toStringUtf8());
        assertEquals("1.0.0.1", IPFunction.int2ip(16777217L).toStringUtf8());
    }
}
