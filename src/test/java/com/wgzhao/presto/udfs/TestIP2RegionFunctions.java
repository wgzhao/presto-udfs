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

import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static com.wgzhao.presto.udfs.scalar.IP2RegionFunction.ip2region;

public class TestIP2RegionFunctions
{
    @Test
    public void testIP2Region()
    {
        assertNull(ip2region(null));
        assertNull(ip2region(utf8Slice("")));
        assertNull(ip2region(utf8Slice("a.b.c")));
        assertNull(ip2region(utf8Slice("192.168.1.a")));
        assertEquals("中国||北京|北京市|腾讯", ip2region(utf8Slice("119.29.29.29")).toStringUtf8());
        assertEquals("中国", ip2region(utf8Slice("119.29.29.29"), utf8Slice("g")).toStringUtf8());
        assertEquals("腾讯", ip2region(utf8Slice("119.29.29.29"), utf8Slice("i")).toStringUtf8());
    }
}
