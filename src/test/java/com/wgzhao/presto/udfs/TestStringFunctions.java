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

import com.wgzhao.presto.udfs.scalar.ExtendedStringFunctions;
import io.airlift.slice.Slice;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestStringFunctions
{
    @Test
    public void testToChinese()
    {
        Slice res;
        res = ExtendedStringFunctions.convertDigitalToChinese(utf8Slice("2002"));
        assertEquals("贰零零贰", res.toStringUtf8());
        res = ExtendedStringFunctions.convertDigitalToChinese(utf8Slice("2002"), true);
        assertEquals("二〇〇二", res.toStringUtf8());
    }

    @Test
    public void testEval()
    {
        double res;
        res = ExtendedStringFunctions.eval(utf8Slice("23+12*1"));
        assertEquals(35, res);
        assertNull(ExtendedStringFunctions.eval(utf8Slice("1+2i")));
        assertNull(ExtendedStringFunctions.eval(utf8Slice("23-")));
    }

    @Test
    public void listAllScriptManager()
    {
        ScriptEngineManager sem = new ScriptEngineManager();
        List<ScriptEngineFactory> factories = sem.getEngineFactories();
        for (ScriptEngineFactory factory : factories)
            System.out.println(factory.getEngineName() + " " + factory.getEngineVersion() + " " + factory.getNames());
        if (factories.isEmpty())
            System.out.println("No Script Engines found");
    }
}
