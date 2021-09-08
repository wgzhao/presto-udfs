/*
 * Copyright 2019
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
package com.wgzhao.presto.udfs;

import com.google.common.collect.ImmutableSet;
import com.wgzhao.presto.udfs.scalar.ExtendedMathematicaFunctions;
import com.wgzhao.presto.udfs.scalar.ExtendedNumberFunctions;
import com.wgzhao.presto.udfs.scalar.ExtendedStringFunctions;
import com.wgzhao.presto.udfs.scalar.IP2RegionFunction;
import com.wgzhao.presto.udfs.scalar.IPFunction;
import com.wgzhao.presto.udfs.scalar.IdCardFunctions;
import com.wgzhao.presto.udfs.scalar.List2Json;
import com.wgzhao.presto.udfs.scalar.Map2Json;
import com.wgzhao.presto.udfs.scalar.Mobile2Region;
import com.wgzhao.presto.udfs.scalar.TradeDateFunctions;
import com.wgzhao.presto.udfs.window.FirstNonNullValueFunction;
import com.wgzhao.presto.udfs.window.LastNonNullValueFunction;
import io.trino.spi.Plugin;

import java.util.Set;

public class UdfPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(ExtendedMathematicaFunctions.class)
                .add(ExtendedStringFunctions.class)
                .add(FirstNonNullValueFunction.class)
                .add(LastNonNullValueFunction.class)
                .add(ExtendedNumberFunctions.class)
                .add(IP2RegionFunction.class)
                .add(Mobile2Region.class)
                .add(Map2Json.class)
                .add(List2Json.class)
                .add(TradeDateFunctions.class)
                .add(IPFunction.class)
                .add(IdCardFunctions.class)
                .build();
    }
}
