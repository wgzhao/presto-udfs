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

import com.wgzhao.presto.udfs.sqlFunction.Hash;
import com.wgzhao.presto.udfs.sqlFunction.Nvl;
import io.prestosql.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import com.wgzhao.presto.udfs.scalar.TradeDateFunctions;
import com.wgzhao.presto.udfs.scalar.IP2Region;
import com.wgzhao.presto.udfs.scalar.IPFunction;
import com.wgzhao.presto.udfs.scalar.IdCheck;
import com.wgzhao.presto.udfs.scalar.List2Json;
import com.wgzhao.presto.udfs.scalar.Map2Json;
import com.wgzhao.presto.udfs.scalar.Mobile2Region;
import com.wgzhao.presto.udfs.scalar.ExtendedDateTimeFunctions;
import com.wgzhao.presto.udfs.scalar.ExtendedMathematicFunctions;
import com.wgzhao.presto.udfs.scalar.ExtendedNumberFunctions;
import com.wgzhao.presto.udfs.scalar.ExtendedStringFunctions;
import com.wgzhao.presto.udfs.window.FirstNonNullValueFunction;
import com.wgzhao.presto.udfs.window.LastNonNullValueFunction;

import java.util.Set;

public class UdfPlugin implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        /*
         * Presto 0.157 does not expose the interfaces to add SqlFunction objects directly
         * We can only add udfs via Annotations now
         *
         * Unsupported udfs right now:
         * Hash
         * Nvl
         * array_aggr
         */
        return ImmutableSet.<Class<?>>builder()
                .add(ExtendedDateTimeFunctions.class)
                .add(ExtendedMathematicFunctions.class)
                .add(ExtendedStringFunctions.class)
                .add(FirstNonNullValueFunction.class)
                .add(LastNonNullValueFunction.class)
                .add(ExtendedNumberFunctions.class)
                .add(IP2Region.class)
                .add(Mobile2Region.class)
                .add(Map2Json.class)
                .add(List2Json.class)
                .add(TradeDateFunctions.class)
                .add(IPFunction.class)
                .add(IdCheck.class)
                .add(Nvl.class)
                .add(Hash.class)
                .build();
    }
}
