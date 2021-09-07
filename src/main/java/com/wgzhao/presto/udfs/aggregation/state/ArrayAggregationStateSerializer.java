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
package com.wgzhao.presto.udfs.aggregation.state;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class ArrayAggregationStateSerializer
        implements AccumulatorStateSerializer<ArrayAggregationState>
{
    @Override
    public Type getSerializedType()
    {
        return VARCHAR;
    }

    @Override
    public void serialize(ArrayAggregationState state, BlockBuilder out)
    {
        out.writeLong(state.getEntries());
        if (state.getEntries() > 0) {
            Slice slice = state.getSliceOutput().slice();
            out.writeLong(slice.length());
            out.writeBytes(slice, 0, slice.length());
        }
        out.closeEntry();
    }

    @Override
    public void deserialize(Block block, int index, ArrayAggregationState state)
    {
        SliceInput input = block.getSlice(index, 0, block.getSliceLength(index)).getInput();
        long entries = input.readLong();
        state.setEntries(entries);
        if (entries > 0) {
            long length = input.readLong();
            SliceOutput sliceOutput = new DynamicSliceOutput((int) length);
            sliceOutput.appendBytes(input.readSlice((int) length));
            state.setSliceOutput(sliceOutput);
        }
    }
}
