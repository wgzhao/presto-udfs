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
package com.wgzhao.presto.udfs.window;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.ValueWindowFunction;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;

public class FirstOrLastValueIgnoreNullFunction
        extends ValueWindowFunction
{
    private final int argumentChannel;
    private final Direction d;
    // data for caching, see processRow
    private int start = -1;
    private int end = -1;
    private int nonNullPositionInFrame = -1;

    protected FirstOrLastValueIgnoreNullFunction(List<Integer> argumentChannels, Direction d)
    {
        this.argumentChannel = getOnlyElement(argumentChannels);
        this.d = d;
    }

    @Override
    public void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition)
    {
        // processRow is called for each row inside the frame => it will be costly to find non null last value everytime
        // => cache that value in object
        if (frameStart < 0) {
            output.appendNull();
            return;
        }

        if (frameStart == start && frameEnd == end) {
            // Found cached value in frame
            windowIndex.appendTo(argumentChannel, nonNullPositionInFrame, output);
            return;
        }

        int pos = -1;
        switch (d) {
            case LAST:
                pos = getLastNotNullPosition(frameStart, frameEnd);
                break;
            case FIRST:
                pos = getFirstNotNullPosition(frameStart, frameEnd);
                break;
            default:
                break;
        }

        if (pos != -1) {
            windowIndex.appendTo(argumentChannel, pos, output);
            nonNullPositionInFrame = pos;
            start = frameStart;
            end = frameEnd;
            return;
        }

        // append NULL if no non-null value found
        output.appendNull();
    }

    private int getLastNotNullPosition(int frameStart, int frameEnd)
    {
        int i;
        for (i = frameEnd; i >= frameStart; i--) {
            if (!windowIndex.isNull(argumentChannel, i)) {
                return i;
            }
        }

        return -1;
    }

    private int getFirstNotNullPosition(int frameStart, int frameEnd)
    {
        int i;
        for (i = frameStart; i <= frameEnd; i++) {
            if (!windowIndex.isNull(argumentChannel, i)) {
                return i;
            }
        }

        return -1;
    }

    public enum Direction
    {
        FIRST,
        LAST
    }
}
