package com.wgzhao.presto.udfs;

import static com.wgzhao.presto.udfs.scalar.ExtendedDateTimeFunctions.toDate8;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestExtendDatetimeFunctions
{
    @Test
    public void testToDate8()
    {
        assertNull(toDate8(null));
        assertNull(toDate8(utf8Slice("")));
        assertNull(toDate8(utf8Slice(" ")));
        assertNull(toDate8(utf8Slice("abc")));
        assertNull(toDate8(utf8Slice("2021-02")));
        assertEquals("20190101", toDate8(utf8Slice("20190101")).toStringUtf8());
        assertEquals("20190101", toDate8(utf8Slice("2019-01-01")).toStringUtf8());
        assertEquals("20190101", toDate8(utf8Slice("2019-01-01 00:00:00")).toStringUtf8());
        assertEquals("20190101", toDate8(utf8Slice("2019-01-01 00:00:00.000")).toStringUtf8());
        assertEquals("20190101", toDate8(utf8Slice("2019-01-01 00:00:00.0000")).toStringUtf8());
        assertEquals("20190101", toDate8(utf8Slice("2019-01-01 00:00:00.00000")).toStringUtf8());
        assertEquals("20190101", toDate8(utf8Slice("2019-01-01 00:00:00.000000")).toStringUtf8());
        assertEquals("20190101", toDate8(utf8Slice("2019-01-01 00:00:00.0000000")).toStringUtf8());
    }
}
