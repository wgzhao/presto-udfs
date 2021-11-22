package com.wgzhao.presto.udfs;

import com.google.common.collect.ImmutableList;
import com.wgzhao.presto.udfs.scalar.XmlFunctions;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestXmlFunctions
{

    @Test
    public void testNullXPath() {
        String xml = "<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>";
        String xpath = "a/text()";
        final Block result = XmlFunctions.xpath(utf8Slice(xml), utf8Slice(xpath));
        System.out.println(result.toString());
    }

    @Test
    public void testNullMultiResultXPath() {
        String xml = "<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>";
        String xpath = "a/b/text()";
        final Block result = XmlFunctions.xpath(utf8Slice(xml), utf8Slice(xpath));
        final ImmutableList<String> expected = ImmutableList.of("b1", "b2", "b3");
        Slice actual = result.getSlice(0, 0, result.getPositionCount());
        System.out.println(actual.toStringUtf8());
    }

    @Test
    public void testXpathBoolean() {
        String xml = "<a><b>1</b></a>";
        String path1 = "a/b";
        String path2 = "a/b = 2";
        Boolean result = XmlFunctions.xpathBoolean(utf8Slice(xml), utf8Slice(path1));
        assertTrue(result);
        result = XmlFunctions.xpathBoolean(utf8Slice(xml), utf8Slice(path2));
        assertFalse(result);
    }

    @Test
    public void testXpathDouble()
    {
        assertEquals(
                XmlFunctions.xpathDouble(utf8Slice("<a><b>1</b><b>2</b></a>"),utf8Slice("sum(a/b)")),
                3.0);
    }

    @Test
    public void testXpathLong()
    {
        assertEquals(
                XmlFunctions.xpathLong(utf8Slice("<a><b>1</b><b>2</b></a>"),utf8Slice("sum(a/b)")),
                3);
    }


    @Test
    public void testXpathString()
    {
        assertEquals(
                XmlFunctions.xpathString(utf8Slice("<a><b>b1</b><b id=\"b_2\">b2</b></a>"),
                        utf8Slice("a/b[@id=\"b_2\"]")),
                utf8Slice("b2"));
    }
}
