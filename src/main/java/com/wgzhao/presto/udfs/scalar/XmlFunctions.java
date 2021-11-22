package com.wgzhao.presto.udfs.scalar;

import com.wgzhao.presto.udfs.utils.UDFXPathUtil;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.w3c.dom.NodeList;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

/**
 * implementation of xml string operation function, migrate from hive function
 * https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/xml/
 */
public class XmlFunctions
{
    private static final UDFXPathUtil xpathUtil = UDFXPathUtil.getInstance();
    private XmlFunctions() {}

    /**
     * xpath(xml, xpath) - Returns a string array of values within xml nodes that match the xpath expression
     * <code>
     *  Example:
     *   SELECT _FUNC_('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/text()') FROM src LIMIT 1
     *   []\n"
     *   SELECT _FUNC_('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b/text()') FROM src LIMIT 1
     *   [\"b1\",\"b2\",\"b3\"]\n"
     *   SELECT _FUNC_('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/c/text()') FROM src LIMIT 1
     *   [\"c1\",\"c2\"]"
     * </code>
     */
    @SqlNullable
    @Description("Returns a string array of values within xml nodes that match the xpath expression")
    @ScalarFunction("udf_xpath")
    @LiteralParameters({"x", "y"})
    @SqlType("array(varchar(x))")
    public static Block xpath(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        BlockBuilder parts = VARCHAR.createBlockBuilder(null, 1, xml.length());
        NodeList nodeList = xpathUtil.evalNodeList(xml.toStringUtf8(), path.toStringUtf8());

        for (int i = 0; i < nodeList.getLength(); i++) {
            VARCHAR.writeSlice(parts, utf8Slice(nodeList.item(i).getNodeValue()));
        }
        return parts.build();
    }

    /**
     * xpath_boolean(xml, xpath) - Returns the number of nodes that match the xpath expression
     * <code>
     *     xpath_boolean('<a><b>1</b></a>','a/b') return true
     *     xpath_boolean('<a><b>1</b></a>','a/b = 2') return false
     * </code>
     * @param xml xml string
     * @param path xpath expression
     * @return true if found , else false
     */
    @SqlNullable
    @Description("Evaluates a boolean xpath expression")
    @ScalarFunction("udf_xpath_boolean")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean xpathBoolean(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        return xpathUtil.evalBoolean(xml.toStringUtf8(), path.toStringUtf8());
    }

    @SqlNullable
    @Description("Returns a double value that matches the xpath expression")
    @ScalarFunction(value = "udf_xpath_double", alias={"udf_xpath_number", "udf_xpath_float"})
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.DOUBLE)
    public static Double xpathDouble(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        return xpathUtil.evalNumber(xml.toStringUtf8(), path.toStringUtf8());
    }


    @SqlNullable
    @Description("Returns a long value that matches the xpath expression")
    @ScalarFunction(value = "udf_xpath_long", alias="udf_xpath_int")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static Long xpathLong(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        return xpathUtil.evalNumber(xml.toStringUtf8(), path.toStringUtf8()).longValue();
    }

    @SqlNullable
    @Description("Returns a string value that matches the xpath expression")
    @ScalarFunction(value = "udf_xpath_string", alias="udf_xpath_str")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice xpathString(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        String result = xpathUtil.evalString(xml.toStringUtf8(), path.toStringUtf8());
        return result == null ? null : utf8Slice(result);
    }
}


