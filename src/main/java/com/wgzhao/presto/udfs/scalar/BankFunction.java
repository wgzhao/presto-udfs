package com.wgzhao.presto.udfs.scalar;

import com.alibaba.fastjson2.JSONObject;
import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.airlift.slice.Slices.utf8Slice;

public class BankFunction
{
    private static final String fileName = "/bankInfo.json";
    private static final List<Integer> checkLength = Arrays.asList(9, 8, 6, 5, 4, 3);
    private static Map<String, String> cardToBankName = new HashMap<>();
    private static Map<String, String> cardToType = new HashMap<>();
    private static Map<String, String> cardToCode = new HashMap<>();

    static {
        try {
            String jsonString =
                    IOUtils.toString(Objects.requireNonNull(BankFunction.class.getResourceAsStream(fileName)), StandardCharsets.UTF_8);
            JSONObject jsonObject = JSONObject.parseObject(jsonString);
            cardToBankName = (Map<String, String>) jsonObject.get("codeToBank");
            cardToType = (Map<String, String>) jsonObject.get("cardToType");
            cardToCode = (Map<String, String>) jsonObject.get("cardToCode");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private BankFunction() {}

    private static String getCardCode(String card)
    {
        for (int len : checkLength) {
            if (card.length() >= len && cardToCode.containsKey(card.substring(0, len))) {
                return cardToCode.get(card.substring(0, len));
            }
        }
        return null;
    }

    @Description("get bank name from card number")
    @ScalarFunction("udf_bank_name")
    @SqlType(StandardTypes.VARCHAR)
    public static @SqlNullable
    Slice getBankName(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice cardNo)
    {
        if (cardNo == null || cardNo.toStringUtf8().isEmpty()) {
            return null;
        }
        String code = getCardCode(cardNo.toStringUtf8());
        if (code == null) {
            return null;
        }
        return utf8Slice(cardToBankName.get(code));
    }

    @Description("get bank code from card number")
    @ScalarFunction("udf_bank_name")
    @SqlType(StandardTypes.VARCHAR)
    public static @SqlNullable
    Slice getBankCode(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice cardNo)
    {
        if (cardNo == null || cardNo.toStringUtf8().isEmpty()) {
            return null;
        }
        String code = getCardCode(cardNo.toStringUtf8());
        return code == null ? null : utf8Slice(code);
    }
}
