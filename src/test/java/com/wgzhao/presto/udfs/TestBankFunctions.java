package com.wgzhao.presto.udfs;

import io.airlift.slice.Slice;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import com.wgzhao.presto.udfs.scalar.BankFunction;

import static io.airlift.slice.Slices.utf8Slice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBankFunctions
{
    /**
     * code = CEB, name = 中国光大银行
     * code = ICBC, name = 中国工商银行
     * code = ICBC, name = 中国工商银行
     * code = CCB, name = 中国建设银行
     * code = CCB, name = 中国建设银行
     * code = ABC, name = 中国农业银行
     * code = GDB, name = 广发银行
     * code = CIB, name = 兴业银行
     * code = PSBC, name = 中国邮政储蓄银行
     * code = CMB, name = 招商银行
     * code = CITIC, name = 中信银行
     * code = BOC, name = 中国银行
     * code = BOC, name = 中国银行
     * code = BOC, name = 中国银行
     */
    List<String> cards = Arrays.asList("622664", "955880", "622203", "621700292010", "62170029", "622848109", "62179226", "6229093",
            "62218855", "62258873", "621768160266", "62175675", "62166075", "45635175");
    @Test
    public void testBankCode()
    {
        Slice code = BankFunction.getBankCode(utf8Slice(cards.get(0)));
        assertEquals("CEB", code.toStringUtf8());
    }

    @Test
    public void testBankName()
    {
        Slice name = BankFunction.getBankName(utf8Slice(cards.get(0)));
        assertEquals("中国光大银行", name.toStringUtf8());
    }
}
