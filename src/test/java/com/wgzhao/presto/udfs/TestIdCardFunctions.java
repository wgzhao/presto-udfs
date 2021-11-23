package com.wgzhao.presto.udfs;

import com.wgzhao.presto.udfs.scalar.IdCardFunctions;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestIdCardFunctions
{
    private final String illegalId = "430203198607230123";
    private final String validStr = "610923195408220390";

    @Test
    public void testIdCheck() {

       boolean isIdCard = IdCardFunctions.idCheck(utf8Slice(illegalId));
       assertFalse(isIdCard);
       isIdCard = IdCardFunctions.idCheck(utf8Slice(validStr));
       assertTrue(isIdCard);
    }

    @Test
    public void testIdGetBirthday()
    {
        assertEquals(0L, IdCardFunctions.idCheckWithBirthday(utf8Slice(illegalId)));
        assertEquals(19540822L, IdCardFunctions.idCheckWithBirthday(utf8Slice(validStr)));
    }

    @Test
    public void testBirthday()
    {
        Long birthFromIdCard = IdCardFunctions.getBirthFromIdCard(utf8Slice(validStr));
        assertEquals(19540822L, birthFromIdCard);
    }
}
