package com.wgzhao.presto.udfs.utils;

import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbMakerConfigException;
import org.lionsoul.ip2region.DbSearcher;

import java.io.IOException;
import java.util.Objects;

public class IPSearcher
{
    private static final String FILE_NAME = "/ip2region.db";

    private static final class IPSearcherHolder
    {
        private static final IPSearcher instance = new IPSearcher();
    }

    private final DbSearcher dbSearcher;

    private IPSearcher()
    {
        try {
            DbConfig config = new DbConfig();
            byte[] dbData = Objects.requireNonNull(IPSearcher.class.getResourceAsStream(FILE_NAME)).readAllBytes();
            this.dbSearcher = new DbSearcher(config, dbData);
            // load into memory when first query
            this.dbSearcher.memorySearch("127.0.0.1");
        }
        catch (DbMakerConfigException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static IPSearcher getInstance()
    {
        return IPSearcherHolder.instance;
    }

    public DataBlock lookup(String ipStr)
    {
        try {
            return this.dbSearcher.memorySearch(ipStr);
        }
        catch (IOException e) {
            return null;
        }
    }

    public DataBlock lookup(long ipLong)
    {
        try {
            return this.dbSearcher.memorySearch(ipLong);
        }
        catch (IOException e) {
            return null;
        }
    }
}
