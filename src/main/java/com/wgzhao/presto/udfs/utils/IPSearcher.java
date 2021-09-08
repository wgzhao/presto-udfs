package com.wgzhao.presto.udfs.utils;

import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbMakerConfigException;
import org.lionsoul.ip2region.DbSearcher;

import java.io.IOException;
import java.util.Objects;

public class IPSearcher
{
    private static final Object mutex = new Object();
    private static volatile IPSearcher instance;
    private final DbSearcher dbSearcher;

    private IPSearcher(String filepath)
            throws DbMakerConfigException, IOException
    {
        DbConfig config = new DbConfig();
        byte[] dbData = Objects.requireNonNull(IPSearcher.class.getResourceAsStream(filepath)).readAllBytes();
        this.dbSearcher = new DbSearcher(config, dbData);
        // load into memory when first query
        this.dbSearcher.memorySearch("127.0.0.1");
    }

    public static IPSearcher getInstance(String filepath)
    {
        try {
            IPSearcher result = instance;
            if (result == null) {
                synchronized (mutex) {
                    result = instance;
                    if (result == null) {
                        instance = result = new IPSearcher(filepath);
                    }
                }
                return result;
            }
            return instance;
        }
        catch (DbMakerConfigException | IOException e) {
            throw new RuntimeException(e);
        }
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
