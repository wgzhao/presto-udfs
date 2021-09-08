package com.wgzhao.presto.udfs.utils;

import me.ihxq.projects.pna.PhoneNumberInfo;
import me.ihxq.projects.pna.PhoneNumberLookup;
import me.ihxq.projects.pna.algorithm.BinarySearchAlgorithmImpl;
import me.ihxq.projects.pna.algorithm.LookupAlgorithm;

/**
 * Created by Administrator on 2020/3/25.
 */
public class MobileSearcher
{
    private static final Object mutex = new Object();
    private static volatile MobileSearcher instance;
    private final PhoneNumberLookup phoneNumberLookup;

    private MobileSearcher()
    {
        this(new BinarySearchAlgorithmImpl());
    }

    private MobileSearcher(LookupAlgorithm lookupAlgorithm)
    {
        this.phoneNumberLookup = new PhoneNumberLookup(lookupAlgorithm);
    }

    public static MobileSearcher getInstance()
    {
        MobileSearcher result = instance;
        if (result == null) {
            synchronized (mutex) {
                result = instance;
                if (result == null) {
                    instance = result = new MobileSearcher();
                }
            }
            return result;
        }
        return instance;
    }

    public PhoneNumberInfo lookup(String phoneNumber)
    {
        return this.phoneNumberLookup.lookup(phoneNumber).orElse(null);
    }
}
