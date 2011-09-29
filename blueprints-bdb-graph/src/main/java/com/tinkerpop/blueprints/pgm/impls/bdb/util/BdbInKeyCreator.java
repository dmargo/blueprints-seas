package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.db.SecondaryKeyCreator;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.SecondaryDatabase;

public class BdbInKeyCreator implements SecondaryKeyCreator {

    private BdbPrimaryKeyBinding binding;

    public BdbInKeyCreator(BdbPrimaryKeyBinding binding) {
        this.binding = binding;
    }

    public boolean createSecondaryKey(SecondaryDatabase secDb,
                                      DatabaseEntry keyEntry, 
                                      DatabaseEntry dataEntry,
                                      DatabaseEntry resultEntry) {

        BdbPrimaryKey key = binding.entryToObject(keyEntry);

        if (key.type != BdbPrimaryKey.EDGE)
            return false;

        LongBinding.longToEntry(key.id2, resultEntry);
        return true;
    }
} 
