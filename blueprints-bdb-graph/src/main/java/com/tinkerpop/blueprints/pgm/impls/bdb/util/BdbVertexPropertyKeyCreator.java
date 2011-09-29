package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.db.SecondaryKeyCreator;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.SecondaryDatabase;

public class BdbVertexPropertyKeyCreator implements SecondaryKeyCreator {

    private BdbPrimaryKeyBinding binding;

    public BdbVertexPropertyKeyCreator(BdbPrimaryKeyBinding binding) {
        this.binding = binding;
    }

    public boolean createSecondaryKey(SecondaryDatabase secDb,
                                      DatabaseEntry keyEntry, 
                                      DatabaseEntry dataEntry,
                                      DatabaseEntry resultEntry) {

        BdbPrimaryKey key = binding.entryToObject(keyEntry);

        if (key.type != BdbPrimaryKey.VERTEX_PROPERTY)
            return false;

        LongBinding.longToEntry(key.id1, resultEntry);
        return true;
    }
} 
