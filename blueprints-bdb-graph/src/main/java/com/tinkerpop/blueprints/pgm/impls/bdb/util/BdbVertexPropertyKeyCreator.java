package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.db.SecondaryKeyCreator;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.SecondaryDatabase;
import com.tinkerpop.blueprints.pgm.impls.bdb.BdbGraph;

public class BdbVertexPropertyKeyCreator implements SecondaryKeyCreator {

    public boolean createSecondaryKey(SecondaryDatabase secDb,
                                      DatabaseEntry keyEntry, 
                                      DatabaseEntry dataEntry,
                                      DatabaseEntry resultEntry) {

        BdbPrimaryKey key = BdbGraph.primaryKeyBinding.entryToObject(keyEntry);

        if (key.type != BdbPrimaryKey.VERTEX_PROPERTY)
            return false;

        LongBinding.longToEntry(key.id1, resultEntry);
        return true;
    }
} 
