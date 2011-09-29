package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.db.SecondaryKeyCreator;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.SecondaryDatabase;

public class BdbEdgePropertyKeyCreator implements SecondaryKeyCreator {

    private BdbPrimaryKeyBinding primaryKeyBinding;
    private BdbEdgeKeyBinding edgeKeyBinding;

    public BdbEdgePropertyKeyCreator(BdbPrimaryKeyBinding primaryKeyBinding, BdbEdgeKeyBinding edgeKeyBinding) {
        this.primaryKeyBinding = primaryKeyBinding;
        this.edgeKeyBinding = edgeKeyBinding;
    }

    public boolean createSecondaryKey(SecondaryDatabase secDb,
                                      DatabaseEntry keyEntry, 
                                      DatabaseEntry dataEntry,
                                      DatabaseEntry resultEntry) {

        BdbPrimaryKey primaryKey = primaryKeyBinding.entryToObject(keyEntry);

        if (primaryKey.type != BdbPrimaryKey.EDGE_PROPERTY)
            return false;

        BdbEdgeKey secondaryKey = new BdbEdgeKey();
        secondaryKey.outId = primaryKey.id1;
        secondaryKey.inId = primaryKey.id2;
        secondaryKey.label = primaryKey.label;
        edgeKeyBinding.objectToEntry(secondaryKey, resultEntry);
        return true;
    }
} 
