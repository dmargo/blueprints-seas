package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.db.SecondaryKeyCreator;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.SecondaryDatabase;
import com.tinkerpop.blueprints.pgm.impls.bdb.BdbEdge;
import com.tinkerpop.blueprints.pgm.impls.bdb.BdbGraph;

public class BdbEdgePropertyKeyCreator implements SecondaryKeyCreator {
	
    public boolean createSecondaryKey(SecondaryDatabase secDb,
                                      DatabaseEntry keyEntry, 
                                      DatabaseEntry dataEntry,
                                      DatabaseEntry resultEntry) {

        BdbPrimaryKey primaryKey = BdbGraph.primaryKeyBinding.entryToObject(keyEntry);

        if (primaryKey.type != BdbPrimaryKey.EDGE_PROPERTY)
            return false;

        BdbEdgeKey secondaryKey = new BdbEdgeKey();
        secondaryKey.outId = primaryKey.id1;
        secondaryKey.inId = primaryKey.id2;
        secondaryKey.label = primaryKey.label;
        BdbEdge.edgeKeyBinding.objectToEntry(secondaryKey, resultEntry);
        return true;
    }
} 
