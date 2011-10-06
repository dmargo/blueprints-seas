package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.db.DatabaseEntry;
import com.tinkerpop.blueprints.pgm.impls.bdb.BdbGraph;

import java.util.Comparator;

public class BdbPrimaryKeyComparator implements Comparator<byte[]> {

    public int compare(byte[] o1, byte[] o2) {	
    	BdbPrimaryKey key1 = BdbGraph.primaryKeyBinding.entryToObject(new DatabaseEntry(o1));
        BdbPrimaryKey key2 = BdbGraph.primaryKeyBinding.entryToObject(new DatabaseEntry(o2));

        // Sort first on id1.
        if (key1.id1 < key2.id1)
            return -1;
        if (key1.id1 > key2.id1)
            return 1;

        // If any record is of type VERTEX or VERTEX_PROPERTY, sort by type next.
        if (key1.type <= BdbPrimaryKey.VERTEX_PROPERTY || key2.type <= BdbPrimaryKey.VERTEX_PROPERTY) {
            if (key1.type < key2.type)
                return -1;
            if (key1.type > key2.type)
                return 1;
            
            // If type VERTEX, they are now equal; if VERTEX_PROPERTY; compare propertyKeys.
            if (key1.type == BdbPrimaryKey.VERTEX)
            	return 0;
            else
            	return key1.propertyKey.compareTo(key2.propertyKey);
        }

        // If we get here, both records are of type EDGE or EDGE_PROPERTY. Sort by id2.
        if (key1.id2 < key2.id2)
            return -1;
        if (key1.id2 > key2.id2)
            return 1;
        
        // Then, sort by label.
        int ret = key1.label.compareTo(key2.label);
        if (ret != 0)
        	return ret;

        // Then, sort by type.
        if (key1.type < key2.type)
            return -1;
        if (key1.type > key2.type)
            return 1;
        
        // If type EDGE, they are now equal; if EDGE_PROPERTY, compare propertyKeys.
        if (key1.type == BdbPrimaryKey.EDGE)
        	return 0;
        else
        	return key1.propertyKey.compareTo(key2.propertyKey);
    }
} 
