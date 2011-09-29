package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class BdbEdgeKeyBinding extends TupleBinding<BdbEdgeKey> {

    public void objectToEntry(BdbEdgeKey object, TupleOutput to) {
    	to.writeLong(object.outId);
        to.writeLong(object.inId);
        to.writeString(object.label);
    }

    public BdbEdgeKey entryToObject(TupleInput ti) {
        BdbEdgeKey object = new BdbEdgeKey();
        object.outId = ti.readLong();
        object.inId = ti.readLong();
        object.label = ti.readString();
        return object;
    }
} 
