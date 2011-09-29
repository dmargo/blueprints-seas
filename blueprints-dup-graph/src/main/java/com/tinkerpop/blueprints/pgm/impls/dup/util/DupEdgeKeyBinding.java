package com.tinkerpop.blueprints.pgm.impls.dup.util;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class DupEdgeKeyBinding extends TupleBinding<DupEdgeKey> {

    public void objectToEntry(DupEdgeKey object, TupleOutput to) {
    	to.writeLong(object.out);
    	to.writeString(object.label);
    	to.writeLong(object.in);
    }

    public DupEdgeKey entryToObject(TupleInput ti) {
    	DupEdgeKey object = new DupEdgeKey();
    	object.out = ti.readLong();
    	object.label = ti.readString();
    	object.in = ti.readLong();
    	return object;
    }
} 
