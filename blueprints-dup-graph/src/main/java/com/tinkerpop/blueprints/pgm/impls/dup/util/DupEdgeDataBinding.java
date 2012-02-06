package com.tinkerpop.blueprints.pgm.impls.dup.util;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class DupEdgeDataBinding extends TupleBinding<DupEdgeData> {

    public void objectToEntry(DupEdgeData object, TupleOutput to) {
    	to.writeString(object.label);
    	to.writeLong(object.id);
    }

    public DupEdgeData entryToObject(TupleInput ti) {
    	DupEdgeData object = new DupEdgeData(ti.readString(), ti.readLong());
    	return object;
    }
} 
