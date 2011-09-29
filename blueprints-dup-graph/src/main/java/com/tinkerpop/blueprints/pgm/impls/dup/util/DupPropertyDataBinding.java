package com.tinkerpop.blueprints.pgm.impls.dup.util;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class DupPropertyDataBinding extends TupleBinding<DupPropertyData> {

    public void objectToEntry(DupPropertyData object, TupleOutput to) {
    	to.writeString(object.pkey);
    	try {
    		new ObjectOutputStream(to).writeObject(object.value);
    	} catch (RuntimeException e) {
    		throw e;
    	} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
    	}
    }

    public DupPropertyData entryToObject(TupleInput ti) {
    	DupPropertyData object = new DupPropertyData();
    	object.pkey = ti.readString();
    	try {
        	object.value = new ObjectInputStream(ti).readObject();
    	} catch (RuntimeException e) {
    		throw e;
    	} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
    	}
    	return object;
    }
} 
