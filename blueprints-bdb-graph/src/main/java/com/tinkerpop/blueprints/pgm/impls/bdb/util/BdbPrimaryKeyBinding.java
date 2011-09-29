package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class BdbPrimaryKeyBinding extends TupleBinding<BdbPrimaryKey> {

    public void objectToEntry(BdbPrimaryKey object, TupleOutput to) {
        to.writeByte(object.type);
        to.writeLong(object.id1);
        if (object.type == BdbPrimaryKey.EDGE || object.type == BdbPrimaryKey.EDGE_PROPERTY) {
            to.writeLong(object.id2);
            to.writeString(object.label);
        }
        if (object.type == BdbPrimaryKey.VERTEX_PROPERTY || object.type == BdbPrimaryKey.EDGE_PROPERTY)
            to.writeString(object.propertyKey);
    }

    public BdbPrimaryKey entryToObject(TupleInput ti) {
        BdbPrimaryKey object = new BdbPrimaryKey();

        object.type = ti.readByte();
        object.id1 = ti.readLong();
        if (object.type == BdbPrimaryKey.EDGE || object.type == BdbPrimaryKey.EDGE_PROPERTY) {
            object.id2 = ti.readLong();
            object.label = ti.readString();
        }
        if (object.type == BdbPrimaryKey.VERTEX_PROPERTY || object.type == BdbPrimaryKey.EDGE_PROPERTY)
            object.propertyKey = ti.readString();

        return object;
    }
} 
