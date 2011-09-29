package com.tinkerpop.blueprints.pgm.impls.bdb.util;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class BdbPrimaryKey {
    //XXX dmargo: For now these are statics instead of an enum, because a
    // primitive is easier to pack into Bdb than a class.
    public static final int VERTEX = 0;
    public static final int VERTEX_PROPERTY = 1;
    public static final int EDGE = 2;
    public static final int EDGE_PROPERTY = 3;

    public int type = -1;
    public long id1 = -1;
    public long id2 = -1;
    public String label = null;
    public String propertyKey = null;
}
