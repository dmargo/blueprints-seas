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

    public int type;
    public long id1;
    public long id2;
    public String label;
    public String propertyKey;
    
    public BdbPrimaryKey() {}
    
    public BdbPrimaryKey(final long id1) {
    	this.type = BdbPrimaryKey.VERTEX;
    	this.id1 = id1;
    	this.id2 = 0;
    	this.label = null;
    	this.propertyKey = null;
    }
    
    public BdbPrimaryKey(final long id1, final String propertyKey) {
    	this.type = BdbPrimaryKey.VERTEX_PROPERTY;
    	this.id1 = id1;
    	this.id2 = 0;
    	this.label = null;
    	this.propertyKey = propertyKey;
    }
    
    public BdbPrimaryKey(final long id1, final long id2, final String label) {
    	this.type = BdbPrimaryKey.EDGE;
    	this.id1 = id1;
    	this.id2 = id2;
    	this.label = label;
    	this.propertyKey = null;
    }
    
    public BdbPrimaryKey(final long id1, final long id2, final String label, final String propertyKey) {
    	this.type = BdbPrimaryKey.EDGE_PROPERTY;
    	this.id1 = id1;
    	this.id2 = id2;
    	this.label = label;
    	this.propertyKey = propertyKey;
    }
}
