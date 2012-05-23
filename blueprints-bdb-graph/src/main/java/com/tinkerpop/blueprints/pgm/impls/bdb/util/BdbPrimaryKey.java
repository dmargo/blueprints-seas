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
    public static final int RANDOM_COMPARISON = 4;		// for getting a random key
    
    public static final BdbPrimaryKey RANDOM = new BdbPrimaryKey(0, 0, null, null, RANDOM_COMPARISON);

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
    
    protected BdbPrimaryKey(final long id1, final long id2, final String label, final String propertyKey, final int type) {
    	this.type = type;
    	this.id1 = id1;
    	this.id2 = id2;
    	this.label = label;
    	this.propertyKey = propertyKey;
    }

    @Override
    public String toString() {
        switch (type) {
            case VERTEX: return "[Vertex " + id1 + "]";
            case VERTEX_PROPERTY: return "[Vertex " + id1 + " Property " + propertyKey + "]";
            case EDGE: return "[Edge " + id1 + "--" + id2 + " " + label + "]";
            case EDGE_PROPERTY: return "[Edge " + id1 + "--" + id2 + " " + label + " Property " + propertyKey + "]";
            case RANDOM_COMPARISON: return "[Random]";
            default: return "[Unknown Type " + type + " " + id1 + "--" + id2 + " " + label + " " + propertyKey + "]";
        }
    }
}
