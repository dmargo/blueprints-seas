package com.tinkerpop.blueprints.pgm.impls.bdb.util;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class BdbEdgeKey {
    public long outId;
    public long inId;
    public String label;
    
    public BdbEdgeKey(final long outId, final long inId, final String label) {
    	this.outId = outId;
    	this.inId = inId;
    	this.label = label;
    }
    
    public String toString() {
    	return "bdbedgekey[" + outId + "." + inId + "." + label + "]";
    }
}
