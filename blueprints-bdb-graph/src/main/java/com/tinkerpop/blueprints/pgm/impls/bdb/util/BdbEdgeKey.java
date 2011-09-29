package com.tinkerpop.blueprints.pgm.impls.bdb.util;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class BdbEdgeKey {
    public long outId = -1;
    public long inId = -1;
    public String label = null;
    
    public String toString() {
    	return "bdbedgekey[" + outId + "." + inId + "." + label + "]";
    }
}
