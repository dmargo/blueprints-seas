package com.tinkerpop.blueprints.pgm.impls.dup.util;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class DupEdgeKey {
	public long out = 0;
	public String label = null;
	public long in = 0;
    
    public String toString() {
    	return "dupedgekey[" + this.out + ":" + this.label + ":" + this.in + "]";
    }
}
