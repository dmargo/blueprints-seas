package com.tinkerpop.blueprints.pgm.impls.dup.util;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class DupEdgeKey {
	public long out;
	public String label;
	public long in;
	
	public DupEdgeKey(final long out, final String label, final long in) {
		this.out = out;
		this.label = label;
		this.in = in;
	}
    
    public String toString() {
    	return "dupedgekey[" + this.out + ":" + this.label + ":" + this.in + "]";
    }
}
