package com.tinkerpop.blueprints.pgm.impls.dup.util;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class DupEdgeData {
	public String label;
	public long id;
	
	public DupEdgeData(final String label, final long id) {
		this.label = label;
		this.id = id;
	}

    public String toString() {
    	return "dupedgedata[" + this.label + ":" + this.id + "]";
    }
}
