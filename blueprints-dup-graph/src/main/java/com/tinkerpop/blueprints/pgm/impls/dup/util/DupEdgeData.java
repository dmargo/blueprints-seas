package com.tinkerpop.blueprints.pgm.impls.dup.util;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class DupEdgeData {
	public String label = null;
	public long id = 0;

    public String toString() {
    	return "dupedgedata[" + this.label + ":" + this.id + "]";
    }
}
