package com.tinkerpop.blueprints.pgm.impls.dup.util;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class DupPropertyData {
	public String pkey = null;
	public Object value = null;
    
    public String toString() {
    	return "duppropertydata[" + this.pkey + ":" + this.value + "]";
    }
}
