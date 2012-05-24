package com.tinkerpop.blueprints.pgm.impls.dup.util;

import java.util.Comparator;

public class DupRecordNumberComparator implements Comparator<byte[]> {
	
	public static final long RANDOM = -1;

    public int compare(byte[] o1, byte[] o2) {
    	
    	// Handle random entry
    	
    	if (o1[o1.length-1] == (byte) 0xff && o1[0] == (byte) 0xff) {
    		for (int i = o1.length - 1; i >= 0; i--) {
    			if (o1[i] == (byte) 0xff) {
    				if (i == 0) return Math.random() < 0.5 ? -1 : 1; 
    			}
    			else break;
    		}
    	}
    	
    	if (o2[o2.length-1] == (byte) 0xff && o2[0] == (byte) 0xff) {
    		for (int i = o2.length - 1; i >= 0; i--) {
    			if (o2[i] == (byte) 0xff) {
    				if (i == 0) return Math.random() < 0.5 ? -1 : 1; 
    			}
    			else break;
    		}
    	}
    	
    	// Regular comparison
    		
    	if (o1.length != o2.length) return o1.length - o2.length;
    	for (int i = o1.length - 1; i >= 0; i--) {
    		int r = (int) (o1[i] & 0xff) - (int) (o2[i] & 0xff);
    		if (r != 0) return r;
    	}
    	return 0;
    }
}
