package com.tinkerpop.blueprints.pgm.impls.dup;

import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.impls.dup.util.DupPropertyDataBinding;

import java.util.Set;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public abstract class DupElement implements Element {
	
    final protected static DupPropertyDataBinding propertyDataBinding = new DupPropertyDataBinding();

    public abstract Object getId();
    
    public abstract Object getProperty(final String propertyKey);
    
    public abstract Set<String> getPropertyKeys();
    
    public abstract void setProperty(final String propertyKey, final Object value);
    
    public abstract Object removeProperty(final String propertyKey);

}
