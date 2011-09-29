package com.tinkerpop.blueprints.pgm.impls.sql;

import com.tinkerpop.blueprints.pgm.Element;

import java.util.Set;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public abstract class SqlElement implements Element {

    public abstract Object getId();
    
    public abstract Object getProperty(final String propertyKey);
    
    public abstract Set<String> getPropertyKeys();
    
    public abstract void setProperty(final String propertyKey, final Object value);
    
    public abstract Object removeProperty(final String propertyKey);

}
