package com.tinkerpop.blueprints.pgm.impls.sql.util;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.impls.sql.SqlGraph;
import com.tinkerpop.blueprints.pgm.impls.sql.SqlEdge;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.sql.*;

/**
 * @author Elaine Angelino (http://www.eecs.harvard.edu/~elaine)
 */
public class SqlEdgeSequence implements Iterable<Edge>, Iterator<Edge> {

    private SqlGraph graph = null;
    private Statement statement = null;
    private ResultSet rs = null;
    private boolean hasNext = false;
    
    public SqlEdgeSequence(final SqlGraph graph) {
        try {
        	this.statement = graph.connection.createStatement();
        	this.rs = this.statement.executeQuery("select * from edge");
        	this.hasNext = this.rs.next();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        this.graph = graph;
    }
    
    public SqlEdgeSequence(final SqlGraph graph, final long vid, final boolean getOut) {
        try {
        	this.statement = graph.connection.createStatement();
        	if (getOut)
        		this.rs = this.statement.executeQuery("select * from edge where outid=" + vid);
        	else
        		this.rs = this.statement.executeQuery("select * from edge where inid=" + vid);
        	this.hasNext = this.rs.next();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        this.graph = graph;
    }
    
    public SqlEdgeSequence(final SqlGraph graph, final long vid, final boolean getOut, final String[] labels) {
    	StringBuilder sb = new StringBuilder("'");
    	sb.append(labels[0]);
    	sb.append("'");
    	for (int i = 1; i < labels.length; ++i) {
    		sb.append(',');
    		sb.append("'");
    		sb.append(labels[i]);
    		sb.append("'");
    	}	

        try {
        	this.statement = graph.connection.createStatement();
        	if (getOut)
        		this.rs = this.statement.executeQuery("select * from edge where outid=" + vid + " and label in (" + sb + ")");
        	else
        		this.rs = this.statement.executeQuery("select * from edge where inid=" + vid + " and label in (" + sb + ")");
        	this.hasNext = this.rs.next();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        this.graph = graph;
    }
	
	public Edge next() {
		Edge result = null;
		
        try {
        	if (this.hasNext) {
        		result = new SqlEdge(
        				this.graph,
        				this.rs.getLong(1),
        				this.rs.getLong(2),
        				this.rs.getLong(3),
        				this.rs.getString(4));
        		this.hasNext = this.rs.next();
        	} else {
        		this.rs.close();
        		this.statement.close();
        	}
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        if (result == null)
        	throw new NoSuchElementException();
        
        return result;
	}

	public boolean hasNext() {
		if (!this.hasNext) {
	        try {
        		this.rs.close();
        		this.statement.close();
	        } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
		}
    	return this.hasNext;
    }
	
	public void close() {
        try {
        	this.rs.close();
        	this.statement.close();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
	}

    public void remove() { 
        throw new UnsupportedOperationException(); 
    } 

    public Iterator<Edge> iterator() {
        return this;
    }
}