package com.tinkerpop.blueprints.pgm.impls.sql.util;

import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.sql.SqlGraph;
import com.tinkerpop.blueprints.pgm.impls.sql.SqlVertex;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.sql.*;

/**
 * @author Elaine Angelino (http://www.eecs.harvard.edu/~elaine)
 */
public class SqlVertexSequence implements Iterable<Vertex>, Iterator<Vertex> {

    private SqlGraph graph = null;
    private Statement statement = null;
    private ResultSet rs = null;
    private boolean hasNext = false;
    
    public SqlVertexSequence(final SqlGraph graph) {
        try {
        	this.statement = graph.connection.createStatement();
        	this.rs = this.statement.executeQuery("select * from vertex");
        	this.hasNext = this.rs.next();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        this.graph = graph;
    }
    
    public SqlVertexSequence(final SqlGraph graph, final long source, final long target) {
    	PreparedStatement statement = null;
    	
    	try {
    		statement = graph.connection.prepareCall("call dijkstra(?,?)");
    		statement.setLong(1, source);
    		statement.setLong(2, target);
    		this.rs = statement.executeQuery();
    		this.hasNext = this.rs.next();
    	} catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    	
    	this.statement = statement;
    	this.graph = graph;
    }
	
	public Vertex next() {
		Vertex result = null;
		
        try {
        	if (this.hasNext) {
        		result = new SqlVertex(this.graph, this.rs.getLong(1));
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

    public Iterator<Vertex> iterator() {
        return this;
    }
}