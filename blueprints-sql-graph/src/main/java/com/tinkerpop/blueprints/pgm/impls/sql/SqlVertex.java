package com.tinkerpop.blueprints.pgm.impls.sql;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import com.tinkerpop.blueprints.pgm.impls.sql.util.SqlEdgeSequence;

import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.*;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class SqlVertex extends SqlElement implements Vertex {
    private SqlGraph graph = null;
    protected long vid = -1;

    protected SqlVertex(final SqlGraph graph) throws SQLException {
		graph.addVertexStatement.executeUpdate();
		
		ResultSet rs = graph.addVertexStatement.getGeneratedKeys();
		rs.next();
		this.vid = rs.getLong(1);
		rs.close();

		this.graph = graph;
    }

    protected SqlVertex(final SqlGraph graph, final Object id) throws SQLException {
    	if(!(id instanceof Long))
    		throw new IllegalArgumentException("SqlGraph: " + id + " is not a valid Vertex ID.");
    	
    	this.vid = ((Long) id).longValue();
    	
		graph.getVertexStatement.setLong(1, this.vid);
		ResultSet rs = graph.getVertexStatement.executeQuery();	
		
		rs.next();
		boolean exists = rs.getBoolean(1);
		rs.close();

		if(!exists)
			throw new RuntimeException("SqlGraph: Vertex " + id + " does not exist.");
		
		this.graph = graph;
    }
    
    public SqlVertex(final SqlGraph graph, final long vid) {
    	this.graph = graph;
    	this.vid = vid;
    }
    
    public static SqlVertex getRandomVertex(final SqlGraph graph) throws SQLException {
    	
		ResultSet rs = graph.getMaxVertexIdStatement.executeQuery();
		rs.next();
		long max = rs.getLong(1);
		rs.close();

		graph.getVertexAfterStatement.setLong(1, (long) (Math.random() * max));
		rs = graph.getVertexAfterStatement.executeQuery();
		if (!rs.next()) {
			rs.close();
			graph.getVertexAfterStatement.setLong(1, 0);
			rs = graph.getVertexAfterStatement.executeQuery();
			if (!rs.next()) {
				rs.close();
				throw new NoSuchElementException();
			}
		}
		long id = rs.getLong(1);
		rs.close();

		return new SqlVertex(graph, id);
    }

    protected void remove() throws SQLException {
    	// Remove linked edges.
        for (Edge e : this.getInEdges())
        	((SqlEdge) e).remove();
    	for (Edge e : this.getOutEdges())
            ((SqlEdge) e).remove();

    	// Remove properties and vertex.
    	this.graph.removeVertexPropertiesStatement.setLong(1, this.vid);
    	this.graph.removeVertexPropertiesStatement.executeUpdate();
    	
    	this.graph.removeVertexStatement.setLong(1, this.vid);
    	this.graph.removeVertexStatement.executeUpdate();

        this.vid = -1;
        this.graph = null;
    }
    
    public Object getId() {
    	return this.vid != -1 ? new Long(this.vid) : null;
    }

    public Iterable<Edge> getOutEdges(final String... labels) {
    	if (labels.length == 0)
        	return new SqlEdgeSequence(this.graph, this.vid, true);
    	else
    		return new SqlEdgeSequence(this.graph, this.vid, true, labels);
    }

    public Iterable<Edge> getInEdges(final String... labels) {
    	if (labels.length == 0)
        	return new SqlEdgeSequence(this.graph, this.vid, false);
    	else
    		return new SqlEdgeSequence(this.graph, this.vid, false, labels);
    }

    public Object getProperty(final String propertyKey) {
        Object result = null;
    	
    	try {
    		this.graph.getVertexPropertyStatement.setLong(1, this.vid);
    		this.graph.getVertexPropertyStatement.setString(2, propertyKey);
    		ResultSet rs = this.graph.getVertexPropertyStatement.executeQuery();

        	if (rs.next()) {
        		ObjectInputStream ois = new ObjectInputStream(rs.getBinaryStream(1));
        		result = ois.readObject();
        	}
        	
        	rs.close();
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		return result;
    }

    public Set<String> getPropertyKeys() {
		Set<String> result = new HashSet<String>();
		
		try {
			this.graph.getVertexPropertyKeysStatement.setLong(1, this.vid);
			ResultSet rs = this.graph.getVertexPropertyKeysStatement.executeQuery();
			
			while (rs.next())
				result.add(rs.getString(1));
			
			rs.close();
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		return result;
    }
    
    public void setProperty(final String propertyKey, final Object value) {
    	if (propertyKey == null || propertyKey.equals("id"))
    		throw new RuntimeException("SqlGraph: Invalid propertyKey.");
    	
        try {
        	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        	ObjectOutputStream oos = new ObjectOutputStream(baos);
        	oos.writeObject(value);
        	
            graph.autoStartTransaction();

        	this.graph.setVertexPropertyStatement.setLong(1, this.vid);
        	this.graph.setVertexPropertyStatement.setString(2, propertyKey);
        	this.graph.setVertexPropertyStatement.setBytes(3, baos.toByteArray());
        	this.graph.setVertexPropertyStatement.executeUpdate();
    		        	
        	graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Object removeProperty(final String propertyKey) {
    	Object result = null;
    	
        try {
            graph.autoStartTransaction();
        	
        	result = this.getProperty(propertyKey);
        	
        	this.graph.removeVertexPropertyStatement.setLong(1, this.vid);
        	this.graph.removeVertexPropertyStatement.setString(2, propertyKey);
        	this.graph.removeVertexPropertyStatement.executeUpdate();
        	
            graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
        
        return result;
    }
    
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        final SqlVertex other = (SqlVertex) obj;
        return (this.vid == other.vid);
    }

    public int hashCode() {
        return (new Long(this.vid)).hashCode();
    }
    
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
