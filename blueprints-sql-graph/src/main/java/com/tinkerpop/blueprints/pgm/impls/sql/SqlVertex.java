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
		graph.addVertexStatement.get().executeUpdate();
		
		ResultSet rs = graph.addVertexStatement.get().getGeneratedKeys();
		rs.next();
		this.vid = rs.getLong(1);
		rs.close();

		this.graph = graph;
    }

    protected SqlVertex(final SqlGraph graph, final Object id) throws SQLException {
    	if(!(id instanceof Long))
    		throw new IllegalArgumentException("SqlGraph: " + id + " is not a valid Vertex ID.");
    	
    	this.vid = ((Long) id).longValue();
    	
    	PreparedStatement getVertexStatement = graph.getVertexStatement.get();
    	getVertexStatement.setLong(1, this.vid);
		ResultSet rs = getVertexStatement.executeQuery();	
		
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
    	
		ResultSet rs = graph.getMaxVertexIdStatement.get().executeQuery();
		rs.next();
		long max = rs.getLong(1);
		rs.close();

		PreparedStatement getVertexAfterStatement = graph.getVertexAfterStatement.get();
		getVertexAfterStatement.setLong(1, (long) (Math.random() * max));
		rs = getVertexAfterStatement.executeQuery();
		if (!rs.next()) {
			rs.close();
			getVertexAfterStatement.setLong(1, 0);
			rs = getVertexAfterStatement.executeQuery();
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
    	PreparedStatement removeVertexPropertiesStatement = graph.removeVertexPropertiesStatement.get();
    	PreparedStatement removeVertexStatement = graph.removeVertexStatement.get();
    	
    	removeVertexPropertiesStatement.setLong(1, this.vid);
    	removeVertexPropertiesStatement.executeUpdate();
    	
    	removeVertexStatement.setLong(1, this.vid);
    	removeVertexStatement.executeUpdate();

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
    		PreparedStatement getVertexPropertyStatement = graph.getVertexPropertyStatement.get();
    		getVertexPropertyStatement.setLong(1, this.vid);
    		getVertexPropertyStatement.setString(2, propertyKey);
    		ResultSet rs = getVertexPropertyStatement.executeQuery();

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
			PreparedStatement getVertexPropertyKeysStatement = graph.getVertexPropertyKeysStatement.get();
			getVertexPropertyKeysStatement.setLong(1, this.vid);
			ResultSet rs = getVertexPropertyKeysStatement.executeQuery();
			
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

            PreparedStatement setVertexPropertyStatement = graph.setVertexPropertyStatement.get();
        	setVertexPropertyStatement.setLong(1, this.vid);
        	setVertexPropertyStatement.setString(2, propertyKey);
        	setVertexPropertyStatement.setBytes(3, baos.toByteArray());
        	setVertexPropertyStatement.executeUpdate();
    		        	
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
        	
        	PreparedStatement removeVertexPropertyStatement = graph.removeVertexPropertyStatement.get();
        	removeVertexPropertyStatement.setLong(1, this.vid);
        	removeVertexPropertyStatement.setString(2, propertyKey);
        	removeVertexPropertyStatement.executeUpdate();
        	
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
