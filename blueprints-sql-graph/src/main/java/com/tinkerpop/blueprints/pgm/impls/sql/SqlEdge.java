package com.tinkerpop.blueprints.pgm.impls.sql;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.*;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class SqlEdge extends SqlElement implements Edge {
	private SqlGraph graph = null;
	
	protected long eid = -1;
	protected long outId = -1;
	protected long inId = -1;
	protected String label = null;
	
    protected SqlEdge(
		final SqlGraph graph,
		final SqlVertex outVertex,
		final SqlVertex inVertex,
		final String label) throws SQLException
    {
    	// Note: There is no need to verify in and out vertex existence,
    	// since the database integrity constrains already take care of it.
 
		// Insert a new edge record.
    	
    	PreparedStatement addEdgeStatement = graph.addEdgeStatement.get();
		addEdgeStatement.setLong(1, outVertex.vid);
		addEdgeStatement.setLong(2, inVertex.vid);
		addEdgeStatement.setString(3, label);
		addEdgeStatement.executeUpdate();
		ResultSet rs = addEdgeStatement.getGeneratedKeys();
		
		rs.next();
		this.eid = rs.getLong(1);
		rs.close();
				
		this.graph = graph;
		this.outId = outVertex.vid;
		this.inId = inVertex.vid;
		this.label = label;
    }

    protected SqlEdge(final SqlGraph graph, final Object id) throws SQLException {
    	if(!(id instanceof Long))
    		throw new IllegalArgumentException("SqlGraph: " + id + " is not a valid Edge ID.");
    	
    	this.eid = ((Long) id).longValue();
    	
    	PreparedStatement getEdgeStatement = graph.getEdgeStatement.get();
		getEdgeStatement.setLong(1, this.eid);
		ResultSet rs = getEdgeStatement.executeQuery();
		
		if (rs.next()) {
			this.outId = rs.getLong(1);
			this.inId = rs.getLong(2);
			this.label = rs.getString(3);
		}
		rs.close();
		
		if (this.outId == -1)
			throw new RuntimeException("SqlGraph: Edge " + id + " does not exist.");
		
		this.graph = graph;
    } 
    
    public SqlEdge(
		final SqlGraph graph,
		final long eid,
		final long outId,
		final long inId,
		final String label)
    {
    	this.graph = graph;
    	this.eid = eid;
    	this.outId = outId;
    	this.inId = inId;
    	this.label = label;
    }
    
    public static SqlEdge getRandomEdge(final SqlGraph graph) throws SQLException {
    	
		ResultSet rs = graph.getMaxEdgeIdStatement.get().executeQuery();
		rs.next();
		long max = rs.getLong(1);
		rs.close();
    	
    	// XXX Might fail if the last edge has been removed
		
		PreparedStatement getEdgeAfterStatement = graph.getEdgeAfterStatement.get();
		getEdgeAfterStatement.setLong(1, (long) (Math.random() * max));
		rs = getEdgeAfterStatement.executeQuery();
		if (!rs.next()) {
			rs.close();
			getEdgeAfterStatement.setLong(1, 0);
			rs = getEdgeAfterStatement.executeQuery();
			if (!rs.next()) {
				rs.close();
				throw new NoSuchElementException();
			}
		}
		long id = rs.getLong(1);
		long outId = rs.getLong(2);
		long inId = rs.getLong(3);
		String label = rs.getString(4);
		rs.close();

		return new SqlEdge(graph, id, outId, inId, label);
    }
    
    protected void remove() throws SQLException {
    	PreparedStatement removeEdgePropertiesStatement = graph.removeEdgePropertiesStatement.get();
    	PreparedStatement removeEdgeStatement = graph.removeEdgeStatement.get();
    	
    	removeEdgePropertiesStatement.setLong(1, this.eid);
    	removeEdgePropertiesStatement.executeUpdate();
    	
    	removeEdgeStatement.setLong(1, this.eid);
    	removeEdgeStatement.executeUpdate();
		
		this.label = null;
        this.inId = -1;
        this.outId = -1;
        this.eid = -1;
        this.graph = null;
    }

    public Object getId() {
    	return this.eid != -1 ? new Long(this.eid) : null;
    }

    public Vertex getOutVertex() {
    	try {
    		return this.outId != -1 ? new SqlVertex(graph, new Long(this.outId)) : null;
    	} catch (RuntimeException e) {
    		throw e;
    	} catch (Exception e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    }

    public Vertex getInVertex() {
    	try {
	    	return this.inId != -1 ? new SqlVertex(graph, new Long(this.inId)) : null;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
    }
    
    public String getLabel() {
    	return this.label;
    }

    public Object getProperty(final String propertyKey) {
        Object result = null;
    	
    	try {
    		PreparedStatement getEdgePropertyStatement = graph.getEdgePropertyStatement.get();
    		getEdgePropertyStatement.setLong(1, this.eid);
    		getEdgePropertyStatement.setString(2, propertyKey);
    		ResultSet rs = getEdgePropertyStatement.executeQuery();

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
			PreparedStatement getEdgePropertyKeysStatement = graph.getEdgePropertyKeysStatement.get();
			getEdgePropertyKeysStatement.setLong(1, this.eid);
			ResultSet rs = getEdgePropertyKeysStatement.executeQuery();
			
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
    	if (propertyKey == null || propertyKey.equals("id") || propertyKey.equals("label"))
    		throw new RuntimeException("SqlGraph: Invalid propertyKey.");
    	
        try {
        	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        	ObjectOutputStream oos = new ObjectOutputStream(baos);
        	oos.writeObject(value);
        	
            graph.autoStartTransaction();
        	
            PreparedStatement setEdgePropertyStatement = graph.setEdgePropertyStatement.get();
        	setEdgePropertyStatement.setLong(1, this.eid);
        	setEdgePropertyStatement.setString(2, propertyKey);
        	setEdgePropertyStatement.setBytes(3, baos.toByteArray());
        	setEdgePropertyStatement.executeUpdate();
        	
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
        	
        	PreparedStatement removeEdgePropertyStatement = graph.removeEdgePropertyStatement.get();
        	removeEdgePropertyStatement.setLong(1, this.eid);
        	removeEdgePropertyStatement.setString(2, propertyKey);
        	removeEdgePropertyStatement.executeUpdate();
        	
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

        final SqlEdge other = (SqlEdge) obj;
        return (
    		this.eid == other.eid &&
        	this.outId == other.outId &&
        	this.inId == other.inId &&
        	this.label.equals(other.label));
    }

    public int hashCode() {
    	return (
			new Long(eid).hashCode() ^
    		new Long(outId).hashCode() ^
    		new Long(inId).hashCode() ^
    		label.hashCode());
    }
    
    public String toString() {
        return StringFactory.edgeString(this);
    }

}
