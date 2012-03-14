package com.tinkerpop.blueprints.pgm.impls.hollow;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class HollowEdge extends HollowElement implements Edge {
	private HollowGraph graph = null;
	
	protected long eid = -1;
	protected long outId = -1;
	protected long inId = -1;
	protected String label = null;
	
    protected HollowEdge(
		final HollowGraph graph,
		final HollowVertex outVertex,
		final HollowVertex inVertex,
		final String label)
    {
    	this.label = label;
    	this.inId = inVertex.vid;
    	this.outId = outVertex.vid;
    	this.eid = graph.edgeCount++;
    	this.graph = graph;
    }

    protected HollowEdge(final HollowGraph graph, final Object id){
    	if(!(id instanceof Long))
    		throw new IllegalArgumentException("HollowGraph: " + id + " is not a valid Edge ID.");
    	
    	this.label = "";
    	this.inId = (long) graph.rand.nextDouble() * graph.vertexCount;
    	this.outId = (long) graph.rand.nextDouble() * graph.vertexCount;
    	this.eid = ((Long) id).longValue();
		this.graph = graph;
    } 
    
    public HollowEdge(
		final HollowGraph graph,
		final long eid,
		final long outId,
		final long inId,
		final String label)
    {
    	this.label = label;
    	this.inId = inId;
    	this.outId = outId;
    	this.eid = eid;
    	this.graph = graph;
    }

    protected void remove() {
		this.label = null;
        this.inId = -1;
        this.outId = -1;
        this.eid = -1;
        this.graph.edgeCount--;
        this.graph = null;
    }

    public Object getId() {
    	return this.eid != -1 ? new Long(this.eid) : null;
    }

    public Vertex getOutVertex() {
    	try {
    		return this.outId != -1 ? new HollowVertex(graph, new Long(this.outId)) : null;
    	} catch (RuntimeException e) {
    		throw e;
    	} catch (Exception e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    }

    public Vertex getInVertex() {
    	try {
	    	return this.inId != -1 ? new HollowVertex(graph, new Long(this.inId)) : null;
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
    	return new Object();
    }

    public Set<String> getPropertyKeys() {
		return new HashSet<String>();
    }
    
    public void setProperty(final String propertyKey, final Object value) {}

    public Object removeProperty(final String propertyKey) {
    	return new Object();
    }
    
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        final HollowEdge other = (HollowEdge) obj;
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
