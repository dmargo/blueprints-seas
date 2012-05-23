package com.tinkerpop.blueprints.pgm.impls.sql;

import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.impls.sql.util.*;

import java.sql.*;

/**
 * A Blueprints implementation of an SQL database (http://www.oracle.com)
 *
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class SqlGraph implements TransactionalGraph {
	private String addr = null;
	public Connection connection = null;
	
	// XXX The prepared statements need to be moved to thread-local storage or pools
	
    private final ThreadLocal<Boolean> tx = new ThreadLocal<Boolean>() {
        protected Boolean initialValue() {
            return false;
        }
    };
    private final ThreadLocal<Integer> txBuffer = new ThreadLocal<Integer>() {
        protected Integer initialValue() {
            return 1;
        }
    };
    private final ThreadLocal<Integer> txCounter = new ThreadLocal<Integer>() {
        protected Integer initialValue() {
            return 0;
        }
    };
    
    /**
     * Creates a new instance of a SqlGraph at directory.
     *
     * @param file The database environment's persistent file name.
     */
    public SqlGraph(final String addr) {
    	this.addr = addr;
    	
    	try {
        	Class.forName("com.mysql.jdbc.Driver").newInstance();
            this.connection = DriverManager.getConnection(
            		"jdbc:mysql:" + this.addr + "&autoDeserialize=true");     
            Statement statement = this.connection.createStatement();
            
            // First, some MySQL housekeeping.
            statement.executeUpdate(
            		"DROP PROCEDURE IF EXISTS create_index_if_not_exists");
        	statement.executeUpdate(
        			"CREATE PROCEDURE create_index_if_not_exists(theIndex VARCHAR(128), theTable VARCHAR(128), theColumn VARCHAR(129))\r\n" + 
        			"BEGIN\r\n" + 
        			"    IF NOT EXISTS (\r\n" + 
        			"        SELECT *\r\n" + 
        			"        FROM information_schema.statistics\r\n" + 
        			"        WHERE table_schema = database()\r\n" + 
        			"        AND table_name = theTable\r\n" + 
        			"        AND index_name = theIndex)\r\n" + 
        			"    THEN\r\n" + 
        			"        SET @s = CONCAT('CREATE INDEX ',theIndex,' ON ',theTable,' (',theColumn,')');\r\n" + 
        			"        PREPARE stmt FROM @s;\r\n" + 
        			"        EXECUTE stmt;\r\n" + 
        			"    END IF;\r\n" + 
        			"END;");
            
            // Create structure tables.
            statement.executeUpdate(
            		"create table if not exists vertex(" +
            			"vid serial primary key)");
            
            statement.executeUpdate(
            		"create table if not exists edge(" +
	            		"eid serial primary key," +
	            		"outid bigint unsigned not null references vertex(vid)," +
	            		"inid bigint unsigned not null references vertex(vid)," +
	            		"label varchar(255))");
            this.connection.prepareCall(
        			"call create_index_if_not_exists('outid_index', 'edge', 'outid')").executeUpdate();
            this.connection.prepareCall(
					"call create_index_if_not_exists('inid_index', 'edge', 'inid')").executeUpdate();
            this.connection.prepareCall(
					"call create_index_if_not_exists('label_index', 'edge', 'label')").executeUpdate();

            // Create property tables.
            statement.executeUpdate(
            		"create table if not exists vertexproperty(" +
	            		"vid bigint unsigned not null references vertex(vid)," +
	            		"pkey varchar(255) not null," +
	            		"value blob," +
	            		"primary key(vid,pkey))");
            //this.connection.prepareCall(
			//		"call create_index_if_not_exists('vertexpkey_index', 'vertexproperty', 'pkey')").executeUpdate();
            
            statement.executeUpdate(
            		"create table if not exists edgeproperty(" +
	            		"eid bigint unsigned not null references edge(eid)," +
	            		"pkey varchar(255) not null," +
	            		"value blob," +
	            		"primary key(eid,pkey))");
            //this.connection.prepareCall(
			//		"call create_index_if_not_exists('edgepkey_index', 'edgeproperty', 'pkey')").executeUpdate();
        	
        	// Create the shortest path procedure.
            statement.executeUpdate("DROP PROCEDURE IF EXISTS dijkstra");
        	statement.executeUpdate(
        			"CREATE PROCEDURE dijkstra(sourceid BIGINT UNSIGNED, targetid BIGINT UNSIGNED)\r\n" + 
        			"BEGIN\r\n" + 
        			"    DECLARE currid BIGINT UNSIGNED;\r\n" + 
        			"    DROP TEMPORARY TABLE IF EXISTS paths;\r\n" + 
        			"    CREATE TEMPORARY TABLE paths (\r\n" + 
        			"        vid BIGINT UNSIGNED NOT NULL PRIMARY KEY,\r\n" + 
        			"        calc TINYINT UNSIGNED NOT NULL,\r\n" + 
        			"        prev BIGINT UNSIGNED\r\n" + 
        			"    );\r\n" + 
        			"\r\n" + 
        			"    INSERT INTO paths (vid, calc, prev) VALUES (sourceid, 0, NULL);\r\n" + 
        			"    SET currid = sourceid;\r\n" + 
        			"    WHILE currid IS NOT NULL DO\r\n" + 
        			"    BEGIN\r\n" + 
        			"        INSERT IGNORE INTO paths (vid, calc, prev)\r\n" + 
        			"        SELECT inid, 0, currid\r\n" + 
        			"        FROM edge\r\n" + 
        			"        WHERE currid = outid;\r\n" + 
        			"\r\n" + 
        			"        UPDATE paths SET calc = 1 WHERE currid = vid;\r\n" + 
        			"\r\n" + 
        			"        IF EXISTS (SELECT vid FROM paths WHERE targetid = vid LIMIT 1)\r\n" + 
        			"        THEN\r\n" + 
        			"            SET currid = NULL;\r\n" + 
        			"        ELSE\r\n" + 
        			"            SET currid = (SELECT vid FROM paths WHERE calc = 0 LIMIT 1);\r\n" + 
        			"        END IF;\r\n" + 
        			"    END;\r\n" + 
        			"    END WHILE;\r\n" + 
        			"\r\n" + 
        			"    DROP TEMPORARY TABLE IF EXISTS result;\r\n" + 
        			"    CREATE TEMPORARY TABLE result (\r\n" + 
        			"        vid BIGINT UNSIGNED NOT NULL PRIMARY KEY\r\n" + 
        			"    );\r\n" + 
        			"\r\n" + 
        			"    SET currid = targetid;\r\n" + 
        			"    WHILE currid IS NOT NULL DO\r\n" + 
        			"    BEGIN\r\n" + 
        			"        INSERT INTO result (vid) VALUES (currid);\r\n" + 
        			"        SET currid = (SELECT prev FROM paths WHERE currid = vid LIMIT 1);\r\n" + 
        			"    END;\r\n" + 
        			"    END WHILE;\r\n" + 
        			"\r\n" + 
        			"    SELECT vid FROM result;\r\n" + 
        			"END;");
        	statement.close();
        	
        	
        	//
        	// Create prepared statements.
        	//
        	
        	// Vertices:
        	
        	this.addVertexStatement = this.connection.prepareStatement(
        			"insert into vertex values(default)",
        			Statement.RETURN_GENERATED_KEYS);
           	
        	this.getVertexStatement = this.connection.prepareStatement(
        			"select exists(select * from vertex where vid=?)");
           	
        	this.getMaxVertexIdStatement = this.connection.prepareStatement(
        			"select max(vid) from vertex;");
        	this.getVertexAfterStatement = this.connection.prepareStatement(
        			"select vid from vertex where vid >= ? order by vid limit 1;");
        	
        	this.countVerticesStatement = this.connection.prepareStatement(
        			"select count(*) from vertex;");
        	
        	this.removeVertexStatement = this.connection.prepareStatement(
        			"delete from vertex where vid=?");
        	this.removeVertexPropertiesStatement = this.connection.prepareStatement(
        			"delete from vertexproperty where vid=?");
        	
        	
        	// Vertex properties:
        	
        	this.getVertexPropertyStatement = this.connection.prepareStatement(
        			"select value from vertexproperty where vid=? and pkey=?");
        	
        	this.getVertexPropertyKeysStatement = this.connection.prepareStatement(
					"select pkey from vertexproperty where vid=?");
        	
        	this.setVertexPropertyStatement = this.connection.prepareStatement(
        			"replace into vertexproperty values(?,?,?)");
        	
        	this.removeVertexPropertyStatement = this.connection.prepareStatement(
    				"delete from vertexproperty where vid=? and pkey=?");
        	
        	
        	// Edges:
        	
        	this.getEdgeVerticesStatement = this.connection.prepareStatement(
					"select exists(select * from vertex where vid=?) " +
        			   "and exists(select * from vertex where vid=?)");
        	this.addEdgeStatement = this.connection.prepareStatement(
        			"insert into edge(outid,inid,label) values(?,?,?)");
        	
        	this.getEdgeStatement = this.connection.prepareStatement(
        			"select outid,inid,label from edge where eid=?");
           	
        	this.getMaxEdgeIdStatement = this.connection.prepareStatement(
        			"select max(eid) from edge;");
        	this.getEdgeAfterStatement = this.connection.prepareStatement(
        			"select eid,outid,inid,label from edge where eid >= ? order by eid limit 1;");
        	
        	this.countEdgesStatement = this.connection.prepareStatement(
        			"select count(*) from edge;");

        	this.removeEdgeStatement = this.connection.prepareStatement(
        			"delete from edge where eid=?");
        	this.removeEdgePropertiesStatement = this.connection.prepareStatement(
        			"delete from edgeproperty where eid=?");
        	
        	
        	// Edge properties:
        	
        	this.getEdgePropertyStatement = this.connection.prepareStatement(
        			"select value from edgeproperty where eid=? and pkey=?");
        	
        	this.getEdgePropertyKeysStatement = this.connection.prepareStatement(
        			"select pkey from edgeproperty where eid=?");
        	
        	this.setEdgePropertyStatement = this.connection.prepareStatement(
        			"replace into edgeproperty values(?,?,?)");
        	
        	this.removeEdgePropertyStatement = this.connection.prepareStatement(
        			"delete from edgeproperty where eid=? and pkey=?");
        			
        	
        	// Disable auto-commit.
        	
            connection.setAutoCommit(false);
            
       } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }        
    }

    // BLUEPRINTS GRAPH INTERFACE
    protected PreparedStatement addVertexStatement;
    public Vertex addVertex(final Object id) {        
        try {
            autoStartTransaction();
            final Vertex vertex = new SqlVertex(this);
            autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
            return vertex;
        } catch (RuntimeException e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }     
    }

    protected PreparedStatement getVertexStatement;
    public Vertex getVertex(final Object id) {
    	if (id == null)
    		throw new IllegalArgumentException("SqlGraph.getVertex(id) cannot be null.");
    	
    	try {
    		return new SqlVertex(this, id);
    	} catch (Exception e) {
    		return null;
    	}
    }

    public Iterable<Vertex> getVertices() {
        return new SqlVertexSequence(this);
    }
    
    protected PreparedStatement countVerticesStatement;
    public long countVertices() {
    	try {
			ResultSet rs = countVerticesStatement.executeQuery();
			rs.next();
			long r = rs.getLong(1);
			rs.close();
			return r;
    	}
    	catch (SQLException e) {
    		throw new RuntimeException(e);
    	}
    }
    
    protected PreparedStatement getMaxVertexIdStatement;
    protected PreparedStatement getVertexAfterStatement;
    public Vertex getRandomVertex() {
    	try {
    		return SqlVertex.getRandomVertex(this);
    	}
    	catch (SQLException e) {
    		throw new RuntimeException(e);
    	}
    }

    protected PreparedStatement removeVertexStatement;
    protected PreparedStatement removeVertexPropertiesStatement;
    public void removeVertex(final Vertex vertex) {
        if (vertex == null || vertex.getId() == null)
            return;
        
        try {
            autoStartTransaction();
            ((SqlVertex) vertex).remove();
            autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    protected PreparedStatement getVertexPropertyStatement;
    protected PreparedStatement getVertexPropertyKeysStatement;
    protected PreparedStatement setVertexPropertyStatement;
    protected PreparedStatement removeVertexPropertyStatement;

    protected PreparedStatement getEdgeVerticesStatement;
    protected PreparedStatement addEdgeStatement;
    public Edge addEdge(
		final Object id,
		final Vertex outVertex,
		final Vertex inVertex,
		final String label)
    {    	
        try {
            autoStartTransaction();
            final Edge edge = new SqlEdge(
        		this,
        		(SqlVertex) outVertex,
        		(SqlVertex) inVertex,
        		label);
            autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
            return edge;
        } catch (RuntimeException e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected PreparedStatement getEdgeStatement;
    public Edge getEdge(final Object id) {
    	if (id == null)
    		throw new IllegalArgumentException("SqlGraph.getEdge(id) cannot be null.");
    	
    	try {
    		return new SqlEdge(this, id);
    	} catch (Exception e) {
    		return null;
    	}
    }

    public Iterable<Edge> getEdges() {
        return new SqlEdgeSequence(this);
    }
    
    protected PreparedStatement countEdgesStatement;
    public long countEdges() {
    	try {
			ResultSet rs = countEdgesStatement.executeQuery();
			rs.next();
			long r = rs.getLong(1);
			rs.close();
			return r;
    	}
    	catch (SQLException e) {
    		throw new RuntimeException(e);
    	}
    }
    
    protected PreparedStatement getMaxEdgeIdStatement;
    protected PreparedStatement getEdgeAfterStatement; 
    public Edge getRandomEdge() {
    	try {
    		return SqlEdge.getRandomEdge(this);
    	}
    	catch (SQLException e) {
    		throw new RuntimeException(e);
    	}
    }

    protected PreparedStatement removeEdgeStatement;
    protected PreparedStatement removeEdgePropertiesStatement;
    public void removeEdge(final Edge edge) {
        if (edge == null || edge.getId() == null)
            return;
        
        try {
            autoStartTransaction();
            ((SqlEdge) edge).remove();
            autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    protected PreparedStatement getEdgePropertyStatement;
    protected PreparedStatement getEdgePropertyKeysStatement;
    protected PreparedStatement setEdgePropertyStatement;
    protected PreparedStatement removeEdgePropertyStatement;
    
    public Iterable<Vertex> getShortestPath(final Vertex source, final Vertex target) {
    	return new SqlVertexSequence(this, ((SqlVertex) source).vid, ((SqlVertex) target).vid);
    }

    public void clear() {
        try {
        	Statement statement = this.connection.createStatement();
        	statement.executeUpdate("delete from edgeproperty");
        	statement.executeUpdate("delete from vertexproperty");
        	statement.executeUpdate("delete from edge");
        	statement.executeUpdate("delete from vertex");
    		statement.close();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }   
    }
    
    public void delete() {
        try {
        	Statement statement = this.connection.createStatement();
        	statement.executeUpdate("drop table edgeproperty");
        	statement.executeUpdate("drop table vertexproperty");
        	statement.executeUpdate("drop table edge");
        	statement.executeUpdate("drop table vertex");
    		statement.close();
    		this.connection.close();
    	} catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }      
    }

    public void shutdown() {
        if (tx.get().booleanValue()) {
            try {
            	if (!connection.getAutoCommit()) connection.commit();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
            tx.set(false);
        }
        try {
    		this.connection.close();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }   
    }

    public String toString() {
    	try {
    		return "sqlgraph[" + this.addr + "]";
    	} catch (Exception e) {
    		return "sqlgraph[?]";
    	}
    }

    /* TRANSACTIONAL GRAPH INTERFACE */

    public void startTransaction() {
        if (!tx.get().booleanValue()) {
            txCounter.set(0);
            tx.set(true);
        } else
            throw new RuntimeException(TransactionalGraph.NESTED_MESSAGE);
    }

    protected void autoStartTransaction() {
        if (this.txBuffer.get() > 0) {
            if (!tx.get().booleanValue()) {
                tx.set(true);
                txCounter.set(0);
            }
        }
    }

    public void stopTransaction(final Conclusion conclusion) {
        if (!tx.get().booleanValue()) {
            txCounter.set(0);
            return;
        }

        try {
            if (conclusion == Conclusion.SUCCESS)
            	if (!connection.getAutoCommit()) connection.commit();
            else
            	if (!connection.getAutoCommit()) connection.rollback();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

        tx.set(false);
        txCounter.set(0);
    }

    protected void autoStopTransaction(final Conclusion conclusion) {
        if (this.txBuffer.get() > 0) {
            txCounter.set(txCounter.get() + 1);
            if (conclusion == Conclusion.FAILURE) {
                try {
                	if (!connection.getAutoCommit()) connection.rollback();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
                tx.set(false);
                txCounter.set(0);
            } else if (this.txCounter.get() % this.txBuffer.get() == 0) {
                try {
                	if (!connection.getAutoCommit()) connection.commit();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
                tx.set(false);
                txCounter.set(0);
            }
        }
    }

	@Override
	public int getCurrentBufferSize() {
		return txCounter.get();
	}

	@Override
	public int getMaxBufferSize() {
		return txBuffer.get();
	}

	@Override
	public void setMaxBufferSize(int size) {
        if (tx.get().booleanValue()) {
            try {
				if (!connection.getAutoCommit()) connection.commit();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
            tx.set(false);
        }
        this.txBuffer.set(size);
        this.txCounter.set(0);
	}


    /* INDEXABLE GRAPH INTERFACE

    public <T extends Element> Index<T> createManualIndex(final String indexName, final Class<T> indexClass) {
        if (this.manualIndices.containsKey(indexName))
            throw new RuntimeException("Index already exists: " + indexName);

        final BdbIndex index = new BdbIndex(this, indexName, indexClass, Index.Type.MANUAL); // Your constructor may vary.
        this.manualIndices.put(index.getIndexName(), index);

        return index;
    }

    public <T extends Element> AutomaticIndex<T> createAutomaticIndex(final String indexName, final Class<T> indexClass, final Set<String> indexKeys) {
        if (this.autoIndices.containsKey(indexName))
            throw new RuntimeException("Index already exists: " + indexName);

        final BdbAutomaticIndex index = new BdbAutomaticIndex<BdbElement>(this, indexName, (Class<BdbElement>) indexClass, indexKeys); // Your constructor may vary.
        this.autoIndices.put(index.getIndexName(), index);
        // You may also need to put into manualIndices depending on implementation.

        return index;
    }

    public <T extends Element> Index<T> getIndex(final String indexName, final Class<T> indexClass) {
        Index<?> index = this.manualIndices.get(indexName);
        if (null == index) {
            index = this.autoIndices.get(indexName);
            if (null == index)
                throw new RuntimeException("No such index exists: " + indexName);
        }

        if (indexClass.isAssignableFrom(index.getIndexClass()))
            return (Index<T>) index;
        else
            throw new RuntimeException("Can not convert " + index.getIndexClass() + " to " + indexClass);
    }

    public Iterable<Index<? extends Element>> getIndices() {
        final List<Index<? extends Element>> list = new ArrayList<Index<? extends Element>>();
        for (Index<?> index : this.manualIndices.values()) {
            list.add(index);
        }
        for (Index<?> index : this.autoIndices.values()) {
            list.add(index);
        }
        return list;
    }

    public void dropIndex(final String iIndexName) {
        if (this.manualIndices.remove(iIndexName) == null)
            this.autoIndices.remove(iIndexName);

        // And of course, drop the actual index from the raw graph.
    }

    */
    
}
