package com.tinkerpop.blueprints.pgm.impls.rdf.impls;

import com.tinkerpop.blueprints.pgm.impls.rdf.RdfGraph;
import org.openrdf.sail.memory.MemoryStore;

import java.io.File;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryStoreRdfGraph extends RdfGraph {

    public MemoryStoreRdfGraph() {
        super(new MemoryStore());
    }

    public MemoryStoreRdfGraph(final String dataDirectory) {
        super(new MemoryStore(new File(dataDirectory)));
    }
}


