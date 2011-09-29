package com.tinkerpop.blueprints.pgm.impls.rdf.impls;

import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NativeStoreRdfGraphTest extends TestCase {

    public void testConstructNativeStore() {
        new NativeStoreRdfGraph("/tmp/rdfgraph");
    }
}
