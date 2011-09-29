package com.tinkerpop.blueprints.pgm.impls.rdf;

import org.openrdf.model.*;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.tinkerpop.blueprints.pgm.impls.sail.SailTokens;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RdfHelper {
    public static final Pattern literalPattern =
		Pattern.compile("^\"(.*?)\"((\\^\\^<(.+?)>)$|(@(.{2}))$)");
    
    protected static void addStatement(
		final Resource subject,
		final URI predicate,
		final Value object,
		final Resource context,
		final SailConnection sailConnection)
    {
    	try {
	    	if (null != context) {
	            sailConnection.addStatement(subject, predicate, object, context);
	        } else {
	            sailConnection.addStatement(subject, predicate, object);
	        }
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    }

    protected static void addStatement(final Statement statement, final SailConnection sailConnection) {
    	addStatement(
			statement.getSubject(),
			statement.getPredicate(),
			statement.getObject(),
			statement.getContext(),
			sailConnection);
    }
    
    protected static void removeStatement(
		final Resource subject,
		final URI predicate,
		final Value object,
		final Resource context,
		final SailConnection sailConnection)
    {
        try {
	        if (null != context) {
	        	sailConnection.removeStatements(subject, predicate, object, context);
	        } else {
	        	sailConnection.removeStatements(subject, predicate, object);
	        }
        } catch (SailException e) {
        	throw new RuntimeException(e.getMessage(), e);
        }
    }
    

    protected static void removeStatement(final Statement statement, final SailConnection sailConnection) {
    	removeStatement(
			statement.getSubject(),
			statement.getPredicate(),
			statement.getObject(),
			statement.getContext(),
			sailConnection);
    }
    
    public static boolean isBNode(final String resource) {
        return
    		resource.length() > 2 &&
    		resource.startsWith(SailTokens.BLANK_NODE_PREFIX);
    }

    public static boolean isLiteral(final String resource) {
        return (
    		literalPattern.matcher(resource).matches() || (
				resource.startsWith("\"") &&
				resource.endsWith("\"") &&
				resource.length() > 1));
    }

    public static boolean isURI(final String resource) {
        return
    		!isBNode(resource) &&
    		!isLiteral(resource) && (
				resource.contains(":") ||
				resource.contains("/") ||
				resource.contains("#"));
    }

    public static Literal makeLiteral(final String resource, SailConnection sailConnection) {
        final Matcher matcher = literalPattern.matcher(resource);
        if (matcher.matches()) {
            if (null != matcher.group(4))
                return new LiteralImpl(
            		matcher.group(1),
            		new URIImpl(RdfGraph.prefixToNamespace(matcher.group(4), sailConnection)));
            else
                return new LiteralImpl(matcher.group(1), matcher.group(6));
        } else {
            if (resource.startsWith("\"") && resource.endsWith("\"") && resource.length() > 1) {
                return new LiteralImpl(resource.substring(1, resource.length() - 1));
            } else {
                return null;
            }
        }
    }
}
