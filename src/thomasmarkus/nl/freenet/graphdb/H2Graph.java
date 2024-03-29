package thomasmarkus.nl.freenet.graphdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class H2Graph {

	H2DB db;
	Connection con;
	
	public H2Graph(Connection con)
	{
		this.con = con;	
		db = new H2DB(con);
	}
	
	public Connection getConnection()
	{
		return this.con;
	}
	
	public void setAutoCommit(boolean enable) throws SQLException
	{
		this.con.setAutoCommit(enable);
	}
	
	public void commit() throws SQLException
	{
		this.con.commit();
	}
	
	public void close() throws SQLException
	{
		this.con.close();
	}
	
	public long createVertex() throws SQLException
	{
		return db.insertVertex();
	}
	
	public List<Long> getAllVerticesWithProperty(String name) throws SQLException
	{
		return db.getAllVerticesWithProperty(name);
	}
	
	public void addVertexProperty(long vertex_id, String name, String value) throws SQLException
	{
		db.insertVertexProperty(vertex_id, name, value);
	}
	
	public Map<String, List<String>> getVertexProperties(long vertex_id) throws SQLException
	{
		return db.getVertexProperties(vertex_id);
	}
	
	public Map<String, List<String>> getEdgeProperties(long edge_id) throws SQLException
	{
		return db.getEdgeProperties(edge_id);
	}

	/**
	 * @param names A list of property names that it will match disjunctively on
	 * @param value Minimum value
	 * @return An iterator for vertices with this property
	 * @throws SQLException
	 */
	
	public VertexIterator getVertices(List<String> names, int value, List<String> properties, Map<String, String> requiredProperties, boolean randomOrder, int limit) throws SQLException
	{
		return db.getVertices(names, value, properties, requiredProperties, randomOrder, limit);
	}

	public VertexIterator getVertices(String name, int value, String property) throws SQLException
	{
		List<String> names = new LinkedList<String>();
		names.add(name);

		List<String> properties = new LinkedList<String>();
		names.add(property);

		Map<String, String> requiredProperties = new HashMap<String, String>();
		
		return db.getVertices(names, value, properties, requiredProperties, false, Integer.MAX_VALUE);
	}

	public VertexIterator getVertices(String name, int value, String property, boolean randomOrder, int limit) throws SQLException
	{
		List<String> names = new LinkedList<String>();
		names.add(name);

		List<String> properties = new LinkedList<String>();
		properties.add(property);

		Map<String, String> requiredProperties = new HashMap<String, String>();
		
		return db.getVertices(names, value, properties, requiredProperties, false, Integer.MAX_VALUE);
	}

	
	
	public Set<Long> getVerticesWithPropertyValueLargerThan(String name, long value) throws SQLException
	{
		return db.getVertexWithPropertyValueLargerThan(name, value);
	}
	
	public void addEdgeProperty(long edge_id, String name, String value) throws SQLException
	{
		db.insertEdgeProperty(edge_id, name, value);
	}
	
	public void updateVertexProperty(long vertex_id, String name, String value) throws SQLException
	{
		db.updateVertexProperty(vertex_id, name, value);
	}

	public void removeVertexPropertyValue(long vertex_id, String name, String value) throws SQLException
	{
		db.removeVertexPropertyValue(vertex_id, name, value);
	}
	
	public void updateEdgeProperty(long edge_id, String name, String value) throws SQLException
	{
		db.updateEdgeProperty(edge_id, name, value);
	}
	
	public List<Long> getVertexByPropertyValue(String name, String value) throws SQLException
	{
		return db.getVertex(name, value);
	}

	public long addEdge(long vertex_from_id, long vertex_to_id) throws SQLException
	{
		return db.insertEdge(vertex_from_id, vertex_to_id);
	}

	public List<Edge> getOutgoingEdges(long vertex_id) throws SQLException
	{
		return db.getOutgoingEdges(vertex_id);
	}

	public List<Edge> getIncomingEdges(long vertex_id) throws SQLException
	{
		return db.getIncomingEdges(vertex_id);
	}

	
	public List<EdgeWithProperty> getOutgoingEdgesWithProperty(long vertex_id, String name) throws SQLException
	{
		return db.getOutgoingEdgesWithProperty(vertex_id, name);
	}

	public List<EdgeWithProperty> getIncomingEdgesWithProperty(long vertex_id, String name) throws SQLException
	{
		return db.getIncomingEdgesWithProperty(vertex_id, name);
	}
	
	public List<Edge> getEdgesByProperty(String name, String value) throws SQLException
	{
		return db.getEdgesByPropertyValue(name, value);
	}
	
	public long getEdgeByVerticesAndProperty(long vertex_from, long vertex_to, String name) throws SQLException
	{
		return db.getEdgeByVerticesAndProperty(vertex_from, vertex_to, name);
	}

	public String getEdgeValueByVerticesAndProperty(long vertex_from, long vertex_to, String name) throws SQLException
	{
		return db.getEdgeValueByVerticesAndProperty(vertex_from, vertex_to, name);
	}
	
	public void removeVertex(long vertex_id) throws SQLException
	{
		db.removeVertex(vertex_id);
	}
	
	public void removePropertyForAllVertices(String name) throws SQLException
	{
		db.removePropertyForAllVertices(name);
	}
	
	public void removeVertexProperty(long vertex_id, String name) throws SQLException
	{
		db.removeVertexProperty(vertex_id, name);
	}
	
	public void removeEdge(long edge_id) throws SQLException
	{
		db.removeEdge(edge_id);
	}
	
	public long getVertexCount() throws SQLException
	{
		return db.countVertices();
	}
	
	public long getEdgeCount() throws SQLException 
	{
		return db.countEdges();
	}
	
	
	public void shutdown() throws SQLException
	{
		db.close();
	}
}
