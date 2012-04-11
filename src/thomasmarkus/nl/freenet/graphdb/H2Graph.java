package thomasmarkus.nl.freenet.graphdb;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class H2Graph {


	H2DB db;
	
	public H2Graph(String dbname) throws ClassNotFoundException, SQLException
	{
			db = new H2DB(dbname);
	}
	
	public long createVertex() throws SQLException
	{
		return db.insertVertex();
	}
	
	public void addVertexProperty(long vertex_id, String name, String value) throws SQLException
	{
		db.insertVertexProperty(vertex_id, name, value);
	}
	
	public Map<String, List<String>> getVertexProperties(long vertex_id) throws SQLException
	{
		return db.getVertexProperties(vertex_id);
	}
	
	public Map<String, List<String>> getEdeProperties(long edge_id) throws SQLException
	{
		return db.getEdgeProperties(edge_id);
	}
	
	public List<Long> getVerticesWithPropertyValueLargerThan(String name, long value) throws SQLException
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
	
	public void removeVertex(long vertex_id) throws SQLException
	{
		db.removeVertex(vertex_id);
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
