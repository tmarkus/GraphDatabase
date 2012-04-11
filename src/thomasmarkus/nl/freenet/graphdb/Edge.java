package thomasmarkus.nl.freenet.graphdb;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Edge {

	H2DB db;
	
	public Edge(H2DB db)
	{
		this.db = db;
	}
	
	public Map<String, List<String>> getProperties()
	{
		try {
			return db.getEdgeProperties(id);
		} catch (SQLException e) {
		}	
		return null;
	}
	
	public String getProperty(String name)
	{
		try {
			return db.getEdgeProperty(id, name);
		} catch (SQLException e) {
		}	
		return null;
	}
	
	
	public long id = -1;
	public long vertex_from;
	public long vertex_to;
}
