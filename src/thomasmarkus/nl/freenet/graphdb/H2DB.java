package thomasmarkus.nl.freenet.graphdb;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class H2DB {

	private Connection con;
	private PreparedStatement ps_outgoing_edges_with_property;
	private PreparedStatement ps_incoming_edges_with_property;
	
	public H2DB(String dbname) throws SQLException, ClassNotFoundException
	{
		//open the database
        Class.forName("org.h2.Driver");
        this.con = DriverManager.getConnection("jdbc:h2:"+dbname, "sa", "");
        con.setAutoCommit(true);
        checkDB();
        setupPreparedStatements();
	}
	
	private void setupPreparedStatements() throws SQLException
	{
		ps_outgoing_edges_with_property = con.prepareStatement(
				"SELECT value, id, vertex_to_id FROM edge_properties, edges " +
				"WHERE " +
				"edge_id IN (SELECT id FROM edges WHERE vertex_from_id = ?) AND " +
				"edges.id = edge_id AND " +
				"name = ?");

		ps_incoming_edges_with_property = con.prepareStatement(
				"SELECT value, id, vertex_from_id FROM edge_properties, edges " +
				"WHERE " +
				"edge_id IN (SELECT id FROM edges WHERE vertex_to_id = ?) AND " +
				"edges.id = edge_id AND " +
				"name = ?");
	}
	
	public void checkDB() throws SQLException
	{
        Statement stmt = con.createStatement();

		//check whether it contains the right tables
        DatabaseMetaData dbm = con.getMetaData();
		ResultSet tables = dbm.getTables(null, null, null, new String[] {"TABLE"});
		
		if (!tables.next()) {
			System.out.println("Creating tables...");
			
			//create the table
            stmt.execute("CREATE TABLE vertices " +
					"(id IDENTITY PRIMARY KEY)");

            stmt.execute("CREATE TABLE edges " +
					"(" +
					"	id IDENTITY PRIMARY KEY," +
					"	vertex_from_id LONG, " +
					"	vertex_to_id LONG " +
					")");
            
            stmt.execute("CREATE INDEX vertex_from_index ON edges(vertex_from_id)");
            stmt.execute("CREATE INDEX vertex_to_index ON edges(vertex_to_id)");
            
            stmt.execute(	"CREATE TABLE vertex_properties " +
            				" (" +
            				"	vertex_id LONG, name VARCHAR(255), value VARCHAR(255), value_number INT" +
            				")"
            		); 

            stmt.execute("CREATE INDEX vertex_properties_vertex_id_index ON vertex_properties(vertex_id)");
            stmt.execute("CREATE INDEX vertex_properties_name_index ON vertex_properties(name)");
            stmt.execute("CREATE INDEX vertex_properties_value_index ON vertex_properties(value)");
            stmt.execute("CREATE INDEX vertex_properties_valuenumber_index ON vertex_properties(value_number)");
            
            stmt.execute(	"CREATE TABLE edge_properties " +
    				" (" +
    				"	edge_id LONG, name VARCHAR(255), value VARCHAR(255), value_number INT " +
    				")"
    		); 

            stmt.execute("CREATE INDEX edge_properties_edge_id ON edge_properties(edge_id)");
            stmt.execute("CREATE INDEX edge_properties_name_index ON edge_properties(name)");
            stmt.execute("CREATE INDEX edge_properties_value_index ON edge_properties(value)");
            stmt.execute("CREATE INDEX edge_properties_valuenumber_index ON edge_properties(value_number)");
            
            //foreign key constraints
            stmt.execute("ALTER TABLE edges ADD FOREIGN KEY (vertex_from_id) REFERENCES vertices(ID) ON DELETE CASCADE");
            stmt.execute("ALTER TABLE edges ADD FOREIGN KEY (vertex_to_id) REFERENCES vertices(ID) ON DELETE CASCADE");
            stmt.execute("ALTER TABLE vertex_properties ADD FOREIGN KEY (vertex_id) REFERENCES vertices(ID) ON DELETE CASCADE");
            stmt.execute("ALTER TABLE edge_properties ADD FOREIGN KEY (edge_id) REFERENCES edges(ID) ON DELETE CASCADE");
		}
	}

	public void close() throws SQLException
	{
        con.close();
	}

	public long insertVertex() throws SQLException
	{
		Statement st = con.createStatement();
		st.executeUpdate("INSERT into vertices () VALUES () ", Statement.RETURN_GENERATED_KEYS);
		ResultSet results = st.getGeneratedKeys();
		
		if (results.next())
		{
			return results.getLong(1); 	
		}

		throw new SQLException("Could not retrieve latest vertices.id");
	}

	public List<Long> getAllVerticesWithProperty(String name) throws SQLException
	{
		PreparedStatement statement = con.prepareStatement("SELECT DISTINCT vertex_id FROM vertex_properties WHERE name = ?");
		statement.setString(1, name);
		ResultSet resultSet = statement.executeQuery();
		
		List<Long> result = new LinkedList<Long>();
		while(resultSet.next())
		{
			result.add(resultSet.getLong("vertex_id"));
		}
		return result;
	}
	
	public void insertVertexProperty(long vertex_id, String name, String value) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("INSERT INTO vertex_properties (vertex_id, name, value, value_number) VALUES (?, ?, ?, ?)");
		ps.setLong(1, vertex_id);
		ps.setString(2, name);
		ps.setString(3, value);
		
		try
		{
			ps.setInt(4, Integer.parseInt(value));	
		}
		catch(NumberFormatException e)
		{
			ps.setNull(4, java.sql.Types.INTEGER);
		}
		
		ps.execute();
	}

	public void updateVertexProperty(long vertex_id, String name, String value) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("DELETE FROM vertex_properties WHERE vertex_id = ? AND name = ?");
		ps.setLong(1, vertex_id);
		ps.setString(2, name);
		ps.execute();

		insertVertexProperty(vertex_id, name, value);
	}
	
	public void updateEdgeProperty(long edge_id, String name, String value) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("DELETE FROM edge_properties WHERE edge_id = ? AND name = ?");
		ps.setLong(1, edge_id);
		ps.setString(2, name);
		ps.execute();

		insertEdgeProperty(edge_id, name, value);
	}
	
	public void insertEdgeProperty(long edge_id, String name, String value) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("INSERT INTO edge_properties (edge_id, name, value, value_number) VALUES (?, ?, ?, ?)");
		ps.setLong(1, edge_id);
		ps.setString(2, name);
		ps.setString(3, value);
		try
		{
			ps.setInt(4, Integer.parseInt(value));	
		}
		catch(NumberFormatException e)
		{
			ps.setNull(4, java.sql.Types.INTEGER);
		}
		ps.execute();
	}

	public Map<String, List<String>> getEdgeProperties(long edge_id) throws SQLException
	{
		//lookup the properties for this edge and add them to the object
		PreparedStatement ps_props = con.prepareStatement("SELECT name, value FROM edge_properties WHERE edge_id = ?");
		ps_props.setLong(1, edge_id);
		ResultSet propertiesValues = ps_props.executeQuery();
	
		Map<String, List<String>> properties = new HashMap<String, List<String>>();
		while(propertiesValues.next())
		{
			String name = propertiesValues.getString("name");
			String value = propertiesValues.getString("value");
			
			if (!properties.containsKey(name))	properties.put(name, new LinkedList<String>());
			properties.get(name).add(value);
		}
		return properties;
	}
	
	public String getEdgeProperty(long edge_id, String name) throws SQLException
	{
		//lookup the properties for this edge and add them to the object
		PreparedStatement ps_props = con.prepareStatement("SELECT value FROM edge_properties WHERE edge_id = ? AND name = ? LIMIT 1");
		ps_props.setLong(1, edge_id);
		ps_props.setString(2, name);
		ResultSet propertiesValues = ps_props.executeQuery();
	
		while(propertiesValues.next())
		{
			return propertiesValues.getString("value");
		}
		return null;
	}


	public List<Edge> getOutgoingEdges(long vertex_from_id) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("SELECT id, vertex_to_id FROM edges WHERE vertex_from_id = ?");
		ps.setLong(1, vertex_from_id);

		List<Edge> edges = new LinkedList<Edge>();
		ResultSet results = ps.executeQuery();
		while(results.next())
		{
			Edge edge = new Edge(this);
			edge.id = results.getLong("id");
			edge.vertex_from = vertex_from_id;
			edge.vertex_to = results.getLong("vertex_to_id");
			edges.add(edge);
		}

		return edges;
	}

	public List<Edge> getIncomingEdges(long vertex_to_id) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("SELECT id, vertex_from_id FROM edges WHERE vertex_to_id = ?");
		ps.setLong(1, vertex_to_id);

		List<Edge> edges = new LinkedList<Edge>();
		ResultSet results = ps.executeQuery();
		while(results.next())
		{
			Edge edge = new Edge(this);
			edge.id = results.getLong("id");
			edge.vertex_from = results.getLong("vertex_from_id");
			edge.vertex_to = vertex_to_id;
			edges.add(edge);
		}
		return edges;
	}
	
	public List<EdgeWithProperty> getOutgoingEdgesWithProperty(long vertex_from_id, String name) throws SQLException
	{
		ps_outgoing_edges_with_property.setLong(1, vertex_from_id);
		ps_outgoing_edges_with_property.setString(2, name);

		List<EdgeWithProperty> edges = new LinkedList<EdgeWithProperty>();
		ResultSet results = ps_outgoing_edges_with_property.executeQuery();
		while(results.next())
		{
			EdgeWithProperty edge = new EdgeWithProperty();
			edge.id = results.getLong("id");
			edge.vertex_from = vertex_from_id;
			edge.vertex_to = results.getLong("vertex_to_id");
			edge.value = results.getString("value");
			edges.add(edge);
		}
		return edges;
	}

	public List<EdgeWithProperty> getIncomingEdgesWithProperty(long vertex_to_id, String name) throws SQLException
	{
		ps_incoming_edges_with_property.setLong(1, vertex_to_id);
		ps_incoming_edges_with_property.setString(2, name);

		List<EdgeWithProperty> edges = new LinkedList<EdgeWithProperty>();
		ResultSet results = ps_outgoing_edges_with_property.executeQuery();
		while(results.next())
		{
			EdgeWithProperty edge = new EdgeWithProperty();
			edge.id = results.getLong("id");
			edge.vertex_from = vertex_to_id;
			edge.vertex_to = results.getLong("vertex_to_id");
			edge.value = results.getString("value");
			edges.add(edge);
		}
		return edges;
	}

	
	public List<Edge> getEdgesByPropertyValue(String name, String value) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("SELECT edge_id, vertex_from_id, vertex_to_id FROM edge_properties, edges WHERE name = ? AND value = ? AND edge_id = edges.id");
		ps.setString(1, name);
		ps.setString(2, value);

		List<Edge> edges = new LinkedList<Edge>();
		ResultSet results = ps.executeQuery();
		while(results.next())
		{
			Edge edge = new Edge(this);
			edge.id = results.getLong("edge_id");
			edge.vertex_from = results.getLong("vertex_from_id");
			edge.vertex_to = results.getLong("vertex_to_id");
			edges.add(edge);
		}
		return edges;
	}
	
	public long getEdgeByVerticesAndProperty(long vertex_from, long vertex_to, String name) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("SELECT edge_id FROM edge_properties, edges WHERE vertex_from_id = ? AND vertex_to_id = ? AND name = ? AND edge_id = edges.id");
		ps.setLong(1, vertex_from);
		ps.setLong(2, vertex_to);
		ps.setString(3, name);

		ResultSet results = ps.executeQuery();
		if (results.next())
		{
			return results.getLong("edge_id");
		}
		else
		{
			throw new SQLException("Could not retrieve edge with these vertices and property name");
		}
	}

	public String getEdgeValueByVerticesAndProperty(long vertex_from, long vertex_to, String name) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("SELECT value FROM edge_properties, edges WHERE vertex_from_id = ? AND vertex_to_id = ? AND name = ? AND edge_id = edges.id");
		ps.setLong(1, vertex_from);
		ps.setLong(2, vertex_to);
		ps.setString(3, name);

		ResultSet results = ps.executeQuery();
		if (results.next())
		{
			return results.getString("value");
		}
		else
		{
			throw new SQLException("Could not retrieve edge value with these vertices and property name");
		}
	}

	
	
	public List<Long> getVertex(String name, String value) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("SELECT vertex_id FROM vertex_properties WHERE name = ? AND value = ?");
		ps.setString(1, name);
		ps.setString(2, value);

		List<Long> vertices = new LinkedList<Long>();
		ResultSet results = ps.executeQuery();
		while(results.next())
		{
			vertices.add(results.getLong("vertex_id"));
		}
		return vertices;
	}

	public List<Long> getVertexWithPropertyValueLargerThan(String name, long value) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("SELECT vertex_id FROM vertex_properties WHERE name = ? AND value_number > ?");
		ps.setString(1, name);
		ps.setLong(2, value);
		
		List<Long> vertices = new LinkedList<Long>();
		ResultSet results = ps.executeQuery();
		while(results.next())
		{
			vertices.add(results.getLong("vertex_id"));
		}
		return vertices;
	}
	
	public Map<String, List<String>> getVertexProperties(long vertex_id) throws SQLException
	{
		//lookup the properties for this edge and add them to the object
		PreparedStatement ps_props = con.prepareStatement("SELECT * FROM vertex_properties WHERE vertex_id = ? ORDER BY name");
		ps_props.setLong(1, vertex_id);
		ResultSet propertiesValues = ps_props.executeQuery();
	
		Map<String, List<String>> properties = new HashMap<String, List<String>>();
		while(propertiesValues.next())
		{
			String name = propertiesValues.getString("name");
			String value = propertiesValues.getString("value");
			if (!properties.containsKey(name))	properties.put(name, new LinkedList<String>());
			properties.get(name).add(value);
		}
		return properties;
	}
	
	
	public long insertEdge(long vertex_from_id, long vertex_to_id) throws SQLException {

		final PreparedStatement ps = con.prepareStatement("INSERT INTO edges (vertex_from_id, vertex_to_id) VALUES (?, ?)", PreparedStatement.RETURN_GENERATED_KEYS);
		
		ps.setLong(1, vertex_from_id);
		ps.setLong(2, vertex_to_id);
		ps.executeUpdate();

		final ResultSet results = ps.getGeneratedKeys();
		if (results.next())
		{
			return results.getLong(1);	
		}
		throw new SQLException("Could not retrieve latest edges.id");

	}

	public void removeVertex(long vertex_id) throws SQLException
	{
		final PreparedStatement ps = con.prepareStatement("DELETE FROM vertices WHERE id = ?");
		ps.setLong(1, vertex_id);
		ps.execute();
	}
	
	public void removePropertyForAllVertices(String name) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("DELETE FROM vertex_properties WHERE name = ?");
		ps.setString(1, name);
		ps.execute();
	}
	
	public void removeVertexProperty(long vertex_id, String name) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("DELETE FROM vertex_properties WHERE vertex_id = ? AND name = ?");
		ps.setLong(1, vertex_id);
		ps.setString(2, name);
		ps.execute();
	}

	public void removeVertexPropertyValue(long vertex_id, String name, String value) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("DELETE FROM vertex_properties WHERE vertex_id = ? AND name = ? AND value = ?");
		ps.setLong(1, vertex_id);
		ps.setString(2, name);
		ps.setString(3, value);
		ps.execute();
	}

	
	public void removeEdge(long edge_id) throws SQLException
	{
		PreparedStatement ps = con.prepareStatement("DELETE FROM edges WHERE id = ?");
		ps.setLong(1, edge_id);
		ps.execute();
	}
	
	
	public Long countVertices() throws SQLException
	{
		//lookup the properties for this edge and add them to the object
		PreparedStatement ps_props = con.prepareStatement("SELECT COUNT(*) AS vertex_count FROM vertices");
		ResultSet count = ps_props.executeQuery();

		if (count.next())
		{
			return count.getLong("vertex_count");
		}
		else
		{
			return -1l;
		}
	}

	public Long countEdges() throws SQLException
	{
		//lookup the properties for this edge and add them to the object
		PreparedStatement ps_props = con.prepareStatement("SELECT COUNT(*) AS edge_count FROM edges");
		ResultSet count = ps_props.executeQuery();

		if (count.next())
		{
			return count.getLong("edge_count");
		}
		else
		{
			return -1l;
		}
	}

	
}
