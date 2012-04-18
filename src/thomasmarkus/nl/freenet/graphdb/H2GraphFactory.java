package thomasmarkus.nl.freenet.graphdb;

import java.sql.SQLException;

import org.h2.jdbcx.JdbcConnectionPool;

public class H2GraphFactory {

	private JdbcConnectionPool cp;
	
	public H2GraphFactory(String dbname) throws ClassNotFoundException, SQLException
	{
		//open the database
        Class.forName("org.h2.Driver");
		cp = JdbcConnectionPool.create("jdbc:h2:"+dbname, "sa", "");
        cp.setMaxConnections(20);
        
        H2DB.checkDB(cp.getConnection());
	}
	
	public H2Graph getGraph() throws SQLException
	{
		return new H2Graph(cp.getConnection());
	}
	
	public void stop()
	{
		cp.dispose();
	}
	
}
