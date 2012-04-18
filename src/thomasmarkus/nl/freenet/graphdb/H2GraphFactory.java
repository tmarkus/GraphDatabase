package thomasmarkus.nl.freenet.graphdb;

import java.sql.SQLException;

import org.h2.jdbcx.JdbcConnectionPool;

public class H2GraphFactory {

	private final JdbcConnectionPool cp;
	
	public H2GraphFactory(String dbname) throws ClassNotFoundException, SQLException
	{
		//open the database
        Class.forName("org.h2.Driver");
		this.cp = JdbcConnectionPool.create("jdbc:h2:"+dbname+";LOCK_MODE=1", "sa", "");
        this.cp.setMaxConnections(20);
        
        H2DB.checkDB(this.cp.getConnection());
	}
	
	public H2Graph getGraph() throws SQLException
	{
		return new H2Graph(this.cp.getConnection());
	}
	
	public void stop()
	{
		this.cp.dispose();
		
	}
	
}
