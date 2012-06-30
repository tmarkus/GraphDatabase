package thomasmarkus.nl.freenet.graphdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.h2.jdbcx.JdbcConnectionPool;

public class H2GraphFactory {

	private JdbcConnectionPool cp = null;
	private Set<Connection> connections;
	
	public H2GraphFactory(String dbname) throws ClassNotFoundException, SQLException
	{
        Class.forName("org.h2.Driver");
		this.cp = JdbcConnectionPool.create("jdbc:h2:"+dbname+";LOCK_MODE=3;LOCK_TIMEOUT=30000", "sa", "");
        this.connections = new HashSet<Connection>();
		H2DB.checkDB(this.cp.getConnection());
	}
	
	public H2GraphFactory(String dbname, int max_connections) throws ClassNotFoundException, SQLException
	{
        this(dbname);
		this.cp.setMaxConnections(max_connections);
	}
	
	public int getActiveConnections()
	{
		return cp.getActiveConnections();
	}
	
	public H2Graph getGraph() throws SQLException
	{
		synchronized (connections) {
			removeClosedConnections();
			Connection con = this.cp.getConnection();
			connections.add(con);
			return new H2Graph(con);
		}
	}
	
	private void removeClosedConnections() throws SQLException
	{
		synchronized (connections) {
			Iterator<Connection> iter = connections.iterator();
			while(iter.hasNext())
			{
				Connection con = iter.next();
				if (con.isClosed()) iter.remove();
			}
		}
	}
	
	public void stop() throws SQLException
	{
		//compact the database further
		Connection compactConnection = this.cp.getConnection();
		compactConnection.prepareStatement("ANALYZE").execute();
		compactConnection.prepareStatement("SHUTDOWN COMPACT").execute();
		compactConnection.close();
		
		removeClosedConnections();

		synchronized (connections) {
			for(Connection con : connections)
			{
				con.close();
			}
		}
		
		this.cp.dispose();
		
	}
	
}
