package thomasmarkus.nl.freenet.graphdb;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class Core {

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		
		H2Graph graph = new H2Graph("~/Freenet/LCWoT");

		String ownIdentityID = "zALLY9pbzMNicVn280HYqS2UkK0ZfX5LiTcln-cLrMU,GoLpCcShPzp3lbQSVClSzY7CH9c9HTw0qRLifBYqywY,AQACAAE";
		graph.getVertexByPropertyValue("id", ownIdentityID);

		Set<Long> pool = new HashSet<Long>(graph.getVertexByPropertyValue("id", ownIdentityID));
		
		
		graph.getOutgoingEdgesWithProperty(0, "score");
		graph.getOutgoingEdgesWithProperty(1, "score");
		graph.getOutgoingEdgesWithProperty(2, "score");
		graph.getOutgoingEdgesWithProperty(3, "score");
		graph.getOutgoingEdgesWithProperty(4, "score");
		
		long start = System.currentTimeMillis();
		graph.getOutgoingEdgesWithProperty(1, "score");
		System.out.println(System.currentTimeMillis()-start);
		
		
		graph.shutdown();
		
		//TODO: get vertices connected to some other vertex
		//TODO: get edge via some property-name pair
		//TODO: 
		
	}

}
