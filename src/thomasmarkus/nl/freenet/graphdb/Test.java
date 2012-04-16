package thomasmarkus.nl.freenet.graphdb;

import java.sql.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Test {

	public static H2Graph graph;
	
	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		
		graph = new H2Graph("Testing");

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

		
		Thread thread = new Thread(new Runnable()
		{
			public void run()
			{
				System.out.println("Adding some vertices...");
				for(int i=0; i < 100000; i++)
				{
					long edge_id;
					try {
						edge_id = graph.addEdge(1, 2);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		});

		
		Thread thread2 = new Thread(new Runnable()
		{
			public void run()
			{
				System.out.println("Adding some vertices...");
				for(int i=0; i < 100000; i++)
				{
					long edge_id;
					try {
						edge_id = graph.createVertex();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		});

		 
		thread.start();
		thread2.start();
		
		
		List<Long> result = graph.getAllVerticesWithProperty("id");
		
		System.out.println("number of results: " + result);
		
		
		
		//TODO: get vertices connected to some other vertex
		//TODO: get edge via some property-name pair
		//TODO: 
		
	}

}
