package thomasmarkus.nl.freenet.graphdb;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Test {

	public static H2Graph graph;
	
	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		final H2GraphFactory gf = new H2GraphFactory("~/Freenet/LCWoT");
		
		graph = gf.getGraph();

		String ownIdentityID = "zALLY9pbzMNicVn280HYqS2UkK0ZfX5LiTcln-cLrMU,GoLpCcShPzp3lbQSVClSzY7CH9c9HTw0qRLifBYqywY,AQACAAE";
		graph.getVertexByPropertyValue("id", ownIdentityID);
		
		long start = System.currentTimeMillis();
		graph.getOutgoingEdgesWithProperty(1, "score");
		System.out.println(System.currentTimeMillis()-start);

		
		Thread thread = new Thread(new Runnable()
		{
			public void run()
			{
				try
				{
					H2Graph graph = gf.getGraph();
					System.out.println("Adding some vertices...");
					for(int i=0; i < 100000; i++)
					{
						long edge_id;
						edge_id = graph.addEdge(1, 2);
					}
				}
				catch(SQLException e)
				{
					
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

		 
		//thread.start();
		//thread2.start();
		
		
		List<Long> result = graph.getAllVerticesWithProperty("id");
		
		System.out.println("number of results: " + result);
		
		
		
		System.out.println("Trying smart query... ");
		List<String> names = new LinkedList<String>();
		names.add("trust.zALLY9pbzMNicVn280HYqS2UkK0ZfX5LiTcln-cLrMU");
		
		List<String> properties = new LinkedList<String>();
		properties.add("name");
		properties.add("edition");
		
		Map<String, String> requiredProperties = new HashMap<String, String>();
		requiredProperties.put("contextName", "Sone");
		requiredProperties.put("name", "digger3");
		
		long startTime = System.currentTimeMillis();
		
		VertexIterator resultIterator = graph.getVertices(names, -1, properties, requiredProperties, false, 100);
		
		System.out.println("This took: " + (System.currentTimeMillis() - startTime));
		
		
		while(resultIterator.hasNext())
		{
			Map<String, List<String>> vertex = resultIterator.next();
			
			System.out.println(vertex.keySet());
			System.out.println(vertex.get("name").get(0));
			
		}
		
		
		graph.close();
		gf.stop();
		
		//TODO: get vertices connected to some other vertex
		//TODO: get edge via some property-name pair
		//TODO: 
		
	}

}
