/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
// Addtional Libraries
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement();

		// issues the update instruction
		stmt.executeUpdate(sql);

		// close the instruction
	    stmt.close();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName() +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Plane");
				System.out.println("2. Add Pilot");
				System.out.println("3. Add Flight");
				System.out.println("4. Add Technician");
				System.out.println("5. Book Flight");
				System.out.println("6. List number of available seats for a given flight.");
				System.out.println("7. List total number of repairs per plane in descending order");
				System.out.println("8. List total number of repairs per year in ascending order");
				System.out.println("9. Find total number of passengers with a given status");
				System.out.println("10. < EXIT");
				
				switch (readChoice()){
					case 1: AddPlane(esql); break;
					case 2: AddPilot(esql); break;
					case 3: AddFlight(esql); break;
					case 4: AddTechnician(esql); break;
					case 5: BookFlight(esql); break;
					case 6: ListNumberOfAvailableSeats(esql); break;
					case 7: ListsTotalNumberOfRepairsPerPlane(esql); break;
					case 8: ListTotalNumberOfRepairsPerYear(esql); break;
					case 9: FindPassengersCountWithStatus(esql); break;
					case 10: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static boolean hasAtomocity(Statement stmt, String flightQueryUpdate, String infoQueryUpdate)
	throws SQLException
	{
		int errorCounter = 0;

		try
		{
			stmt.executeUpdate(flightQueryUpdate);
		}
		catch(SQLException e)
		{
			System.out.print("ERROR: " + e.getMessage());
			errorCounter++;
		}

		if (errorCounter == 0) 
		{
			try 
			{

				stmt.executeUpdate(infoQueryUpdate);
			}
			catch(SQLException e)
			{
				System.out.print("ERROR: " + e.getMessage());
				errorCounter++;
			}
		}


		if(errorCounter > 0)
			return false;
		else
			return true;

	}

	public static boolean hasID(Statement stmt, String table, String idType, String idnum)
	throws SQLException
	{
		int rowCount =0;
		String query = "SELECT " + idType + " FROM " + table + " WHERE " + idType + " = '" +  idnum +"';";

		try
		{
			ResultSet rs = stmt.executeQuery(query);
	        
	        ResultSetMetaData rsmd = rs.getMetaData();
	        rowCount = rsmd.getColumnCount();
		}
		catch(SQLException e)
		{
			System.out.print("ERROR: " + e.getMessage());
		}

		if (rowCount > 0)
			return true;
		else
			return false;

	}

	private static String setDate(){
    String dt;
    while(true){
      try {
        dt = in.readLine();
        // check if the input date is correct
        new SimpleDateFormat("yyyy-mm-dd").parse(dt);

        break;
      }
      catch(Exception e)
      {
        System.out.println("Invalid date input! Try again.");
        continue;
      }
    }

    return dt;
  }

	public static void AddPlane(DBproject esql)
	 {//1
	 	int planeID, planeAge, numSeats;
	 	String query, make, model, planeIDString, planeAgeString, numSeatsString;
    
		planeAge = 0;
		numSeats= -1;
	     try
	     {	
		 	// generate random plane id
			query = "SELECT MAX(id) FROM Plane;";
			Statement stmt = esql._connection.createStatement();
	    	//issues the query instruction
	    	ResultSet rs = stmt.executeQuery (query);

	    	if (rs.next()) 
	    	{
	    		planeID = Integer.parseInt(rs.getString(1)) + 1;
	    	}

	    	else 
	    		planeID = 0;

			planeIDString = Integer.toString(planeID);
	    	
	   
			// user enters planeAge
			do
			{
				System.out.println("Please enter age of plane: ");

				try
				{
					planeAge  = Integer.parseInt(in.readLine());

					if(planeAge < 0)
						System.out.println("Entry must be greater than zero!");

					break;
				}
				catch(Exception e)
				{
					System.out.println("Your input is invalid!");
					continue;
				}
			}
			while(true && planeAge > 0);

			planeAgeString = Integer.toString(planeAge);

			// user enters numSeats
			do
			{
				System.out.println("Please enter num of seats: ");

				try
				{
					numSeats  = Integer.parseInt(in.readLine());
					if (numSeats >= 500)
					{	 
						System.out.println("Entry must be greater than 500!");
						continue;
					}
					else if(numSeats < 0)
					{
						System.out.println("Entry must be greater than zero!");
						continue;
					}
					else
					break;
				}
				catch(Exception e)
				{
					System.out.println("Your input is invalid!");
					continue;
				}
			}
			while(true && numSeats < 500 && numSeats > 0);

			numSeatsString = Integer.toString(numSeats);

			// user enters make of plane
			do
			{
				System.out.println("Please enter plane the make of the plane: ");
				try
				{
					make = in.readLine();
					         break;
	                        }
	                        catch(Exception e)
	                        {
	                          System.out.println("Error: " + e.getMessage());
	                                continue;
	                        }
			}
			while(true);
			
			// user enters model of plane
			do
			{
				
				System.out.println("Please enter plane the model of the plane: ");
				try{
					model = in.readLine();
					break;
				}
				catch(Exception e)
				{
				  System.out.println("Error: " + e.getMessage());
					continue;
				}
			}
			while(true);



			query = "INSERT INTO Plane (id, make, model, age, seats) VALUES ( " + planeIDString + " , '" + make + "' , '" + model + "' , " + planeAgeString + " , " + numSeatsString + " );";
			
	 		System.out.print(query);  
 	}
	catch(SQLException e)
	{
		System.out.println("Error: " + e.getMessage());
	}


	}

	public static void AddPilot(DBproject esql) 
	{//2
		int pilotId;
		String query, fullName, nationality, pilotIdString;
		try
		{
			// Auto-generate pilot's id
			query = "SELECT MAX(id) FROM Pilot;";
			Statement stmt = esql._connection.createStatement();
	    	//issues the query instruction
	    	ResultSet rs = stmt.executeQuery(query);

	    	if (rs.next()) 
	    	{
	    		pilotId = Integer.parseInt(rs.getString(1)) + 1;
	    	}

	    	else 
	    		pilotId = 0;

			pilotIdString = Integer.toString(pilotId);

			// enter full name of pilot
			do
			{

				try
				{
					System.out.print("Please enter the Pilot's full name: ");
					fullName = in.readLine();

					System.out.print("Name entered: " + fullName);
					break;
				}
				catch(Exception e)
				{
					System.out.println("Error: " + e.getMessage());
					continue;
				}
			}
			while(true);

			//enter nationality of pilot 
			do
			{
				System.out.print("Please enter the nationality of the Pilot: ");
				try
				{
					nationality = in.readLine();
					break;
				}
				catch(Exception e)
				{
					System.out.println("Error: " + e.getMessage());
					continue;
				}
			}
			while(true);


			query = "INSERT INTO Pilot(id, fullname, nationality) VALUES (" + pilotIdString + " , '" + fullName + "' , '" + nationality + "' );";
			esql.executeUpdate(query);
			System.out.println(query);

		}
		catch(SQLException e)
		{
			System.out.println("Error: " + e.getMessage());
		}

	}

	public static void AddFlight(DBproject esql)
	{
		int flightNum, cost, numSold, numStops;
		String query, departure_date, arrival_date, arrival_airport, departure_airport, flightNumString, costString, numSoldString, numStopsString;
		
		flightNum = 0;
		cost = 0;
		numSold = -1;
		numStops = -1;
	    
			// flightNum entry
		try
		{
			// generate random flight num
			query = "SELECT MAX(fnum) FROM Flight;";
			Statement stmt1 = esql._connection.createStatement();
	    	//issues the query instruction
	    	ResultSet rs = stmt1.executeQuery (query);

	    	if (rs.next()) 
	    	{
	    		flightNum = Integer.parseInt(rs.getString(1)) + 1;
	    	}

	    	else 
	    		flightNum = 0;

			flightNumString = Integer.toString(flightNum);

			// cost entry
			do
			{
				System.out.println("Please enter the cost of the flight: ");

				try
				{
					cost = Integer.parseInt(in.readLine());
					if(cost < 0)
					System.out.print("Entry must be greater than zero!");

					break;
				}
				catch(Exception e)
				{
					System.out.println("Your input is invalid!");
					continue;
				}
			}
			while(cost == 0);

			costString = Integer.toString(cost);

			// num tickets sold
			do
			{
				System.out.println("Please enter the number of tickets sold for this flight: ");
				try
				{
					numSold = Integer.parseInt(in.readLine());
					if(numSold < 0)
						System.out.print("Entry must be greater than zero!");

					break;
				}
				catch(Exception e)
				{
					System.out.println("Error: " + e.getMessage());
					continue;
				}
			}
			while(numSold == -1);

			numSoldString = Integer.toString(numSold);

			// num of stops
			do
			{
				System.out.println("Please enter the number of stops for this flight: ");
				try
				{
					numStops = Integer.parseInt(in.readLine());
					if (numStops < 0)
						System.out.print("Entry must be greater than zero!");	

					break;
				}
				catch(Exception e)
				{
					System.out.println("Error: " + e.getMessage());
					continue;
				}
			}
			while(numStops == -1);

			numStopsString = Integer.toString(numStops);

			//departure date
			do
			{
				System.out.println("Please enter the actual departure date (format yyyy-mm-dd): ");
				try
				{
					departure_date = setDate();

					break;
				}
				catch(Exception e)
				{
					System.out.println("Error: " + e.getMessage());
					continue;
				}
			}
			while(true);

			// arrival date
			do
			{
				System.out.println("Please enter the actual arrival date (format yyyy-mm-dd): ");
				try
				{
					arrival_date = setDate();
					break;
				}
				catch(Exception e)
				{
					System.out.println("Error: " + e.getMessage());
					continue;
				}
			}
			while(true);

			//arrival airport
			do
			{
				System.out.println("Please enter the arrival airport: ");
				try
				{
					arrival_airport = in.readLine();
					break;
				}
				catch(Exception e)
				{
					System.out.println("Error: " + e.getMessage());
					continue;
				}
			}
			while(true);


			// departure airport
			do
			{
				System.out.println("Please enter the departure airport: ");

				try
				{
					departure_airport = in.readLine();
					break;
				}
				catch(Exception e)
				{
					System.out.println("Your input is invalid!");
					continue;
				}
			}
			while(true);

			query = "INSERT INTO Flight (fnum, cost, num_sold, num_stops, actual_departure_date, actual_arrival_date, arrival_airport, departure_airport) VALUES (" + flightNumString + " , " + costString + " , " + numSoldString + " , " + numStopsString + " , '" + departure_date + "' , '" + arrival_date + "' , '" + arrival_airport + "' , '" + departure_airport + "' );";
			esql.executeUpdate(query);

			}

			catch(SQLException e)
			{
	  			System.out.print("ERROR: " + e.getMessage());
			}


			// *** Beginning of Flight info entries ***

			int fiid;
			String query2, fiidString, fi_fnum;
			try
			{
				// generate random flight info id
				query2 = "SELECT MAX(fiid) FROM FlightInfo;";

				Statement stmt2 = esql._connection.createStatement();
	    		//issues the query instruction
	    		ResultSet rs2 = stmt2.executeQuery (query2);

	    		if (rs2.next()) 
	    		{
	    			fiid = Integer.parseInt(rs2.getString(1)) + 1;
	    		}

	    		else 
	    			fiid = 0;

				fiidString = Integer.toString(fiid);
				

				Statement stmt3 = esql._connection.createStatement();
				String fnumVal = "fnum";
				String flightVal = "Flight";
				fi_fnum = "";
				
				do
				{
					System.out.println("Please enter the flight number for flight info: ");

					try
					{
						fi_fnum = in.readLine();
						break;
					}
					catch(Exception e)
					{
						System.out.println("Your input is invalid!");
						continue;
					}
				}
				while((hasID(stmt3, flightVal , fnumVal, fi_fnum)) == false);


			}
			catch(SQLException e)
			{
				System.out.print("Error: " + e.getMessage());
			}

	}

	public static void AddTechnician(DBproject esql) 
	{
		int technicianId;
		String query, fullName, technicianIdString;

		// technician entry

		try
		{
			// Auto-generate technician id
		 	query = "SELECT MAX(id) FROM Technician;";
			Statement stmt = esql._connection.createStatement();
	    	//issues the query instruction
	    	ResultSet rs = stmt.executeQuery(query);

	    	if (rs.next()) 
	    	{
	    		technicianId = Integer.parseInt(rs.getString(1)) + 1;
	    	}

	    	else 
	    		technicianId = 0;

			technicianIdString = Integer.toString(technicianId);

			// full name entry
			do
			{
				System.out.println("Please enter the full name of this new technician: ");
				try
				{
					fullName = in.readLine();
					break;
				}
				catch(Exception e)
				{
					System.out.println("Error: " + e.getMessage());
					continue;
				}
			}
			while(true);


			query = "INSERT INTO Technician (id, full_name) VALUES ( " + technicianIdString + " , '"+ fullName + "' );";
			stmt.executeUpdate(query);
			System.out.println(query);

	}
		catch(SQLException e)
		{
			System.out.println("Error: " + e.getMessage());
		}

		

	}

	public static void BookFlight(DBproject esql) {//5
		// Given a customer and a flight that he/she wants to book, add a reservation to the DB
	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//6
		// For flight number and date, find the number of availalbe seats (i.e. total plane capacity minus booked seats )
	}

	public static void ListsTotalNumberOfRepairsPerPlane(DBproject esql) {//7
		// Count number of repairs per planes and list them in descending order
	}

	public static void ListTotalNumberOfRepairsPerYear(DBproject esql) {//8
		// Count repairs per year and list them in ascending order
	}
	
	public static void FindPassengersCountWithStatus(DBproject esql) {//9
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number.
	}
}
