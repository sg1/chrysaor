package org.chrysaor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;


public class Migrator {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException, SQLException {
		File config = new File(args[0]);
		Properties tables = new Properties();
		tables.load(new FileReader(config));
		String[] tableNames = tables.keySet().toArray(new String[tables.keySet().size()]);
		ArrayList<String> tableNamesList = new ArrayList<String>(tableNames.length);
		for (int i = 0; i < tableNames.length; i++) {
			tableNamesList.add(tableNames[i]);
		}
		
		Db source = new Db(args[1], args[2], args[3], args[4], args[5]);
		Db destination = new Db(args[6], args[7], args[8], args[9], args[10]);
		
		source._connect();
		destination._connect();
		
		ResultSet destinationTables = destination.con.getMetaData().getTables("", "public", null, null);
		while (destinationTables.next()) {
			String destinationTable = destinationTables.getString("TABLE_NAME");
			if (!tableNamesList.contains(destinationTable)) {
				tableNamesList.add(destinationTable);
			}
		}
		
		Iterator<String> iterator = tableNamesList.iterator();
		while (iterator.hasNext()) {
			String table = iterator.next();
			File sqlFile = new File(config.getParentFile().getAbsolutePath() + File.separator + table + ".sql");
			if (!sqlFile.exists() || !sqlFile.canRead()) {
				System.err.println("SQL file for table " + table + " is missing, migration can not continue.");
				System.out.println(sqlFile.getAbsolutePath());
				System.exit(1);
			}
		}
		
		destination.con.setAutoCommit(false);
		
		iterator = tableNamesList.iterator();
		while (iterator.hasNext()) {
			String table = iterator.next();
			File sqlFile = new File(config.getParentFile().getAbsolutePath() + File.separator + table + ".sql");
			String sql = "";
			BufferedReader reader = new BufferedReader(new FileReader(sqlFile));
			String line;
			while ((line = reader.readLine()) != null) {
				sql += line;
			}
						
			ResultSet sourceResult = source.con.createStatement().executeQuery(sql);
			String destinationSql = _prepareQuery(sourceResult.getMetaData().getColumnCount(), table);
			
			while (sourceResult.next()) {
				PreparedStatement destinationStatement = destination.con.prepareStatement(destinationSql);
				for (int i = 0; i < sourceResult.getMetaData().getColumnCount(); i++) {
					destinationStatement.setObject(i + 1, sourceResult.getObject(i+1));
				}
				
				destinationStatement.execute();
			}
		}
		
		destination.con.commit();
	}
	
	//	prepares the export queries for the target database
	//returns the query as String
	private static String _prepareQuery (Integer colNum, String tableName) {
		String query = "INSERT INTO " + tableName + " VALUES ( ";
		String tempQuery = "";
			for (int i = 0; i < colNum; i++) {
				if (i < colNum-1) {
					tempQuery += " ? ,";
				} else {
					tempQuery += " ?";
				}				
			};
		query += tempQuery + " );";	
		//System.out.println(query);
		
		return query;
	}
	
}
