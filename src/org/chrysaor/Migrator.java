package org.chrysaor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;

public class Migrator {

    public static void main(String[] args) throws FileNotFoundException,
	    IOException, SQLException, InstantiationException,
	    IllegalAccessException, ClassNotFoundException {
	File config = new File(args[0]);
	String[] tableNames = readFile(config).split("\n");
	ArrayList<String> tableNamesList = new ArrayList<String>(
		tableNames.length);
	for (int i = 0; i < tableNames.length; i++) {
	    tableNamesList.add(tableNames[i]);
	}

	Db source = new Db(args[2], args[3], args[4], args[5], args[6]);
	Db destination = new Db(args[7], args[8], args[9], args[10], args[11]);

	source._connect();
	destination._connect();

	destination.con.setAutoCommit(false);

	String initSql = readSqlFile(config, args[1]);
	String[] lines = initSql.split(";\n");
	for (int i = 0; i < lines.length; i++) {
	    try {
		destination.con.createStatement().execute(lines[i]);
	    } catch (Exception ex) {
		System.err.println(lines[i]);
		ex.printStackTrace();
		System.exit(1);
	    }
	}
	destination.con.commit();

	Iterator<String> iterator = tableNamesList.iterator();
	while (iterator.hasNext()) {
	    String table = iterator.next();
	    File sqlFile = new File(config.getAbsoluteFile().getParentFile()
		    .getParentFile().getAbsolutePath()
		    + File.separator + "sql" + File.separator + table + ".sql");
	    if (!sqlFile.exists() || !sqlFile.canRead()) {
		System.err.println("SQL file for table " + table
			+ " is missing, migration can not continue.");
		System.out.println(sqlFile.getAbsolutePath());
		System.exit(1);
	    }
	}

	destination.con.createStatement().execute(
		"SET CONSTRAINTS ALL DEFERRED");

	iterator = tableNamesList.iterator();
	while (iterator.hasNext()) {
	    String table = iterator.next();
	    String sql = readSqlFile(config, table);

	    System.out.print(table + " ");

	    ResultSet sourceResult = source.con.createStatement().executeQuery(
		    sql);
	    String destinationSql = _prepareQuery(sourceResult.getMetaData()
		    .getColumnCount(), table);

	    destination.con.createStatement().execute(
		    "ALTER TABLE public." + table + " DISABLE TRIGGER USER");
	    File preSqlFile = new File(config.getAbsoluteFile().getParentFile()
		    .getParentFile().getAbsolutePath()
		    + File.separator
		    + "sql"
		    + File.separator
		    + table
		    + "_pre.sql");
	    if (preSqlFile.exists() && preSqlFile.canRead()) {
		destination.con.createStatement().execute(
			readSqlFile(config, table + "_pre"));
	    }

	    while (sourceResult.next()) {
		PreparedStatement destinationStatement = destination.con
			.prepareStatement(destinationSql);
		for (int i = 0; i < sourceResult.getMetaData().getColumnCount(); i++) {
		    if (sourceResult.getObject(i + 1) instanceof BigInteger) {
			destinationStatement.setObject(i + 1, sourceResult
				.getObject(i + 1), Types.DECIMAL);
		    } else {
			destinationStatement.setObject(i + 1, sourceResult
				.getObject(i + 1));
		    }
		}

		destinationStatement.execute();
	    }

	    File postSqlFile = new File(config.getAbsoluteFile()
		    .getParentFile().getParentFile().getAbsolutePath()
		    + File.separator
		    + "sql"
		    + File.separator
		    + table
		    + "_post.sql");
	    if (postSqlFile.exists() && postSqlFile.canRead()) {
		destination.con.createStatement().execute(
			readSqlFile(config, table + "_post"));
	    }

	    System.out.println("");
	}

	destination.con.createStatement().execute(
		"SET CONSTRAINTS ALL IMMEDIATE");
	destination.con.commit();
    }

    private static String readSqlFile(File config, String table)
	    throws FileNotFoundException, IOException {
	File sqlFile = new File(config.getAbsoluteFile().getParentFile()
		.getParentFile().getAbsolutePath()
		+ File.separator + "sql" + File.separator + table + ".sql");
	return readFile(sqlFile);
    }

    private static String readFile(File file) throws FileNotFoundException,
	    IOException {
	StringBuffer sql = new StringBuffer();
	BufferedReader reader = new BufferedReader(new FileReader(file));
	String line;
	while ((line = reader.readLine()) != null) {
	    if (!sql.toString().equals("")) {
		sql.append("\n");
	    }

	    sql.append(line);
	}
	return sql.toString();
    }

    private static String _prepareQuery(Integer colNum, String tableName) {
	String query = "INSERT INTO public." + tableName + " VALUES ( ";
	String tempQuery = "";
	for (int i = 0; i < colNum; i++) {
	    if (i < colNum - 1) {
		tempQuery += " ? ,";
	    } else {
		tempQuery += " ?";
	    }
	}
	query += tempQuery + " );";

	return query;
    }

}