package org.chrysaor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Db {
    String host;
    String user;
    String pass;
    String url;
    String dbs;
    Connection con;
    String className;

    public Db(String host, String user, String pass, String type, String db) {
	this.host = host;
	this.user = user;
	this.pass = pass;
	this.dbs = db;

	if (type.equals("mysql")) {
	    this.url = "jdbc:mysql://" + host + "/" + db;
	    className = "com.mysql.jdbc.Driver";
	} else if (type.equals("postgres")) {
	    this.url = "jdbc:postgresql://" + host + "/" + db;
	    className = "org.postgresql.Driver";
	}
    }

    public void _connect() throws InstantiationException,
	    IllegalAccessException, ClassNotFoundException, SQLException {
	Class.forName(className).newInstance();
	con = DriverManager.getConnection(url, user, pass);
    }
}