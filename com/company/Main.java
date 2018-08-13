package com.company;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.io.*;

public class Main {

    public static void main(String[] args) {

        String url = "jdbc:postgresql://localhost:5432/postgres";
        //String url = "jdbc:postgresql://localhost:5432/dvdrental";
        String user = "postgres";
        String password = "p123";
        //String SQL = "SELECT actor_id,first_name, last_name FROM actor";
        String SQL = "Select 'hello world'";
        String fileFormat = "csv";
        String path = "c:/temp/fl";
        DBSQL db = new DBSQL(url, user, password, path);
        db.executeSelect(SQL, fileFormat);
    }

}

class DBSQL {
    private String dirPath;
    private String url;
    private String user;
    private String password;

    public DBSQL(String _url,String _user,String _password,String _dirPath) {
        url = _url;
        user = _user;
        password = _password;
        dirPath = _dirPath;
    }

    public Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the Database server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    public void writeToFileCsv(ResultSet rset) throws SQLException, IOException{
        FileWriter fileWriter = null;
        try {
            String fileName = dirPath + "." + "csv";

            ResultSetMetaData rsmd = rset.getMetaData();
            fileWriter = new FileWriter(fileName);
            //Header line
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                fileWriter.append(rsmd.getColumnName(i));
                fileWriter.append(",");
                fileWriter.flush();
            }
            fileWriter.append(System.getProperty("line.separator"));

            while (rset.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if (rset.getObject(i) != null) {
                        String data = rset.getObject(i).toString().replaceAll(",", "");
                        fileWriter.append(data);
                        fileWriter.append(",");
                    } else {
                        String data = "null";
                        fileWriter.append(data);
                        fileWriter.append(",");
                    }
                }
                //new line entered after each row
                fileWriter.append(System.getProperty("line.separator"));
            }
        }
        catch (Exception e) {
        e.printStackTrace();
        } finally {
        if (fileWriter != null) {
            fileWriter.flush();
            fileWriter.close();
        }
        if (rset != null) {
        rset.close();
        }
        }
    }

    public void executeSelect(String query, String fileFormat) {
        Connection connection = this.connect();

        Statement stmt ;
        try {
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            if (fileFormat.equals("csv"))
                this.writeToFileCsv(rs);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}