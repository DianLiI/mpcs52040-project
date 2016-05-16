import org.jgroups.blocks.ResponseMode;

import java.sql.*;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by dianli on 5/15/16.
 */
public class UserTable {
    // JDBC driver name and database URL
    private final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private final String DB_URL = "jdbc:mysql://localhost/";

    //  Database credentials
    private final String USER = "root";
    private final String PASS = "0";

    // Database connection
    private Connection conn;
    private Statement stmt;
    private PreparedStatement pstmt;
    private String db_name;

    public UserTable(String db_name) throws SQLException, ClassNotFoundException {
        this.db_name = db_name;
        this.initDatabase();
    }

    private void initDatabase() throws ClassNotFoundException, SQLException {

        // Register JDBC driver
        Class.forName(this.JDBC_DRIVER);

        // Open a connection
        System.out.println("Connecting to database...");
        this.conn = DriverManager.getConnection(DB_URL, USER, PASS);
        this.stmt = this.conn.createStatement();
        this.stmt.executeUpdate("USE " + db_name);
        System.out.println("Successfully connected to database");

        // create database if not exists
        String sql = "CREATE DATABASE IF NOT EXISTS " + db_name;
        stmt.executeUpdate(sql);
        // create tables if not exists
        sql = "CREATE TABLE IF NOT EXISTS UserInfo(username VARCHAR(100) UNIQUE, password VARCHAR(100), balance DOUBLE, type VARCHAR(10))";
        stmt.executeUpdate(sql);
        sql = "CREATE TABLE IF NOT EXISTS UserStock(username VARCHAR(100), stockname VARCHAR(100), amount INTEGER, UNIQUE(username, stockname))";
        stmt.executeUpdate(sql);
    }

    public Holder login(String username, String password) throws SQLException {
        String sql = "SELECT * FROM UserInfo WHERE username = \"" + username +"\"";
        ResultSet rs = this.stmt.executeQuery(sql);
        if (rs.next()) {
            if (rs.getString("password").equals(password)) {
                if (rs.getString("type").equals("company")) {
                    return new Company(username);
                }
                else {
                    return new Investor(username, rs.getDouble("balance"), this.getStock(username));
                }
            }
        }
        return null;
    }

    public boolean register(String username, String password, String type) {
        String sql = String.format("INSERT INTO UserInfo VALUES(\"%s\", \"%s\", \"%d\", \"%s\")"
                , username, password, 10000, type);
        try {
            this.stmt.executeUpdate(sql);
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    private Hashtable<String, Integer> getStock(String username) throws SQLException {
        Hashtable<String, Integer> stockMap = new Hashtable<>();
        String sql = String.format("SELECT stockname, amount FROM UserStock where username = \"%s\"", username);
        ResultSet rs = this.stmt.executeQuery(sql);
        while(rs.next()) {
            stockMap.put(rs.getString("stockname"), rs.getInt("amount"));
        }
        return stockMap;
    }

    public void updateUser(Holder holder) throws SQLException {
        // only investors could change states
        Investor investor = (Investor) holder;
        String sql = String.format("UPDATE UserInfo SET balance=\"%f\" WHERE username = \"%s\"",
                investor.balance, investor.username);
        this.stmt.executeUpdate(sql);
        for (String k : investor.stockTable.keySet()) {
            sql = String.format("INSERT INTO UserStock VALUES(\"%s\", \"%s\", \"%d\") ON DUPLICATE KEY UPDATE username=\"%s\", stockname=\"%s\", amount=\"%d\"",
                    investor.username, k, investor.stockTable.get(k), investor.username, k, investor.stockTable.get(k));
            this.stmt.executeUpdate(sql);
        }
    }
}
