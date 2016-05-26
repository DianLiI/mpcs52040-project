import net.sf.hajdbc.SimpleDatabaseClusterConfigurationFactory;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.cache.eager.SharedEagerDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.mysql.MySQLDialectFactory;
import net.sf.hajdbc.distributed.jgroups.JGroupsCommandDispatcherFactory;
import net.sf.hajdbc.durability.fine.FineDurabilityFactory;
import net.sf.hajdbc.sql.DriverDatabase;
import net.sf.hajdbc.sql.DriverDatabaseClusterConfiguration;
import net.sf.hajdbc.state.simple.SimpleStateManagerFactory;
import net.sf.hajdbc.sync.DumpRestoreSynchronizationStrategy;
import net.sf.hajdbc.sync.FastDifferentialSynchronizationStrategy;
import net.sf.hajdbc.sync.FullSynchronizationStrategy;
import net.sf.hajdbc.util.concurrent.cron.CronExpression;
import org.jgroups.blocks.ResponseMode;

import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
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
    //    private Statement stmt;
    private PreparedStatement pstmt;
    private String db_name;

    public UserTable(String db_name) throws SQLException, ClassNotFoundException, ParseException {
        this.db_name = db_name;
        this.initDatabase();
    }

    private void initDatabase() throws ClassNotFoundException, SQLException, ParseException {

        //STEP 2: Register JDBC driver
        Class.forName("com.mysql.jdbc.Driver");
        List<DriverDatabase> lst = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String name = this.db_name + "_" + i;
            DriverDatabase db = new DriverDatabase();
            db.setId(name);
            db.setLocation("jdbc:mysql://localhost:3306/" + name);
            db.setUser("root");
            db.setPassword("0");
            lst.add(db);
        }
//        DriverDatabase db1 = new DriverDatabase();
//        db1.setId("db1");
//        db1.setLocation("jdbc:mysql://localhost:3306/db1");
//        db1.setUser("root");
//        db1.setPassword("0");
//
//        DriverDatabase db2 = new DriverDatabase();
//        db2.setId("db2");
//        db2.setLocation("jdbc:mysql://localhost:3306/db2");
//        db2.setUser("root");
//        db2.setPassword("0");

        // Define the cluster configuration itself
        DriverDatabaseClusterConfiguration config = new DriverDatabaseClusterConfiguration();
        // Specify the database composing this cluster
        config.setDatabases(lst);
        // Define the dialect
        config.setDialectFactory(new MySQLDialectFactory());
        // Don't cache any meta data
        config.setDatabaseMetaDataCacheFactory(new SharedEagerDatabaseMetaDataCacheFactory());
        // Use an in-memory state manager
        config.setStateManagerFactory(new SimpleStateManagerFactory());
        // Make the cluster distributable
        config.setDispatcherFactory(new JGroupsCommandDispatcherFactory());
        // Activate every minute
        config.setAutoActivationExpression(new CronExpression("0 0/1 * 1/1 * ? *"));
        // Strategy
        config.setDurabilityFactory(new FineDurabilityFactory());
        Map<String, SynchronizationStrategy> map = new Hashtable<>();
        map.put("dump-restore", new DumpRestoreSynchronizationStrategy());
        map.put("full", new FullSynchronizationStrategy());
        map.put("diff", new FastDifferentialSynchronizationStrategy());
        config.setSynchronizationStrategyMap(map);
        config.setDefaultSynchronizationStrategy("full");
        config.setFailureDetectionExpression(new CronExpression("0 0/1 * 1/1 * ? *"));

        // Register the configuration with the HA-JDBC driver
        net.sf.hajdbc.sql.Driver.setConfigurationFactory(this.db_name, new SimpleDatabaseClusterConfigurationFactory<Driver, DriverDatabase>(config));
        // Database cluster is now ready to be used!
        conn = DriverManager.getConnection("jdbc:ha-jdbc:" + this.db_name, "root", "0");
        //STEP 3: Open a connection
//        System.out.println("Connecting to database...");
//        conn = DriverManager.getConnection(DB_URL, USER, PASS);

        //STEP 4: Execute a query
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();

        // create tables if not exists
        String sql = "CREATE TABLE IF NOT EXISTS UserInfo(username VARCHAR(100) UNIQUE, password VARCHAR(100), balance DOUBLE, type VARCHAR(10))";
        stmt.executeUpdate(sql);
        sql = "CREATE TABLE IF NOT EXISTS UserStock(username VARCHAR(100), stockname VARCHAR(100), amount INTEGER, UNIQUE(username, stockname))";
        stmt.executeUpdate(sql);
        stmt.close();
    }

    public Holder login(String username, String password) throws SQLException {
        String sql = "SELECT * FROM UserInfo WHERE username = \"" + username +"\"";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        if (rs.next()) {
            if (rs.getString("password").equals(password)) {
                if (rs.getString("type").equals("company")) {
                    stmt.close();
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
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private Hashtable<String, Integer> getStock(String username) throws SQLException {
        Hashtable<String, Integer> stockMap = new Hashtable<>();
        String sql = String.format("SELECT stockname, amount FROM UserStock where username = \"%s\"", username);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while(rs.next()) {
            stockMap.put(rs.getString("stockname"), rs.getInt("amount"));
        }
        stmt.close();
        return stockMap;
    }

    public void updateUser(Holder holder) throws SQLException {
        // only investors could change states
        Investor investor = (Investor) holder;
        String sql = String.format("UPDATE UserInfo SET balance=\"%f\" WHERE username = \"%s\"",
                investor.balance, investor.username);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
        for (String k : investor.stockTable.keySet()) {
            sql = String.format("INSERT INTO UserStock VALUES(\"%s\", \"%s\", \"%d\") ON DUPLICATE KEY UPDATE username=\"%s\", stockname=\"%s\", amount=\"%d\"",
                    investor.username, k, investor.stockTable.get(k), investor.username, k, investor.stockTable.get(k));
            stmt.executeUpdate(sql);
        }
        stmt.close();
    }
}
