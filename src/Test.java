import net.sf.hajdbc.SimpleDatabaseClusterConfigurationFactory;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.cache.eager.SharedEagerDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.cache.simple.SimpleDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.dialect.mysql.MySQLDialectFactory;
import net.sf.hajdbc.distributed.jgroups.JGroupsCommandDispatcherFactory;
import net.sf.hajdbc.sql.DriverDatabase;
import net.sf.hajdbc.sql.DriverDatabaseClusterConfiguration;
import net.sf.hajdbc.state.simple.SimpleStateManagerFactory;
import net.sf.hajdbc.sync.DumpRestoreSynchronizationStrategy;
import net.sf.hajdbc.sync.FastDifferentialSynchronizationStrategy;
import net.sf.hajdbc.sync.FullSynchronizationStrategy;
import net.sf.hajdbc.util.concurrent.cron.CronExpression;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by dianli on 5/26/16.
 */
public class Test {
    public static void main(String[] argv) throws ClassNotFoundException, SQLException, ParseException, IOException {
        Class.forName("com.mysql.jdbc.Driver");
        // Define each database in the cluster
        DriverDatabase db1 = new DriverDatabase();
        db1.setId("db1");
        db1.setLocation("jdbc:mysql://localhost:3306/db1");
        db1.setUser("root");
        db1.setPassword("0");

        DriverDatabase db2 = new DriverDatabase();
        db2.setId("db2");
        db2.setLocation("jdbc:mysql://localhost:3306/db2");
        db2.setUser("root");
        db2.setPassword("0");

// Define the cluster configuration itself
        DriverDatabaseClusterConfiguration config = new DriverDatabaseClusterConfiguration();
// Specify the database composing this cluster
        config.setDatabases(Arrays.asList(db1, db2));
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
        Map<String, SynchronizationStrategy> map = new Hashtable<>();
        map.put("dump-restore", new DumpRestoreSynchronizationStrategy());
        map.put("full", new FullSynchronizationStrategy());
        map.put("diff", new FastDifferentialSynchronizationStrategy());
        config.setSynchronizationStrategyMap(map);
        config.setDefaultSynchronizationStrategy("full");


// Register the configuration with the HA-JDBC driver
        net.sf.hajdbc.sql.Driver.setConfigurationFactory("mycluster", new SimpleDatabaseClusterConfigurationFactory<Driver, DriverDatabase>(config));
        // Database cluster is now ready to be used!
        Connection connection = DriverManager.getConnection("jdbc:ha-jdbc:mycluster", "root", "0");
        Statement stmt = connection.createStatement();

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        int i = 0;
//        while (true) {
//            String line= in.readLine().trim().toLowerCase();
//            stmt.executeUpdate("CREATE TABLE " + "pet"  + i + " (name VARCHAR(20), owner VARCHAR(20),species VARCHAR(20), sex CHAR(1), birth DATE, death DATE);");
////            stmt.executeUpdate("INSERT INTO `pet0` VALUES ('haha','hehe','hehe','h','0000-00-00','0000-00-00');");
//            i++;
//        }
//        System.out.println(db2.isActive());

    }
}
