import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import java.sql.*;

public class StockTicker {

  // JDBC driver name and database URL
   final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
   final String DB_URL = "jdbc:mysql://localhost/";

   //  Database credentials
   final String USER = "root";
   final String PASS = "0";

   // Database connection
   Connection conn = null;
   Statement stmt = null;
   PreparedStatement pstmt = null;

   private java.util.Date issue_time;
   private java.util.Date ctime;
   private SimpleDateFormat ft;
   public long time_offset = 0; // change to private later

   private String db_name = "DB_CHICAGO";

  public StockTicker(String db_name) throws ParseException, SQLException{
    this.db_name = db_name;

    DatabaseInit();
    LoadQTY_CSVData("data/qty_stocks.csv");
    LoadPRICE_CSVData("data/price_stocks.csv");
    CreateStockData();
    initTIME();
    System.out.println("Start time: " + ft.format(ctime));
  }

  private void databaseTest() throws SQLException{
    String sql = "SELECT SUM(QTY) AS DIFF FROM QUANTITY WHERE STOCK LIKE " +
          "\"ACCOR\" AND DATE > \"2016-01-01 08:00:00\" AND DATE < \"2016-01-01 08:00:00\"";
    ResultSet rs = stmt.executeQuery(sql);
    rs.next();
    int diff = rs.getInt("diff");
    //System.out.println(Integer.toString(diff));
    rs.close();
  }

  public double getPrice(String stock) throws ParseException,SQLException{
    java.util.Date sys_time = new java.util.Date();
    java.util.Date market_time = new java.util.Date(sys_time.getTime() + time_offset);

    String sql = "SELECT PRICE FROM PRICE WHERE COMPANY LIKE \"" + stock + "\" AND DATE <= \"";
    sql = sql + ft.format(market_time) + "\" ORDER BY DATE DESC LIMIT 1";
    ResultSet rs = stmt.executeQuery(sql);
    if(rs.next())
    {
      return rs.getDouble("PRICE");
    }
    else{
      return -1.0;    //did not find the price
    }
  }

  public int hasStock(String stock) throws ParseException, SQLException{
    java.util.Date sys_time = new java.util.Date();
    java.util.Date market_time = new java.util.Date(sys_time.getTime() + time_offset);
    syscTime(market_time);
    String sql = "SELECT AMOUNT FROM STOCKS WHERE STOCK LIKE \"" + stock + "\"";
    ResultSet rs = stmt.executeQuery(sql);

    if(rs.next()){
      return rs.getInt("AMOUNT");
    }
    else{
      return 0;
    }
  }

  public Return buyStocks(String stock, int amount) throws ParseException,SQLException{
    int new_amount;
    String sql = "SELECT AMOUNT FROM STOCKS WHERE STOCK LIKE \"" + stock + "\"";
    ResultSet rs = stmt.executeQuery(sql);
    rs.next();
    new_amount = rs.getInt("AMOUNT") - amount;
    rs.close();

    if(new_amount < 0){
      return new Return("not enough stocks", false);
    }
    else
    {
      sql = "UPDATE STOCKS SET AMOUNT = " + Integer.toString(new_amount) + " WHERE STOCK LIKE \"" + stock + "\"";
      stmt.executeUpdate(sql);
      double pr = getPrice(stock);
      return new Return("Price : " + Double.toString(pr), true);
    }
  }

  public double sellStocks(String stock, int amount) throws ParseException, SQLException{
    int new_amount;

    String sql = "SELECT AMOUNT FROM STOCKS WHERE STOCK LIKE \"" + stock + "\"";
    ResultSet rs = stmt.executeQuery(sql);
    rs.next();
    new_amount = rs.getInt("AMOUNT") + amount;
      System.out.println(new_amount);
    rs.close();

    //sell always accept
    sql = "UPDATE STOCKS SET AMOUNT = " + Integer.toString(new_amount) + " WHERE STOCK LIKE \"" + stock + "\"";
    stmt.executeUpdate(sql);

    double pr = getPrice(stock);
    return pr;
  }

  public double issueStocks(String stock, int amount) throws ParseException,SQLException{
    return sellStocks(stock, amount);
  }

  public void syscTime(java.util.Date time) throws ParseException,SQLException{
    java.util.Date sys_time = new java.util.Date();
    time_offset = time.getTime() - sys_time.getTime();

    //sysc stock issue
    syscStockIssue(time);
    issue_time = time;
  }

  private void initTIME() throws SQLException,ParseException{
    ft = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss");
    ctime = ft.parse("2016-01-01 08:00:00");
    issue_time = ctime;
    syscTime(ctime);
  }

  private void syscStockIssue(java.util.Date now) throws ParseException, SQLException{
    String start = ft.format(issue_time);
    String end = ft.format(now);

    String fi_time = "2016-01-01 8:00:00";

    //get company name
    String sql = "SELECT COUNT(*) AS NUM FROM QUANTITY WHERE DATE = \"" + fi_time + "\"";
    ResultSet rs = stmt.executeQuery(sql);
    rs.next();
    int num_stocks = rs.getInt("NUM");
    rs.close();

    String[] company_name = new String[num_stocks];
    int[] current_stock = new int[num_stocks];      //already issued STOCKS
    int[] issue_stock = new int[num_stocks];        //need to be issued at this time

    sql = "SELECT * FROM QUANTITY WHERE DATE = \"" + fi_time + "\"";
    rs = stmt.executeQuery(sql);

    int i = 0;
    while(rs.next()){
      company_name[i] = rs.getString("stock");
      i++;
    }
    rs.close();

    //get current amount of stocks and issued stocks
    for(i = 0; i < num_stocks; i++){
      sql = "SELECT * FROM STOCKS WHERE STOCK LIKE \"" + company_name[i] + "\"";
      rs = stmt.executeQuery(sql);
      rs.next();
      current_stock[i] = rs.getInt("amount");
      rs.close();

      sql = "SELECT SUM(QTY) AS DIFF FROM QUANTITY WHERE STOCK LIKE \"" + company_name[i] + "\" AND " +
            "DATE > \"" + start + "\" AND DATE <= \"" + end + "\"";
      rs = stmt.executeQuery(sql);
      rs.next();
      issue_stock[i] = rs.getInt("diff");
      rs.close();
    }

    //update stocks
    for(i = 0; i < num_stocks; i++){
      int newValue = current_stock[i] + issue_stock[i];
      sql = "UPDATE STOCKS SET AMOUNT = " + Integer.toString(newValue) + " WHERE STOCK LIKE \"" + company_name[i] + "\"";
      //System.out.println("syscStockIssue -- " + sql);
      stmt.executeUpdate(sql);
    }


  }

  private void CreateStockData(){
    try
    {
        String sql = "USE " + db_name;
        stmt.executeUpdate(sql);

        sql = "DROP TABLE IF EXISTS STOCKS";
        stmt.executeUpdate(sql);

        sql = "CREATE TABLE STOCKS (" +
              "stock VARCHAR(100), amount INTEGER)";
        stmt.executeUpdate(sql);

        String fi_time = "2016-01-01 8:00:00";
        //insert first issue
        //get first issue company name

        sql = "SELECT COUNT(*) AS NUM FROM QUANTITY WHERE DATE = \"" + fi_time + "\"";
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        int num_stocks = rs.getInt("NUM");
        rs.close();

        String[] company_name = new String[num_stocks];
        int[] amount = new int[num_stocks];

        sql = "SELECT * FROM QUANTITY WHERE DATE = \"" + fi_time + "\"";
        rs = stmt.executeQuery(sql);

        int i = 0;
        while(rs.next()){
          company_name[i] = rs.getString("stock");
          amount[i] = rs.getInt("qty");
          i++;
        }
        rs.close();

        for(i = 0; i < num_stocks; i++){
          sql = "INSERT INTO STOCKS " +
                "VALUES (\"" + company_name[i] + "\", " + Integer.toString(amount[i]) + ")";
          stmt.executeUpdate(sql);
        }

      } catch(SQLException se){
         //Handle errors for JDBC
         se.printStackTrace();
      } catch(Exception e){
         //Handle errors for Class.forName
         e.printStackTrace();
      }
  }

  // public Transaction getTransaction(){}
  private void LoadPRICE_CSVData(String filename){

    try
    {
        String sql = "USE " + db_name;
        stmt.executeUpdate(sql);

        sql = "DROP TABLE IF EXISTS PRICE";
        stmt.executeUpdate(sql);



        Scanner scanner = new Scanner(new File(filename));
        //scanner.useDelimiter(",");

        //read header
        //header1 - continent
        String header1 = scanner.nextLine();
        String[] parts1 = header1.split("\\,");

        //header2 - country
        String header2 = scanner.nextLine();
        String[] parts2 = header2.split("\\,");

        //header3 - market
        String header3 = scanner.nextLine();
        String[] parts3 = header3.split("\\,");

        //header 4 - company
        String header4 = scanner.nextLine();
        String[] parts4 = header4.split("\\,");
        String[] parts5 = new String[parts4.length];
        for(int i = 0, j = 0; i < parts4.length; i++){
          if(parts4[i].charAt(0) != '\"'){
            parts5[j] = parts4[i];
            j++;
          }
          else{
            parts5[j] = parts4[i];
            int len = parts4[i].length();
            while(parts4[i].charAt(len - 1) != '\"'){
              i++;
              parts5[j] = parts5[j] + "," + parts4[i];
              len = parts4[i].length();
            }
            parts5[j] = parts5[j].replace("\"","");
            j++;
          }
        }


        sql =  "CREATE TABLE PRICE(" +
                      "date DATETIME, company VARCHAR(100), price DOUBLE)";

        System.out.println("CreateTable sql : " + sql);

        stmt.executeUpdate(sql);
        System.out.println("Table created successfully...");
        System.out.print("Import price data......");

        while(scanner.hasNextLine()){
            // System.out.print(scanner.nextLine()+"|||");
            String aLine = scanner.nextLine();
            String[] tokens = aLine.split("\\,");
            for(int i = 3; i < tokens.length; i++){
              String company_name = parts5[i];
              if(company_name.equals("ACCOR") || company_name.equals("AIR LIQUIDE") || company_name.equals("AIRBUS GROUP"))
              // System.out.print(tokens[i] + "\t");
            {
              sql = "INSERT INTO PRICE " +
                    "VALUES (STR_TO_DATE(\'" + tokens[0] + " " + tokens[1] +":00\', \'%m/%d/%Y %H:%i:%s\')";
              sql = sql + ", \"" + parts5[i];  //company name
              sql = sql + "\", " + tokens[i]; //
              sql = sql + ")";

              // System.out.println("sql: " + sql);
              stmt.executeUpdate(sql);
            }
            }

            // System.out.println(tokens[0] + " " + tokens[1]);
        }
        System.out.println("Success");
        scanner.close();
      } catch (FileNotFoundException e){
        System.err.println("cannot open file");
      } catch(SQLException se){
         //Handle errors for JDBC
         se.printStackTrace();
      } catch(Exception e){
         //Handle errors for Class.forName
         e.printStackTrace();
      }

  }


  private void LoadQTY_CSVData(String filename){
    try
    {
        String sql = "USE " + db_name;
        stmt.executeUpdate(sql);

        sql = "DROP TABLE IF EXISTS QUANTITY";
        stmt.executeUpdate(sql);



        Scanner scanner = new Scanner(new File(filename));
        //scanner.useDelimiter(",");

        //read header
        String header = scanner.nextLine();
        String[] parts = header.split("\\,");

        //parts[0] : Date
        //parts[1] : Time
        //parts[2] : Stock

        sql =  "CREATE TABLE QUANTITY(" +
                      "date DATETIME, stock VARCHAR(100), qty INTEGER)";

        System.out.println("CreateTable sql : " + sql);

        stmt.executeUpdate(sql);
        System.out.println("Table created successfully...");

        while(scanner.hasNextLine()){
            // System.out.print(scanner.nextLine()+"|||");
            String aLine = scanner.nextLine();
            String[] tokens = aLine.split("\\,");
            for(int i = 3; i < tokens.length; i++){
              // System.out.print(tokens[i] + "\t");
              sql = "INSERT INTO QUANTITY " +
                    "VALUES (STR_TO_DATE(\'" + tokens[0] + " " + tokens[1] +":00\', \'%m/%d/%Y %H:%i:%s\')";
              sql = sql + ", \'" + parts[i];  //company name
              sql = sql + "\', " + tokens[i]; //
              sql = sql + ")";

              // System.out.println("sql: " + sql);
              stmt.executeUpdate(sql);
            }
        }
        scanner.close();
      } catch (FileNotFoundException e){
        System.err.println("cannot open file");
      } catch(SQLException se){
         //Handle errors for JDBC
         se.printStackTrace();
      } catch(Exception e){
         //Handle errors for Class.forName
         e.printStackTrace();
      }

  }

  //connect and create database
  private void DatabaseInit(){
    conn = null;
    stmt = null;
     try{
        //STEP 2: Register JDBC driver
        Class.forName("com.mysql.jdbc.Driver");

        //STEP 3: Open a connection
        System.out.println("Connecting to database...");
        conn = DriverManager.getConnection(DB_URL, USER, PASS);

        //STEP 4: Execute a query
        System.out.println("Creating database...");
        stmt = conn.createStatement();

//        String sql = "DROP DATABASE IF EXISTS " + db_name;
//        stmt.executeUpdate(sql);

        String sql = "CREATE DATABASE IF NOT EXISTS " + db_name;
        stmt.executeUpdate(sql);
         this.stmt.executeUpdate("USE " + db_name);

        System.out.println("Database created successfully...");
     }catch(SQLException se){
        //Handle errors for JDBC
        se.printStackTrace();
     }catch(Exception e){
        //Handle errors for Class.forName
        e.printStackTrace();
     }
    }
}
