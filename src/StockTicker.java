import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import java.sql.*;

public class StockTicker {

  // JDBC driver name and database URL
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
   static final String DB_URL = "jdbc:mysql://localhost/";

   //  Database credentials
   static final String USER = "root";
   static final String PASS = "0";

   // Database connection
   static Connection conn = null;
   static Statement stmt = null;
   static PreparedStatement pstmt = null;

   static private int current_time;
   static private java.util.Date ctime;

  //private static HashMap<Integer, Transaction> recentTransaction;
  private static int idCounter;

  public static void main(String[] args) {
    DatabaseInit();
    LoadQTY_CSVData("qty_stocks.csv");
    //LoadPRICE_CSVData("price_stocks.csv");
    CreateStockData();

    ctime = new java.util.Date();
    SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss");
    try
    {
      ctime = ft.parse("2016-01-01 08:00:00");
    }
    catch (ParseException e){
      System.err.print("parse error");
    }
    String nowtime = ft.format(ctime);
    System.out.println(nowtime);
  }

  public void syscTime(java.util.Date time){
    this.ctime = time;
  }

  // public Transaction createTransaction(Holder seller, Holder buer){
  //
  // }

  public void print(){
    System.out.println(Integer.toString(idCounter));
  }

  // public Transaction getTransaction(int tid){
  //   if(recentTransaction.containsKey(tid)){
  //     return recentTransaction.get(tid);
  //   }
  //   else{
  //     System.err.println("Transaction " + Integer.toString(tid) + " not found");
  //     return null;
  //   }
  // }

  public static void CreateStockData(){
    try
    {
        String sql = "USE PROJECTDB";
        stmt.executeUpdate(sql);

        sql = "DROP TABLE IF EXISTS STOCKS";
        stmt.executeUpdate(sql);

        sql = "CREATE TABLE STOCKS (" +
              "company VARCHAR(100), amount INTEGER )";
        stmt.executeUpdate(sql);

        String fi_time = "2016-01-01 8:00:00";
        //insert first issue
        //get first issue company name

        sql = "SELECT COUNT(*) AS NUM FROM QUANTITY WHERE DATE = \"" + fi_time + "\"";
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        int num_stocks = rs.getInt("NUM");
        System.out.println(Integer.toString(num_stocks));
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
          System.out.println(company_name[i] + " :" + Integer.toString(amount[i]));
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
  public static void LoadPRICE_CSVData(String filename){

    try
    {
        String sql = "USE PROJECTDB";
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
        System.out.println("Table Price build successfully");
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


  public static void LoadQTY_CSVData(String filename){
    try
    {
        String sql = "USE PROJECTDB";
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
  public static void DatabaseInit(){
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

        String sql = "DROP DATABASE IF EXISTS PROJECTDB";
        stmt.executeUpdate(sql);

        sql = "CREATE DATABASE PROJECTDB";
        stmt.executeUpdate(sql);

        System.out.println("Database created successfully...");
     }catch(SQLException se){
        //Handle errors for JDBC
        se.printStackTrace();
     }catch(Exception e){
        //Handle errors for Class.forName
        e.printStackTrace();
     }
     System.out.println("Goodbye!");
    }
}
