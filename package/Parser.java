package com.ef;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * @author Roman Chepelyev.
 */
public class Parser {

  private static String csvFile = "access.log";
  private static final String dbURL = "jdbc:mysql://localhost:3306/wallethub";
  private static final String username = "root";
  private static final String password = "root";
  private static Connection conn = null;

  static void loadDriver() throws Exception {
    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      System.out.println("Loaded the appropriate driver");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  static Connection getConnection() throws Exception {
    try {
      if (conn != null)
        return conn;
      // Get a connection
      conn = DriverManager.getConnection(dbURL, username, password);
      System.out.println("Connected to the database ");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    return conn;
  }

  public static void main(String[] args){
    Map<String, String> options = new HashMap<>();
    for (String arg: args) {
      String[] pairs = arg.split("=");
      if (pairs.length == 2)
      options.put(pairs[0].substring(2), pairs[1]);
    }
    if (!args[args.length -1].startsWith("--"))
      csvFile = args[args.length -1];

    try {
      loadDriver();
      getConnection();
      clearLog();
      CSVReader.read(csvFile);
      DateFormat indf = new SimpleDateFormat("yyyy-MM-dd.HH:mm:ss");
      DateFormat outdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Date startDate = indf.parse(options.get("startDate"));
      String duration = options.get("duration");
      int hours;
      switch (duration) {
        case "hourly":
          hours = 1;
          break;
        case "daily":
          hours = 24;
          break;
        default:
          throw new IllegalArgumentException("Unknown parameter value [duration]");
      }
      Calendar cal = new GregorianCalendar();
      cal.setTime(startDate);
      cal.add(Calendar.HOUR, hours);
      Date endDate = cal.getTime();
      String threshold = options.get("threshold");
      Parser.findLog(new String[]{outdf.format(startDate), outdf.format(endDate), threshold});
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (conn != null)
        try {
          conn.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }

  }

  static void clearLog() throws Exception {
    Statement st = null;
    try {
      System.out.println("Clearing table from the database ");
      String query = "delete from accesslog";
      st = conn.createStatement();
      st.executeUpdate(query);
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    } finally {
      if (st != null)
        st.close();
    }
  }

  static void saveLog(String[] record) throws Exception {
    PreparedStatement st = null;
    try {
      String query = "insert into accesslog (Date,IP,Request,Status,UserAgent) "+
          "values(?,?,?,?,?)";
      st = conn.prepareStatement(query);
      st.setString(1, record[0]);
      st.setString(2, record[1]);
      st.setString(3, record[2]);
      st.setString(4, record[3]);
      st.setString(5, record[4]);
      st.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    } finally {
      if (st != null)
        st.close();
    }
  }

  static void findLog(String[] options) throws Exception {
    PreparedStatement st = null;
    ResultSet rs = null;
    try {
      String startDate = options[0];
      String endDate = options[1];
      String threshold = options[2];
      System.out.println("Finding logs by startDate="+startDate+", endDate="+endDate+", threshold="+threshold);
      String query = "select * from accesslog where Date between ? and ? group by IP having count(*) > ?";
      st = conn.prepareStatement(query);
      st.setString(1, startDate);
      st.setString(2, endDate);
      st.setString(3, threshold);
      rs =  st.executeQuery();
      while (rs.next()) {
        String[] record  = new String[5];
        record[0] = rs.getString(2);
        record[1] = rs.getString(3);
        record[2] = rs.getString(4);
        record[3] = rs.getString(5);
        record[4] = rs.getString(6);
        System.out.println(Arrays.toString(record));
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    } finally {
      if (rs != null)
        rs.close();
      if (st != null)
        st.close();
    }
  }

}

class CSVReader {

  static void read(String csvFile) {
    String line;
    String cvsSplitBy = "\\|";
    System.out.println("Reading data from file "+ csvFile);
    try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
      while ((line = br.readLine()) != null) {
        String[] record = line.split(cvsSplitBy);
        Parser.saveLog(record);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}