import org.pyrrhodb.*;

public class JCTest 
{
  static Connection conn;
  public static void main(String args[]) throws Exception
  {
      conn = DriverManager.getConnection ("def","Student","password");
      CreateTable();
      AddData();
      ShowTable();
      conn.close();
  }

  static void CreateTable() throws Exception
  {
      Statement stmt = conn.createStatement();
       try {
           stmt.executeUpdate("drop table a");
       } catch (Exception e) {}
       stmt.executeUpdate("create table a(b int,c char)");
  }
  
  static void AddData() throws Exception
  {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate("insert into a values(1,'One'),(2,'Two')");
  }
  static void ShowTable()
  {
     try {
       Statement stmt = conn.createStatement();
       ResultSet rs = stmt.executeQuery("select * from a");
       while (rs.next())
       {
           System.out.println(""+rs.getInt("B")+"; "+rs.getString("C"));
       }
       rs.close();
    }
    catch(Exception e)
    {
        System.out.println(e.getMessage());
    }
  }
}
