/*
 * To change this template, choose Tools | Templates
 
* and open the template in the editor.
 */

import org.pyrrhodb.Connection;

import java.sql.Statement;

import java.sql.ResultSet;

/**
 *
 * @author Malcolm
 */

public class JCTest 
{
    
  public static void main(String args[])
    
  {
        
     System.out.println("Connecting to abc");
     try {
        
       Connection conn = Connection.getConnection 	("localhost","abc","UWS-STAFF\\66668214","abc");
 
       Statement stmt = conn.createStatement();

       ResultSet rs = stmt.executeQuery("select * from a");

       for (boolean b = rs.first();b;b=rs.next())

       {

            System.out.println(""+rs.getInt("B")+"; "+rs.getString("C"));
        }

    }
    catch(Exception e)

    {

            System.out.println(e.getMessage());

    }

  }

}
