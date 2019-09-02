/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;
import java.io.IOException;
import java.util.HashMap;

/**
 *
 * @author Malcolm
 */
public class DriverManager {
        public static Connection getConnection(String url,String user, String psw) throws IOException
    {
        HashMap<String,String> props = new HashMap<String,String>();
        props.put("Files",url);
        props.put("User", System.getProperty("user.name"));
        props.put("Role",url);
        return new Connection(props);
    }
}
