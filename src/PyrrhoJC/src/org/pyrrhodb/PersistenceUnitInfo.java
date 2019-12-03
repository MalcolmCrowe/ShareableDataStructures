/*
 * PersistenceUnitInfo.java
 *
 * Created on 25 November 2006, 14:19
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;
import java.net.URL;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Malcolm
 */
public class PersistenceUnitInfo {
    
    PersistenceProvider pp;
    String name;
    Properties properties;
    /** Creates a new instance of PersistenceUnitInfo */
    PersistenceUnitInfo(PersistenceProvider p,String n) {
        pp = p; name = n;
    }
    public void addTransformer(ClassTransformer transformer)
    {}
    public boolean excludeUnlistedClasses()
    {   return false; }
    public ClassLoader getClassLoader()
    {   return null;  }
    public List<URL> getJarFileUrls()
    { return null; }
    public Object getJtaDataSource()
    { return null; }
    public List<String> getManagedClassNames()
    {   return null; }
    public List<String> getMappingFileNames()
    {   return null; }
    public ClassLoader getNewTempClassLoader()
    {   return null; }
    public Object getNonJtaDataSource()
    {   return null; }
    public String getPersistenceProviderClassName()
    {   return pp.getClass().getName(); }
    public String getPersistenceUnitName()
    {   return name; }
    public URL getPersistenceUnitRootUrl()
    {   return null; }
    public Properties getProperties()
    {   return properties; }
    public PersistenceUnitTransactionType getTransactionType()
    {   return PersistenceUnitTransactionType.JTA; }
}
