import javax.persistence.Entity;
import javax.persistence.Id;


/*
 * TestEntity.java
 *
 * Created on 24 November 2006, 19:15
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

/**
 *
 * @author Malcolm
 */
@Entity
 public class TestEntity  {
   
    private
     int id;
     
    String aname;
     
    /** Creates a new instance of TestEntity */
    public TestEntity() {
    }
    
    @Id
      public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAname() {
        return aname;
    }
    
    public void setAname(String aName)
    {
        aname = aName;
    }
    
}
