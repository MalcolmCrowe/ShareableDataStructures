

/*
 * NewMain.java
 *
 * Created on 22 November 2006, 23:40
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

import java.io.IOException;
import org.pyrrhodb.*;

/**
 *
 * @author Malcolm
 */
public class NewMain {
    
    @PersistenceUnit(name="Tutorial",
        properties={//@PersistenceProperty(name="Host",value="192.168.0.2"),
                    @PersistenceProperty(name="User",value="TORE\\Malcolm")})
    static EntityManagerFactory emf;
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws NoResultException, IOException, PersistenceException {
         EntityManager em = emf.createEntityManager();
         Query // qr =  em.createQuery("update \"Order\" t set totalPrice=123456 where id=2");
      //   qr.executeUpdate();
         qr = em.createQuery("delete from Player p where p.teams is empty");
         qr.executeUpdate();
         qr = em.createQuery("select c from Customer c join c.orders o where c.status=CustomerStatus.PUNCTUAL and o.totalPrice>10000");
         Customer c = (Customer)qr.getResultList().get(0);
          System.out.println(c.name);
         return;
    }
}
