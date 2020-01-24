import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import javax.persistence.Entity;
import javax.persistence.Id;
/*
 * NewMain.java
 *
 * Created on 22 November 2006, 23:40
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */



/**
 *
 * @author Malcolm
 */
public class NewMain {
    
    /** Creates a new instance of NewMain */
    public NewMain() {

    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        TestEntity t = new TestEntity();
        t.setId(678);
        Entity e = TestEntity.class.getAnnotation(Entity.class);
        String s = e.name();
        ArrayList a = new ArrayList();
        Object o = Proxy.getInvocationHandler(e);
        Id i = TestEntity.class.getAnnotation(Id.class);
                System.out.println(i.toString());
    }
    
}
