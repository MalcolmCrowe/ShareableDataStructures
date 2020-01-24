/*
 * League.java
 *
 * Created on 06 January 2007, 13:15
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
import org.pyrrhodb.*;
/**
 *
 * @author Malcolm
 */
@Entity
public class League {
    @Id @Column public int id;
    @Column public String name;
}
