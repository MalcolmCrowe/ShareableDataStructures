/*
 * Team.java
 *
 * Created on 30 December 2006, 16:17
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
public class Team {
    @Id @Column int Id;
    @Column public String Name;
}
