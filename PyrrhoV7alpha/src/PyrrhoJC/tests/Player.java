/*
 * Player.java
 *
 * Created on 25 November 2006, 13:58
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
import org.pyrrhodb.*;
/**
 *
 * @author Malcolm
 */
@Entity @SecondaryTable(name="PlayerDetails")
public class Player {
    @Id @Column public int id;
    @Column public String firstName;
    @Column public String surname;
    @Column(table="PlayerDetails") public String address;
    @ManyToOne @JoinColumn(name="cid") public Coach coach;
    @ManyToMany @JoinTable(name="PlayerTeam") public Team[] teams;
}
