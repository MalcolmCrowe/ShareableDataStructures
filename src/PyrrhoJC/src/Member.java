/*
 * Member.java
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
@Entity @SecondaryTable(name="MemberDetails")
public class Member {
    @Id @Column public int Id;
    @Column public String FirstName;
    @Column public String Surname;
    @Column(table="MemberDetails") public String Address;
    @ManyToOne @JoinColumn(name="Cid") public Coach coach;
    @ManyToMany @JoinTable(name="MemberTeam") Team[] teams;
}
