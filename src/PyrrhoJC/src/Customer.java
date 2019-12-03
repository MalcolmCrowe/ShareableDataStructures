/*
 * Customer.java
 *
 * Created on 07 January 2007, 15:49
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
public class Customer {
    @Id @Column public int id;
    @Column public String name;
    @Column public String address;
    @Column public CustomerStatus status;
    @OneToMany(mappedBy="customer") public Order[] orders;
}
