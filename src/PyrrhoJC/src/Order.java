/*
 * Order.java
 *
 * Created on 07 January 2007, 15:52
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
public class Order {
    @Id @Column public int id;
    @ManyToOne @JoinColumn(name="cid") public Customer customer;
    @Column public int totalPrice;
}
