/*
 * OrderItem.java
 *
 * Created on 07 January 2007, 15:56
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
public class OrderItem {
    @ManyToOne @JoinColumn(name="oid") public Order order;
    @Column public int iid;
    @Column public String prodname;
    @Column public int qty;
}
