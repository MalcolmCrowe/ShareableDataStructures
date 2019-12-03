/*
 * Coach.java
 *
 * Created on 30 December 2006, 16:14
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
public class Coach {
  @Id @Column public int id;
  @OneToOne(fetch=FetchType.LAZY) @JoinColumn(name="pid") public Player player;
  @OneToMany(mappedBy="coach",fetch=FetchType.LAZY) public Player[] coaching;
}
