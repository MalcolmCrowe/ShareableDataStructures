/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata.test.common;

import java.util.LinkedList;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author 77800577
 */
public class TestLinkedListDeepcopy {
    
    public TestLinkedListDeepcopy() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
        
        
        
        
    }

    
    @Test
    public void TestLinkedListDeepCopy() {
        PayLoad p1 = new PayLoad("Payload" + 1);
        PayLoad p2 = new PayLoad("Payload" + 1);
        PayLoad p3 = new PayLoad("Payload" + 1);
        PayLoad p4 = new PayLoad("Payload" + 1);
        LinkedList<PayLoad> originalList = new LinkedList<PayLoad>();
        originalList.add(p1);
        originalList.add(p2);
        originalList.add(p3);
        originalList.add(p4);
        
        LinkedList<PayLoad> deepCopidList = LinkedListCopy.deepCopy(originalList);
        
        
        for(int i = 0; i < deepCopidList.size(); i++){
            Assert.assertEquals("PayLoads are Equals",deepCopidList.get(i), originalList.get(i));
            if (deepCopidList.get(i) == originalList.get(i)){
                Assert.fail("Shallow Copied elements: "+i+originalList.get(i).toString());
            }
            
            
        }
        
    }
}
