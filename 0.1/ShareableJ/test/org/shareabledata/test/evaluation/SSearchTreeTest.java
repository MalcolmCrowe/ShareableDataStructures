/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata.test.evaluation;

import java.io.IOException;
import java.util.LinkedList;
import org.junit.After;
import org.shareabledata.test.common.*;
import org.shareabledata.*;
import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author 77800577
 */
public class SSearchTreeTest {
    
    private static TimeAndMemoryLogger tml;
    
    
    public SSearchTreeTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
        tml = new TimeAndMemoryLogger();
    }
    
    @AfterClass
    public static void tearDownClass() {
        try {
            tml.writeToCSV("SSearchTreeTestOutput_Java.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
       
    @After
    public void tearDown() {
        Runtime.getRuntime().gc();
    }

    public void reusabeTreeTestCase(String caseName, int numberOfElements) throws Exception {
        tml.setTestCaseName(caseName);
        
        SSearchTree<PayLoad> tree = new SSearchTree(new PayLoad("Load 0"));
        
        for (int i = 1; i < numberOfElements; i++)
        {
            tree.Add(new PayLoad("Load "+i));
            tml.logTimeAndMemoryUsage(i+1);
        }
        
        
    }
    
    @Test
    public void testInsert100Nodes() throws Exception {
	String caseID = "SSearchTreeTest 100 insert";
	reusabeTreeTestCase(caseID, 100);
		
    }
    
    @Test
    public void testInsert1000Nodes() throws Exception {
	String caseID = "SSearchTreeTest 1000 insert";
	reusabeTreeTestCase(caseID, 1000);
		
    }
    
     @Test
    public void testInsert10000Nodes() throws Exception {
	String caseID = "SSearchTreeTest 10000 insert";
	reusabeTreeTestCase(caseID, 10000);
		
    }
    
    @Test
    public void FindElementIn100() throws Exception {
        tml.setTestCaseName("Find elements in 100");
        
        SSearchTree<PayLoad> tree = createTreeOfSize(100);

        PayLoad toBeFound_25 = new PayLoad("Load 25");
        PayLoad toBeFound_50 = new PayLoad("Load 50");
        PayLoad toBeFound_75 = new PayLoad("Load 75");
        PayLoad toBeFound_100 = new PayLoad("Load 99");
        tml.setInitialTimeAndMemory();
        tree.Contains(toBeFound_25);
        tml.logTimeAndMemoryUsage(25);
        tree.Contains(toBeFound_50);
        tml.logTimeAndMemoryUsage(50);
        tree.Contains(toBeFound_75);
        tml.logTimeAndMemoryUsage(75);
        tree.Contains(toBeFound_100);
        tml.logTimeAndMemoryUsage(100);
        
    }

    private SSearchTree<PayLoad> createTreeOfSize(int size) throws Exception {
        SSearchTree<PayLoad> tree = new SSearchTree(new PayLoad("Load 0"));
        
        for (int i = 1; i < size; i++)
        {
            tree.Add(new PayLoad("Load "+i));
            
        }
        
        return tree;
    }
    
    @Test
    public void deepCopyAndFindTest() throws Exception{
        tml.setTestCaseName("DeepCopyAndFindIn100");
        SSearchTree<PayLoad> tree = createTreeOfSize(100);
        
        tml.setInitialTimeAndMemory();
        SSearchTree<PayLoad> treeCpy = tree;
        treeCpy.Contains(new PayLoad("Load 25"));
        tml.logTimeAndMemoryUsage(25);
        ///////////
        tree = createTreeOfSize(100);
        Runtime.getRuntime().gc();
        tml.setInitialTimeAndMemory();
        treeCpy = tree;
        treeCpy.Contains(new PayLoad("Load 50"));
        tml.logTimeAndMemoryUsage(50);
        ///////////////
        tree = createTreeOfSize(100);
        Runtime.getRuntime().gc();
        tml.setInitialTimeAndMemory();
        treeCpy = tree;
        treeCpy.Contains(new PayLoad("Load 75"));
        tml.logTimeAndMemoryUsage(75);
        //////////////
        tree = createTreeOfSize(100);
        Runtime.getRuntime().gc();
        tml.setInitialTimeAndMemory();
        treeCpy = tree;
        treeCpy.Contains(new PayLoad("Load 99"));
        tml.logTimeAndMemoryUsage(100);


        
        
    }
}
