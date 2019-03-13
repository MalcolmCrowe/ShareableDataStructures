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
public class TestTreeTest {
    
    private static TimeAndMemoryLogger tml;
    
    
    public TestTreeTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
        tml = new TimeAndMemoryLogger();
    }
    
    @AfterClass
    public static void tearDownClass() {
        try {
            tml.writeToCSV("TestTreeTestOutput_Java.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
       
    @After
    public void tearDown() {
        Runtime.getRuntime().gc();
    }

    
    public void reusabeTreeTestCase(String caseName, int numberOfElements){
        tml.setTestCaseName(caseName);
        
        tml.setInitialTimeAndMemory();
        DietelJavaTree<PayLoad> tree = new DietelJavaTree<PayLoad>();
        
        for (int i = 0; i < numberOfElements; i++){
            tree.insertNode(new PayLoad("Load "+i));
            tml.logTimeAndMemoryUsage(i+1);
        }
        
        
        
    }
    
    @Test
    public void testInsert100LinkedList() {
	String caseID = "TestTreeTest 100 insert";
	reusabeTreeTestCase(caseID, 100);
		
    }
    
    @Test
    public void testInsert1000LinkedList() {
	String caseID = "TestTreeTest 1000 insert";
	reusabeTreeTestCase(caseID, 1000);
		
    }
    
    @Test
    public void testInsert10000LinkedList() {
	String caseID = "TestTreeTest 10000 insert";
	reusabeTreeTestCase(caseID, 10000);
		
    }
    
     @Test
    public void FindElementIn100() throws Exception {
        tml.setTestCaseName("Find elements in 100");
        
        DietelJavaTree<PayLoad> tree = createTreeOfSize(100);

        PayLoad toBeFound_25 = new PayLoad("Load 25");
        PayLoad toBeFound_50 = new PayLoad("Load 50");
        PayLoad toBeFound_75 = new PayLoad("Load 75");
        PayLoad toBeFound_100 = new PayLoad("Load 99");
        boolean testOutput = false;
        tml.setInitialTimeAndMemory();
        testOutput = tree.contains(toBeFound_25);
        
        tml.logTimeAndMemoryUsage(25);
        testOutput = tree.contains(toBeFound_50);
        
        
        tml.logTimeAndMemoryUsage(50);
        testOutput = tree.contains(toBeFound_75);
        
        tml.logTimeAndMemoryUsage(75);
        
        testOutput = tree.contains(toBeFound_100);
        
        tml.logTimeAndMemoryUsage(100);
        
    }

    private DietelJavaTree<PayLoad> createTreeOfSize(int size) throws Exception {
        DietelJavaTree<PayLoad> tree = new DietelJavaTree();
        
        for (int i = 1; i < size; i++)
        {
            tree.insertNode(new PayLoad("Load "+i));
            
        }
        
        return tree;
    }
    
    @Test
    public void deepCopyAndFindTest() throws Exception{
        tml.setTestCaseName("DeepCopyAndFindIn100");
        DietelJavaTree<PayLoad> tree = createTreeOfSize(100);
        
        
        
        tml.setInitialTimeAndMemory();
        DietelJavaTree<PayLoad> treeCpy = TreeDeepCopy.deepCopy(tree);
        treeCpy.contains(new PayLoad("Load 25"));
        tml.logTimeAndMemoryUsage(25);
        ///////////
        tree = createTreeOfSize(100);
        Runtime.getRuntime().gc();
        
        tml.setInitialTimeAndMemory();
        treeCpy = tree;
        treeCpy.contains(new PayLoad("Load 50"));
        tml.logTimeAndMemoryUsage(50);
        ///////////////
        tree = createTreeOfSize(100);
        Runtime.getRuntime().gc();
        tml.setInitialTimeAndMemory();
        treeCpy = tree;
        treeCpy.contains(new PayLoad("Load 75"));
        tml.logTimeAndMemoryUsage(75);
        //////////////
        tree = createTreeOfSize(100);
        Runtime.getRuntime().gc();
        tml.setInitialTimeAndMemory();
        treeCpy = tree;
        treeCpy.contains(new PayLoad("Load 99"));
        tml.logTimeAndMemoryUsage(100);
                
    }
}
