/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata.test.evaluation;

import java.io.IOException;
import java.util.LinkedList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.shareabledata.SList;


import org.shareabledata.test.common.*;


/**
 *
 * @author Santiago
 */
public class SListTest {
    
    private static TimeAndMemoryLogger tml;
    
    @BeforeClass
    public static void setUpClass() {
        tml = new TimeAndMemoryLogger();
    }
    
    @AfterClass
    public static void tearDownClass() {
        try {
            tml.writeToCSV("SListTestOutput.csv");
	} catch (IOException e) {
	e.printStackTrace();
	}
    }
    
      
    @After
    public void tearDown() {
        Runtime.getRuntime().gc();
    }

    private void reusableSlistTestCase(String caseName, int numberOfElements) throws Exception {
        tml.setTestCaseName(caseName);
        
        SList<PayLoad> list;
        PayLoad firstElement = new PayLoad("Load 0");
        tml.setInitialTimeAndMemory();
        list = new SList<PayLoad>(firstElement);
        tml.logTimeAndMemoryUsage(1);
        
        for (int i = 1; i < numberOfElements; i++)
        {
            
            list.InsertAt(new PayLoad("Load "+i), 0);

            tml.logTimeAndMemoryUsage(i+1);
        }
        
    }
   
    @Test
    public void testInsert100Slist() throws Exception {
        String caseID = "SList 100 insert";
	reusableSlistTestCase(caseID, 100);
    }
    
     @Test
    public void testInsert1000Slist() throws Exception {
        String caseID = "SList 1000 insert";
	reusableSlistTestCase(caseID, 1000);
    }
    
     @Test
    public void testInsert10000Slist() throws Exception {
        String caseID = "SList 10000 insert";
	reusableSlistTestCase(caseID, 10000);
    }
    
    @Test
    public void find25thElementIn100() throws Exception{
                tml.setTestCaseName("Find elements in 100");
				
		SList<PayLoad> list = this.creatASListWithNelement(100);
                PayLoad toBeFound_25 = new PayLoad("Load 25");
                PayLoad toBeFound_50 = new PayLoad("Load 50");
                PayLoad toBeFound_100 = new PayLoad("Load 100");
                tml.setInitialTimeAndMemory();
                //list.contains(toBeFound_25);
                tml.logTimeAndMemoryUsage(25);
                //list.contains(toBeFound_50);
                tml.logTimeAndMemoryUsage(50);
                //list.contains(toBeFound_100);
                tml.logTimeAndMemoryUsage(100);
    }
    
    @Test
    public void RemoveElementIn100() throws Exception {
                tml.setTestCaseName("Remove elements in 100");
				
		SList<PayLoad> list = this.creatASListWithNelement(100);
                PayLoad toBeFound_25 = new PayLoad("Load 25");
                PayLoad toBeFound_50 = new PayLoad("Load 50");
                PayLoad toBeFound_75 = new PayLoad("Load 75");
                
                PayLoad toBeFound_100 = new PayLoad("Load 100");
                Assert.assertEquals(100, list.Length);
                tml.setInitialTimeAndMemory();
                list = list.RemoveAt(24);
                Assert.assertEquals(99, list.Length);
                tml.logTimeAndMemoryUsage(25);
                list = list.RemoveAt(48);
                Assert.assertEquals(98, list.Length);
                tml.logTimeAndMemoryUsage(50);
                list = list.RemoveAt(73);
                Assert.assertEquals(97, list.Length);
                tml.logTimeAndMemoryUsage(75);
                list = list.RemoveAt(96);
                Assert.assertEquals(96, list.Length);
                tml.logTimeAndMemoryUsage(100);
    }
    
    
    public void DeepCopyAndAddIn100() throws Exception {
                tml.setTestCaseName("DeepCopyAndAddIn100 100");
				
		SList<PayLoad> list = this.creatASListWithNelement(100);
               
                
                tml.setInitialTimeAndMemory();
                SList<PayLoad>listCpy = list;
                listCpy.InsertAt(new PayLoad("Load 25*"), 25);
                tml.logTimeAndMemoryUsage(25);
                ///////////
                tml.setInitialTimeAndMemory();
                listCpy = list;
                listCpy.InsertAt(new PayLoad("Load 50*"), 50);
                tml.logTimeAndMemoryUsage(50);
                ///////////////
                tml.setInitialTimeAndMemory();
                listCpy = list;
                listCpy.InsertAt(new PayLoad("Load 75*"), 75);
                tml.logTimeAndMemoryUsage(75);
                //////////////
                tml.setInitialTimeAndMemory();
                listCpy = list;
                listCpy.InsertAt(new PayLoad("Load 99*"), 99);
                tml.logTimeAndMemoryUsage(100);
                
                
                
                
    }
    
    private SList<PayLoad> creatASListWithNelement(int numberOfElements) throws Exception {
            SList<PayLoad> list;
            PayLoad firstElement = new PayLoad("Load 0");         
            list = new SList<PayLoad>(firstElement);

            for (int i = 1; i < numberOfElements; i++)
            {
                list = list.InsertAt(new PayLoad("Load "+i), 0);
            }
            return list;
        }
}
