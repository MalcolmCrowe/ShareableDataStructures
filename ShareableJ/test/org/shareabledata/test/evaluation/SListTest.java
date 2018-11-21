/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata.test.evaluation;

import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.shareabledata.SList;
import org.shareabledata.test.common.PayLoad;
import org.shareabledata.test.common.TimeAndMemoryLogger;

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
        tml.logTimeAndMemoryUsage();
        
        for (int i = 0; i < numberOfElements; i++)
        {
            
            list.InsertAt(new PayLoad("Load "+i), 0);

            tml.logTimeAndMemoryUsage();
        }
        
    }
   
    @Test
    public void testInsert100Slist() throws Exception {
        String caseID = "SList 100 insert";
	reusableSlistTestCase(caseID, 100);
    }
    
     @Test
    public void testInsert1000Slist() throws Exception {
        String caseID = "LinkedList 1000 insert";
	reusableSlistTestCase(caseID, 1000);
    }
    
     @Test
    public void testInsert10000Slist() throws Exception {
        String caseID = "LinkedList 10000 insert";
	reusableSlistTestCase(caseID, 10000);
    }
    
}
