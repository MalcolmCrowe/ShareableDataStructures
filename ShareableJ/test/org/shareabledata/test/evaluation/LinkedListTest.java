package org.shareabledata.test.evaluation;

import org.shareabledata.test.common.TimeAndMemoryLogger;
import org.shareabledata.test.common.PayLoad;
import java.io.IOException;
import java.util.LinkedList;
import org.junit.After;
import org.shareabledata.test.common.*;
import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class LinkedListTest {
	

	private static TimeAndMemoryLogger tml;
	
	@BeforeClass
	public static void setUpClass() {
		tml = new TimeAndMemoryLogger();
	}

	@AfterClass
	public static void tearDownClass() {
                
		try {
			tml.writeToCSV("LinkedListTestOutput.csv");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
        
        @After
        public void tearDown() {
            Runtime.getRuntime().gc();
        }

/**
 * Main body of the test case by
 * @param caseID
 */
	private void reusabeLinkedListTestCase(String caseName, int numberOfElements) {
		tml.setTestCaseName(caseName);
				
		LinkedList<PayLoad> list = new LinkedList<PayLoad>();
		tml.resetCounters();
		tml.setInitialTimeAndMemory();
		for (int i = 0; i < numberOfElements; i++)
		{
			list.add(new PayLoad("Load "+i));
			
			tml.logTimeAndMemoryUsage();
		}
		
	}
	
	@Test
	public void testInsert100LinkedList() {
		String caseID = "LinkedList 100 insert";
		reusabeLinkedListTestCase(caseID, 100);
		
	}

	
	@Test
	public void testInsert1000LinkedList() {
		String caseID = "LinkedList 1000 insert";
		
		reusabeLinkedListTestCase(caseID, 1000);
		
	}
	
	@Test
	public void testInsert10000LinkedList() {
		String caseID = "LinkedList 10000 insert";
		
		reusabeLinkedListTestCase(caseID, 10000);
		
	}
	
	

}
