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

public class LinkedListTest {
	

	private static TimeAndMemoryLogger tml;
	
	@BeforeClass
	public static void setUpClass() {
		tml = new TimeAndMemoryLogger();
	}

	@AfterClass
	public static void tearDownClass() {
                
		try {
			tml.writeToCSV("LinkedListTestOutput_Java.csv");
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
			
			tml.logTimeAndMemoryUsage(i+1);
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
	
	
        @Test
        public void find25thElementIn100(){
                tml.setTestCaseName("Find elements in 100");
				
		LinkedList<PayLoad> list = this.creatAlistWithNelement(100);
                PayLoad toBeFound_25 = new PayLoad("Load 25");
                PayLoad toBeFound_50 = new PayLoad("Load 50");
                PayLoad toBeFound_75 = new PayLoad("Load 75");
                PayLoad toBeFound_100 = new PayLoad("Load 100");
                tml.setInitialTimeAndMemory();
                list.contains(toBeFound_25);
                tml.logTimeAndMemoryUsage(25);
                list.contains(toBeFound_50);
                tml.logTimeAndMemoryUsage(50);
                list.contains(toBeFound_75);
                tml.logTimeAndMemoryUsage(75);
                list.contains(toBeFound_100);
                tml.logTimeAndMemoryUsage(100);
                
        }
        
        @Test
        public void removeElementIn100(){
                tml.setTestCaseName("Remove elements in 100");
				
		LinkedList<PayLoad> list = this.creatAlistWithNelement(100);
                PayLoad toBeFound_25 = new PayLoad("Load 25");
                PayLoad toBeFound_50 = new PayLoad("Load 50");
                PayLoad toBeFound_75 = new PayLoad("Load 75");
                PayLoad toBeFound_100 = new PayLoad("Load 100");
                tml.setInitialTimeAndMemory();
                list.remove(24);
                Assert.assertEquals(99, list.size());
                tml.logTimeAndMemoryUsage(25);
                list.remove(48);
                Assert.assertEquals(98, list.size());
                tml.logTimeAndMemoryUsage(50);
                list.remove(73);
                Assert.assertEquals(97, list.size());
                tml.logTimeAndMemoryUsage(75);
                list.remove(96);
                Assert.assertEquals(96, list.size());
                tml.logTimeAndMemoryUsage(100);
                
        }
        
        @Test
        public void DeepCopyAndAddIn100() throws Exception {
                tml.setTestCaseName("DeepCopyAndAddIn100");
				
		LinkedList<PayLoad> list = this.creatAlistWithNelement(100);
               
                
                tml.setInitialTimeAndMemory();
                LinkedList<PayLoad> listCpy = this.deepCopy(list);
                listCpy.add(25, new PayLoad("Load 25*"));
                tml.logTimeAndMemoryUsage(25);
                ///////////
                list = this.creatAlistWithNelement(100);
                Runtime.getRuntime().gc();
                tml.setInitialTimeAndMemory();
                listCpy = this.deepCopy(list);
                listCpy.add(50, new PayLoad("Load 50*"));
                tml.logTimeAndMemoryUsage(50);
                ///////////////
                list = this.creatAlistWithNelement(100);
                Runtime.getRuntime().gc();
                tml.setInitialTimeAndMemory();
                listCpy = this.deepCopy(list);
                listCpy.add(75, new PayLoad("Load 75*"));
                tml.logTimeAndMemoryUsage(75);
                //////////////
                list = this.creatAlistWithNelement(100);
                Runtime.getRuntime().gc();
                tml.setInitialTimeAndMemory();
                listCpy = this.deepCopy(list);
                listCpy.add(99, new PayLoad("Load 99*"));
                tml.logTimeAndMemoryUsage(100);
                
                
                
                
    }
        
        private LinkedList<PayLoad> creatAlistWithNelement(int numberOfElements){
            LinkedList<PayLoad> list = new LinkedList<PayLoad>();
		for (int i = 0; i < 100; i++)
		{
                    list.add(new PayLoad("Load "+i));
		}
            return list;
        }

    private LinkedList<PayLoad> deepCopy(LinkedList<PayLoad> list) {
        return LinkedListCopy.deepCopy(list);//Todo
    }

}
