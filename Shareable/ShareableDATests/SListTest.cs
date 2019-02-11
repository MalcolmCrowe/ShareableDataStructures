using NUnit.Framework;
using Shareable;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShareableDATests
{
    [TestFixture]
    public class SListTest
    {
        private static TimeAndMemoryLogger tml;

        [OneTimeSetUp]
        public static void setUpClass()
        {
            tml = new TimeAndMemoryLogger();
        }

        [OneTimeTearDown]
        public static void tearDownClass()
        {

            try
            {
                tml.writeToCSV("LinkedListTestOutput_CSharp.csv");
            }
            catch (Exception e)
            {
                // TODO Auto-generated catch block
                Console.Write(e.StackTrace);
                throw e;
            }
        }

        [TearDown]
        public void tearDown()
        {
            callTheGC();
        }

        private void callTheGC()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        private void reusableSlistTestCase(String caseName, int numberOfElements)
        {
            tml.setTestCaseName(caseName);

            SList<Payload> list;
            Payload firstElement = new Payload("Load 0");
            tml.setInitialTimeAndMemory();
            list = SList<Payload>.New(firstElement);


            tml.logTimeAndMemoryUsage(1);
        
            for (int i = 1; i<numberOfElements; i++)
            {
                (SList<Payload> operand, int pos) prevList = (SList<Payload>.New(new Payload("Load " + i)), i);

                //list = list + prevList;
                //list = list + (SList<Payload>.New(new Payload("Load " + i)), i);
                //list.InsertAt(new Payload("Load "+i), 0);

                tml.logTimeAndMemoryUsage(i+1);
            }

        }

        [Test]
        public void testInsert100Slist()
        {
            String caseID = "SList 100 insert";
            reusableSlistTestCase(caseID, 100);

        }

        [Test]
        public void testInsert1000Slist()
        {
            String caseID = "SList 1000 insert";
            reusableSlistTestCase(caseID, 1000);

        }

        [Test]
        public void testInsert10000Slist()
        {
            String caseID = "SList 10000 insert";
            reusableSlistTestCase(caseID, 10000);

        }


        /*[Test]
        public void find25thElementIn100()
        {
            tml.setTestCaseName("Find elements in 100");

            SList<Payload> list = this.creatAlistWithNelement(100);
            Payload toBeFound_25 = new Payload("Load 25");
            Payload toBeFound_50 = new Payload("Load 50");
            Payload toBeFound_75 = new Payload("Load 75");
            Payload toBeFound_100 = new Payload("Load 100");
            tml.setInitialTimeAndMemory();
            list.
            list.contains(toBeFound_25);
            tml.logTimeAndMemoryUsage(25);
            list.contains(toBeFound_50);
            tml.logTimeAndMemoryUsage(50);
            list.contains(toBeFound_75);
            tml.logTimeAndMemoryUsage(75);
            list.contains(toBeFound_100);
            tml.logTimeAndMemoryUsage(100);

        }*/

        private SList<Payload> creatASListWithNelement(int numberOfElements) 
        {
            SList<Payload> list;
            Payload firstElement = new Payload("Load 0");
            list = SList<Payload>.New(firstElement);

            for (int i = 1; i<numberOfElements; i++)
            {
                list += new Payload("Load "+i);
            }

            
            return list;
        }


        [Test]
        public void RemoveElementIn100() 
        {
            tml.setTestCaseName("Remove elements in 100");
				
		    SList<Payload> list = this.creatASListWithNelement(100);
            Payload toBeFound_25 = new Payload("Load 25");
            Payload toBeFound_50 = new Payload("Load 50");
            Payload toBeFound_75 = new Payload("Load 75");

            Payload toBeFound_100 = new Payload("Load 100");
            Assert.AreEqual(100, list.Length);
            tml.setInitialTimeAndMemory();
            list -= 25;//removes element at 25
            Assert.AreEqual(99, list.Length);
            tml.logTimeAndMemoryUsage(25);
            list -= 48;
            Assert.AreEqual(98, list.Length);
            tml.logTimeAndMemoryUsage(50);
            list -= 73;
            Assert.AreEqual(97, list.Length);
            tml.logTimeAndMemoryUsage(75);
            list -= 96;
            Assert.AreEqual(96, list.Length);
            tml.logTimeAndMemoryUsage(100);
         }

        public void DeepCopyAndAddIn100() 
        {
            tml.setTestCaseName("DeepCopyAndAddIn100");
				
		    SList<Payload> list = this.creatASListWithNelement(100);


            tml.setInitialTimeAndMemory();
            SList<Payload> listCpy = list;
            list += (new Payload("Load 25*"), 24);
            
            tml.logTimeAndMemoryUsage(25);
                ///////////
            list = this.creatASListWithNelement(100);
                
            tml.setInitialTimeAndMemory();
            listCpy = list;
            list += (new Payload("Load 50*"), 50);
            tml.logTimeAndMemoryUsage(50);
            ///////////////
            list = this.creatASListWithNelement(100);
            this.callTheGC();
            tml.setInitialTimeAndMemory();
            listCpy = list;
            list += (new Payload("Load 75*"), 75);
            tml.logTimeAndMemoryUsage(75);
            //////////////
            list = this.creatASListWithNelement(100);
            this.callTheGC();
            tml.setInitialTimeAndMemory();
            listCpy = list;
            list += (new Payload("Load 99*"), 99);
            
            tml.logTimeAndMemoryUsage(100);
                
                
                
                
        }

    }
}
