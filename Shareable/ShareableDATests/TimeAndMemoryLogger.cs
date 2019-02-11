using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic.Devices;
using Microsoft.VisualBasic.FileIO;

namespace ShareableDATests
{
    public class TimeAndMemoryLogger
    {
        private long initialTime = 0L;
        private String testCase;

        private long totalMemory = 0L;
        private long freeMemory = 0L;

        StringBuilder contents;

        private const char delimiter = ',';
        public TimeAndMemoryLogger() {
            contents = new StringBuilder();
            contents.AppendLine("Test Case ID" + delimiter + "CaseCounter" + delimiter + "TimeDifference" + delimiter + "TotalMemory" + delimiter + "FreeMemory" );
        }

        public void resetCounters()
        {
            this.initialTime = 0L;

            this.totalMemory = 0L;
            this.freeMemory = 0L;
        }

        public void setTestCaseName(String testCaseName)
        {
            this.testCase = testCaseName;
        }


        public void setInitialTimeAndMemory()
        {

            ComputerInfo CI = new ComputerInfo();


            this.initialTime = TimeAndMemoryLogger.nanoTime();
            this.totalMemory = (long)CI.TotalPhysicalMemory; //I might be loosing presicion here?


            this.freeMemory = (long) CI.AvailablePhysicalMemory;
            contents.AppendLine(testCase+delimiter+
                            0+delimiter+
                            0+delimiter+
                            totalMemory+delimiter+
                            freeMemory);


        }

        public void logTimeAndMemoryUsage(int caseIdNumber)
        {
            ComputerInfo CI = new ComputerInfo();
            long endTime = TimeAndMemoryLogger.nanoTime();
            long endTotalMemory = (long)CI.TotalPhysicalMemory;
            long endFreeMemory = (long)CI.AvailablePhysicalMemory;


            contents.AppendLine(testCase+delimiter+
                            caseIdNumber+delimiter+
                            (endTime-this.initialTime)+delimiter+
                            endTotalMemory+delimiter+
                            endFreeMemory+delimiter);

        }

        public void writeToCSV(String FileName) 
        {
            string path = AppDomain.CurrentDomain.BaseDirectory;
            //Directory.GetCurrentDirectory(); points to VS execution
            path = Path.Combine(path, "execution", FileName);

            
            Console.WriteLine(path);

            StreamWriter Writer = new StreamWriter(path);



            Writer.Write(contents.ToString());
            Writer.Close();
        }

        private static long nanoTime()
        {
            long nano = 10000L * System.Diagnostics.Stopwatch.GetTimestamp();
            nano /= TimeSpan.TicksPerMillisecond;
            nano *= 100L;
            return nano;
        }





    }



    
}
