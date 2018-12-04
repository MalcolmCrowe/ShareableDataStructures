package org.shareabledata.test.common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import com.opencsv.*;

public class TimeAndMemoryLogger {
	private long initialTime = 0L;
	private String testCase;

	private long totalMemory = 0L;
	private long freeMemory = 0L;
	
	private ArrayList<String[]> contents;
	
	public TimeAndMemoryLogger() {
		contents = new ArrayList<String[]>();
		contents.add(new String[] {"Test Case ID", "CaseCounter", "TimeDifference", "TotalMemory", "FreeMemory"});
	}
	
	public void resetCounters() {
		this.initialTime = 0L;
	
		this.totalMemory = 0L;
		this.freeMemory = 0L;
	}
	
	public void setTestCaseName(String testCaseName) {
		this.testCase = testCaseName;
	}
	
	public void setInitialTimeAndMemory() {
		this.initialTime = System.nanoTime();
		this.totalMemory = Runtime.getRuntime().totalMemory();
		this.freeMemory = Runtime.getRuntime().freeMemory();
		contents.add(new String[] {testCase, ""+0,
									0+"",
									totalMemory+"",
									freeMemory+""}
					);
		
						
	}
	
	public void logTimeAndMemoryUsage(int caseIdNumber) {
		long endTime = System.nanoTime();
		long endTotalMemory = Runtime.getRuntime().totalMemory();
		long endFreeMemory = Runtime.getRuntime().freeMemory();
		
		
		contents.add(new String[] {testCase, ""+caseIdNumber,
									(endTime-this.initialTime)+"",
									endTotalMemory+"",
									endFreeMemory+""}
					);
			
	}
	
	public void writeToCSV(String FileName) throws IOException {
		String help = System.getProperty("user.dir");
		help = help + "\\execution\\";
		System.out.println(help);
		
		File outputFile = new File(help+FileName);
		FileWriter outputFileWriter = new FileWriter(outputFile);
		CSVWriter outputCSVWriter = new CSVWriter(outputFileWriter);
		outputCSVWriter.writeAll(contents);
		outputCSVWriter.close();
		
		
	}
	
	
	
		
}
