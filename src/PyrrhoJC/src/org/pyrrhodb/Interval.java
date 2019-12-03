/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;

/**
 *
 * @author 66668214
 */
public class Interval {
    	public int years;
	public int months;
	public long ticks;
	public static long TicksPerSecond()
	{
		return 10000000;
	}
	public String Format()
	{
		long seconds, minutes, hours, days;
		seconds = ticks / TicksPerSecond();
		minutes = seconds / 60; seconds = seconds % 60;
		hours = minutes / 60; minutes = minutes % 60;
		days = hours / 24; hours = hours % 24;
		return "INTERVAL '" + years + "-" + digits(months) +
			"-" + digits(days) + " " +
			digits(hours) + ":" +
			digits(minutes) + ":" + digits(seconds) +
			"'YEAR TO SECOND";
	}
	public String toString()
	{
		return "(" + years + "yr," + months + "mo," + ticks + "ti)";
	}
	String digits(long x)
	{
		if (x>10)
			return ""+x;
		return "0" + x;
	}
}
