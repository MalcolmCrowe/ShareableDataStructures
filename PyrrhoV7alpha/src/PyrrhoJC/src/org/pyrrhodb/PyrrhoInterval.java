package org.pyrrhodb;
import java.util.*;

public class PyrrhoInterval
{
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
		seconds = ticks / PyrrhoInterval.TicksPerSecond();
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