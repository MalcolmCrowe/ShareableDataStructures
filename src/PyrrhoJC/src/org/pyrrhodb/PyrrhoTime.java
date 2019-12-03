package org.pyrrhodb;
import java.util.*;

public class PyrrhoTime
{
	public long hours;
	public long minutes;
	public long seconds;
	public long ticks;
	public static long TicksPerSecond()
	{
		return 10000000;
	}
	public PyrrhoTime(long t)
	{
		ticks = t;
		seconds = ticks / PyrrhoTime.TicksPerSecond();
		minutes = seconds / 60; seconds = seconds % 60;
		hours = minutes / 60; minutes = minutes % 60;
	}
	public String Format()
	{
		return "TIME '" + digits(hours) + ":" +
			digits(minutes) + ":" + digits(seconds) + "'";	}
	public String toString()
	{
		return Format();
	}
	String digits(long x)
	{
		if (x > 10)
			return ""+x;
		return "0" + x;
	}
}