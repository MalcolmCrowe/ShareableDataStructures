package org.pyrrhodb;
import java.io.*;
import java.util.*;

public class Crypt
{
	PyrrhoInputStream is;
	PyrrhoOutputStream os;
        long state;
        long mult=73928681;
//	PyrrhoInteger state;
//	PyrrhoInteger mult = new PyrrhoInteger(new byte[] { 89, 113, -5, -52, 59, -93, -25, 49 });
	Crypt(PyrrhoInputStream i,PyrrhoOutputStream o)
	{
		is = i; os = o;
	}
        void sendConnectionString(HashMap<String,String>properties) throws IOException
        {
            state = GetLong(is);
            os.write(0);
            Iterator<Map.Entry<String,String>> it = properties.entrySet().iterator();
            while(it.hasNext())
            {
                byte b = 0;
                Map.Entry<String,String> e = it.next();
                String n = e.getKey();
                String v = e.getValue();
                if (n.equals("Provider") || n.equals("Port"))
                    continue;
                if (n.equals("Host"))
                    b = (byte)26;
                else if (n.equals("Files"))
                    b = (byte)22;
                else if (n.equals("Role"))
                    b = (byte)23;
                else if (n.equals("Stop"))
                    b =(byte)25;
                else if (n.equals("User"))
                    b = (byte)21;
                else throw new IOException();
                Send(b,v);
            }
            Send((byte)24);
        }
	byte Encrypt(byte b)
	{
		byte c = (byte)(b + state); //.last());
		state = (state*mult)>>8; //PyrrhoInteger.times(state, mult).high();
                if (state==0)
                    state = 1;
		return c;
	}
	byte Decrypt(byte c)
	{
		byte b = (byte)(c - state); //.last());
		state = (state*mult)>>8; //PyrrhoInteger.times(state, mult).high();
                if (state==0)
                    state = 1;
		return b;
	}
	void write(byte[] b, int n) throws IOException // b is in cleartext
	{
		for (int j = 0; j < n; j++)
			b[j] = Encrypt(b[j]);
		os.write(b, 0, n); // b is in cyphertext
	}
	void write(byte b) throws IOException
	{
		os.write(Encrypt(b));
	}
	int read(byte[] b, int len) throws IOException
	{
		int n = is.read(b, 0, len); // b is in cyphertext
		for (int j = 0; j < n; j++)
			b[j] = Decrypt(b[j]);
		return n; // b is in cleartext
	}
	int read() throws IOException
	{
		int n = is.read();
		if (n < 0)
			return n;
		return (int)Decrypt((byte)n);
	}
	int GetInt() throws IOException
	{
		byte[] bytes = new byte[4];
		read(bytes, 4);
		int n = 0;
		for (int j = 0; j < 4; j++)
		{
			int b = (int)bytes[j];
			if (b < 0)
				b += 256;
			n = (n << 8) + b;
		}
		return n;
	}
        // read WITHOUT decrypting!
        long GetLong(InputStream s) throws IOException
	{
		long n = 0;
		for (int j = 0; j < 8; j++)
		{
			int b = (int)s.read();
			if (b < 0)
				b += 256;
			n = (n << 8) + b;
		}
		return n;
	}
	String GetString() throws IOException
	{
		int n = GetInt();
		byte[] bytes = new byte[n];
		read(bytes, n);
		return new String(bytes);
	}
	void PutInt(int n) throws IOException
	{
		byte[] b = new byte[4];
		b[0] = (byte)(n >> 24);
		b[1] = (byte)(n >> 16);
		b[2] = (byte)(n >> 8);
		b[3] = (byte)n;
		write(b, 4);
	}
	void PutString(String text) throws IOException
	{
		byte[] bytes = text.getBytes();
		int n = bytes.length;
		PutInt(n);
		write(bytes, n);
	}
	void Send(byte proto, String text) throws IOException
	{
		write(proto);
		PutString(text);
		os.flush();
	}
        void Send(byte proto) throws IOException
	{
		write(proto);
		os.flush();
	}
	void close() throws IOException
	{
		is.close();
		os.close();
	}
}
