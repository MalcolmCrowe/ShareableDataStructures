using System.Text;
using Pyrrho.Common;
using Pyrrho.Level5;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Security
{
#if (!EMBEDDED) || CANREMOTE || WINDOWS_PHONE
    /// <summary>
    /// A class for doing encrypted i/o for connection strings over the Internet
    /// </summary>
	public class Crypt
	{
        /// <summary>
        /// The stream on which encryption is taking place
        /// </summary>
		public Stream stream;
        /// <summary>
        /// whether we are using open-source algorithms
        /// </summary>
        public const bool openSource = true;
        /// <summary>
        /// The encryption key
        /// </summary>
		public long key
		{
			set { state = value; }
		}
        /// <summary>
        /// the current statem of the coding engine
        /// </summary>
		long state;
        /// <summary>
        /// The multiplier used for encoding
        /// </summary>
        readonly long mult = 73928681;
        /// <summary>
        /// Create the Crypt instance
        /// </summary>
        /// <param name="s">The stream to use</param>
		public Crypt(Stream s)
		{
			stream = s;
		}
        /// <summary>
        /// Send an encrypted connection string
        /// </summary>
        /// <param name="cs">the connection string</param>
		public void SendConnectionString(string cs,string us)
		{
			key = GetLong(stream); // nonce
			stream.WriteByte(0);
			string[] fields = cs.Split(';');
            string f;
			Send(Connecting.User,us);
			for (int j=0;j<fields.Length;j++)
			{
				f = fields[j];
				int m = f.IndexOf('=');
				if (m<0)
					goto bad;
				string n = f[0..m];
				string v = f[(m+1)..];
				switch (n)
				{
					case "Provider": break;
                    case "Host": Send(Connecting.Host, v); break;
					case "Port": break;
					case "Files": Send(Connecting.Files, v); break;
					case "Role": Send(Connecting.Role, v); break;
					case "Stop": Send(Connecting.Stop, v); break;
                    case "User": Send(Connecting.User, v); break;
                    case "Base": Send(Connecting.Base, v); break;
					case "Locale": Send(Connecting.Culture, v); break;
                    case "BaseServer": Send(Connecting.BaseServer, v); break;
                    case "Coordinator": Send(Connecting.Coordinator, v); break;
                    case "Password": Send(Connecting.Password, v); break;
                    case "Modify": Send(Connecting.Modify, v); break;
                    case "Length": Send(Connecting.Length, v); break;
					case "CaseSensitive": Send(Connecting.CaseSensitive, v); break;
					case "Schema": Send(Connecting.Schema, v); break;
					case "Graph": Send(Connecting.Graph, v); break;
					default: goto bad;
				}
			}
			Send(Connecting.Done);
			return;
			bad:
				throw new PEException(f);//"PE27");
		}
        /// <summary>
        /// Send a byte and a string
        /// </summary>
        /// <param name="proto">the byte</param>
        /// <param name="text">the string</param>
        public void Send(Connecting proto, string text)
        {
            WriteByte((byte)proto);
            PutString(text);
            stream.Flush();
        }
        /// <summary>
        /// Send a byte
        /// </summary>
        /// <param name="proto">the byte</param>
        public void Send(Connecting proto)
        {
            WriteByte((byte)proto);
            stream.Flush();
        }
        /// <summary>
        /// Encrypt a byte
        /// </summary>
        /// <param name="b">the byte</param>
        /// <returns>the encrypted byte</returns>
		byte Encrypt(byte b)
		{
			byte c = (byte)(b+state);
			state = (state*mult)>>8;
            if (state == 0)
                state = 1;
			return c;
		}
        /// <summary>
        /// Decrypt a byte
        /// </summary>
        /// <param name="c">the byte</param>
        /// <returns>the decrypted byte</returns>
		byte Decrypt(byte c)
		{
			byte b = (byte)(c-state);
			state = (state*mult)>>8;
            if (state == 0)
                state = 1;
            return b;
		}
        /// <summary>
        /// Encrypt a byte array and write it
        /// </summary>
        /// <param name="b">the byte array</param>
        /// <param name="n">the number of bytes</param>
		public void Write(byte[] b,int n) // b is in cleartext
		{
			for (int j=0;j<n;j++)
				b[j] = Encrypt(b[j]);
			stream.Write(b,0,n);// b is in cyphertext
		}
        /// <summary>
        /// Encrypt a byte and write it
        /// </summary>
        /// <param name="b">the byte</param>
		public void WriteByte(byte b)
		{
			stream.WriteByte(Encrypt(b));
		}
        /// <summary>
        /// Read and decrypt a byte array
        /// </summary>
        /// <param name="b">the buffer to use</param>
        /// <param name="len">The buffer length</param>
        /// <returns>the number of bytes read</returns>
		public int Read(byte[] b,int len) 
		{
			int n = stream.Read(b,0,len);// b is in cyphertext
			for (int j=0;j<n;j++)
				b[j] = Decrypt(b[j]);
			return n;// b is in cleartext
		}
        /// <summary>
        /// Read a byte and decrypt it
        /// </summary>
        /// <returns>the byte</returns>
		public int ReadByte()
		{
			int n = stream.ReadByte();
			if (n<0)
				return n;
			return (int)Decrypt((byte)n);
		}
        /// <summary>
        /// Get an integer from the encrypted stream 
        /// </summary>
        /// <returns>the integer</returns>
		public int GetInt()
		{
			byte[] bytes = new byte[4];
			Read(bytes,4);
			int n = 0;
			for (int j=0;j<4;j++)
				n = (n<<8)+bytes[j];
			return n;
		}
        /// <summary>
        /// Get a long from the encrypted stream
        /// </summary>
        /// <param name="stream">the stream</param>
        /// <returns>the long</returns>
		static long GetLong(Stream stream)
		{
			byte[] bytes = new byte[8];
			stream.Read(bytes,0,8);
			long n = 0L;
			for (int j=0;j<8;j++)
				n = (n<<8)+bytes[j];
			return n;
		}
        /// <summary>
        /// Send an int to the encrypted stream
        /// </summary>
        /// <param name="n">the int</param>
		public void PutInt(int n)
		{
			byte[] b = new byte[4];
			b[0] = (byte)(n>>24);
			b[1] = (byte)(n>>16);
			b[2] = (byte)(n>>8);
			b[3] = (byte)n;
			Write(b,4);
		}
        /// <summary>
        /// Get s string from the encrypted stream
        /// </summary>
        /// <returns>the string</returns>
		public string GetString()
		{
			int n = GetInt();
			byte[] bytes = new byte[n];
			Read(bytes,n);
			return Encoding.UTF8.GetString(bytes,0,bytes.Length);
		}
        /// <summary>
        /// Send a string to the encrypted stream
        /// </summary>
        /// <param name="text">the string to send</param>
        public void PutString(string text)
		{
			byte[] bytes = Encoding.UTF8.GetBytes(text);
			int n = bytes.Length;
			PutInt(n);
			Write(bytes,n);
		}
        /// <summary>
        /// Close the Crypt instance and the stream
        /// </summary>
		public void Close()
		{
			stream.Close();
		}
	}
#endif
}
