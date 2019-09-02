using System.Text;
using System.IO;
using Pyrrho.Common;

// Pyrrho Database Service by Malcolm Crowe at the Unbiversity of Paisley
// (c) Malcolm Crowe, University of Paisley 2004-2006
//
// Patent Applied For:
// This software incorporates and is a sample implementation of JournalDB technology covered by 
// British Patent Application No 0620986.0 in the name of the University of Paisley
// entitled "Improvements in and Relating to Database Technology"
// 
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of Paisley

// OPEN SOURCE EDITIONS
namespace Pyrrho.Security
{
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
        long mult = 73928681;
        /// <summary>
        /// Create the Crypt instance
        /// </summary>
        /// <param name="s">The stream to use</param>
        public Crypt(Stream s)
        {
            stream = s;
        }
        /// <summary>
        /// Encrypt a byte
        /// </summary>
        /// <param name="b">the byte</param>
        /// <returns>the encrypted byte</returns>
        byte Encrypt(byte b)
        {
            byte c = (byte)(b + state);
            state = (state * mult) >> 8;
            return c;
        }
        /// <summary>
        /// Decrypt a byte
        /// </summary>
        /// <param name="c">the byte</param>
        /// <returns>the decrypted byte</returns>
        byte Decrypt(byte c)
        {
            byte b = (byte)(c - state);
            state = (state * mult) >> 8;
            return b;
        }
        /// <summary>
        /// Encrypt a byte array and write it
        /// </summary>
        /// <param name="b">the byte array</param>
        /// <param name="n">the number of bytes</param>
        public void Write(byte[] b, int n) // b is in cleartext
        {
            for (int j = 0; j < n; j++)
                b[j] = Encrypt(b[j]);
            stream.Write(b, 0, n);// b is in cyphertext
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
        public int Read(byte[] b, int len)
        {
            int n = stream.Read(b, 0, len);// b is in cyphertext
            for (int j = 0; j < n; j++)
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
            if (n < 0)
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
            Read(bytes, 4);
            int n = 0;
            for (int j = 0; j < 4; j++)
                n = (n << 8) + bytes[j];
            return n;
        }
        /// <summary>
        /// Get a string from the encrypted stream
        /// </summary>
        /// <returns>the string</returns>
        public string GetString()
        {
            int n = GetInt();
            byte[] bytes = new byte[n];
            Read(bytes, n);
            return Encoding.ASCII.GetString(bytes, 0, bytes.Length);
        }
        /// <summary>
        /// Send an int to the encrypted stream
        /// </summary>
        /// <param name="n">the int</param>
        public void PutInt(int n)
        {
            byte[] b = new byte[4];
            b[0] = (byte)(n >> 24);
            b[1] = (byte)(n >> 16);
            b[2] = (byte)(n >> 8);
            b[3] = (byte)n;
            Write(b, 4);
        }
        /// <summary>
        /// Send a string to the encrypted stream
        /// </summary>
        /// <param name="text">the string to send</param>
        public void PutString(string text)
        {
            byte[] bytes = Encoding.ASCII.GetBytes(text);
            int n = bytes.Length;
            PutInt(n);
            Write(bytes, n);
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
        /// Close the Crypt instance and the stream
        /// </summary>
        public void Close()
        {
            stream.Close();
        }
    }
}