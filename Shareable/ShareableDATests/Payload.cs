using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace ShareableDATests
{
    public class Payload : ICloneable, IComparable<Payload>
    {
        private String _payload;

        public Payload(string payload)
        {
            this._payload = payload;
            
        }

        public string PayloadContent { get => _payload; set => _payload = value; }

        public object Clone()
        {
            return new Payload(this.PayloadContent);
        }

        public int CompareTo(Payload other)
        {
            return this.PayloadContent.CompareTo(other.PayloadContent);
        }

        public override bool Equals(object obj)
        {
            var payload = obj as Payload;
            return payload != null &&
                   _payload == payload._payload &&
                   PayloadContent == payload.PayloadContent;
        }

        public override int GetHashCode()
        {
            var hashCode = 106508454;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(_payload);
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(PayloadContent);
            return hashCode;
        }
    }
}
