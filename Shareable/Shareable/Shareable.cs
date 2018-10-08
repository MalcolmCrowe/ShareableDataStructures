using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shareable
{
    public interface Shareable<T>
    {
        Bookmark<T> First();
    }
    public interface Bookmark<T>
    {
        Bookmark<T> Next();
        T Value();
        int Position(); // >=0
    }
}
