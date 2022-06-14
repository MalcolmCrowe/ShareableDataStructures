using System;
using Pyrrho;

[Table(104,273)]
/// <summary>
/// Class Customer from Database D, Role Sales
/// </summary>
public class Customer : Versioned
{
    [Key(0)]
    // autoKey enabled
    public Int64? ID;
    [Unique(216, 0)]
    public String NAME;
    public Orders[] orderss => conn.FindWith<Orders>(("CUST",ID));
}

[Table(377, 613)]
/// <summary>
/// Class Orders from Database D, Role Sales
/// </summary>
public class Orders : Versioned
{
    [Key(0)]
    // autoKey enabled
    public Int64? ID;
    public Int64 CUST;
    [Field(PyrrhoDbType.Date)]
    public Date OrderDate;
    [Field(PyrrhoDbType.Decimal, 6, 2)]
    public Decimal Total;
    public Customer customer => conn.FindOne<Customer>(CUST);
    public OrderItem[] orderItems => conn.FindWith<OrderItem>(("OID", ID));
}

[Table(777, 923)]
/// <summary>
/// Class Item from Database D, Role Sales
/// </summary>
public class Item : Versioned
{
    [Key(0)]
    // autoKey enabled
    public Int64? ID;
    public String NAME;
    [Field(PyrrhoDbType.Decimal, 6, 2)]
    public Decimal PRICE;
    public OrderItem[] orderItems => conn.FindWith<OrderItem>(("ITEM", ID));
}

[Table(1123, 1349)]
/// <summary>
/// Class OrderItem from Database D, Role Sales
/// </summary>
public class OrderItem : Versioned
{
    [Key(1)]
    // autoKey enabled
    public Int64? IT;
    [Key(0)]
    public Int64 OID;
    public Int64 ITEM;
    public Int64 QTY;
    public Orders orders => conn.FindOne<Orders>(OID);
    public Item item => conn.FindOne<Item>(ITEM);
}

namespace Demo
{
    /// <summary>
    /// The demo could be made more elegant with some sepecific helper methods
    /// and extra indexes (e.g. the lookup for Customer Name is useful)
    /// </summary>
    internal class Program
    {
        static void Main()
        {
            var conn = new PyrrhoConnect("Files=D;Role=Sales");
            conn.Open();
            try
            {
                // Get a list of all orders showing the customer name
                var aa = conn.FindAll<Orders>();
                foreach (var a in aa)
                    Console.WriteLine(a.ID + ": " + a.customer.NAME);
                if (aa.Length == 0)
                {
                    Console.WriteLine("The Customer table is empty");
                    goto skip;
                }
                // change the customer name of the first (update to a navigation property)
                var j = aa[0].customer;
                j.NAME = "Johnny";
                j.Put();
                // add a new customer 
                var g = new Customer() { NAME = "Greta" };
                conn.Post(g);
                // place a new order for Mary (try without single quotes!) 
                var m = conn.FindOne<Customer>("Mary");
                var o = new Orders() { CUST=(long)m.ID, OrderDate=Date.Today };
                conn.Post(o);
                // FindWith has (string,value) tuples. Here the value is a string
                var p = conn.FindWith<Item>(("NAME","Pins"))[0];
                var i1 = new OrderItem() { OID=(long)o.ID, ITEM=(long)p.ID, QTY = 2 };
                conn.Post(i1);
                var b = conn.FindWith<Item>(("NAME","Bag"))[0];
                var i2 = new OrderItem() { OID = (long)o.ID, ITEM = (long)b.ID, QTY = 1 };
                conn.Post(i2);
                // calculate the total for the new order (M indicates a decimal constant in C#)
                var t = 0.0m;
                foreach (var i in o.orderItems)
                    t += i.item.PRICE * i.QTY;
                o.Total = t;
                o.Put();
                i1.Get();
                // delete the last order for Fred
                var f = conn.FindOne<Customer>("Fred");
                var fo = f.orderss;
                conn.Delete(f.orderss[fo.Length - 1]);
                // try to delete the Rug item (fails)
                var r = conn.FindWith<Item>(("NANE","Rug"))[0];
                r.Delete();
            skip:;
            } catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.ReadLine();
            conn.Close();
        }
    }
}
