using System.Text;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level3
{
    /// <summary>
    /// Level 3 Role object. Roles have access to role-based database objects: table/views, procedures and domains. 
    /// ALTER ROLE statements can onl y be executed by the role or its creator.
    /// Two special implicit roles cannot be ALTERed.
    /// The default role schemaRole has access to all entities i.e. the set of tables that have primary keys.
    /// The guest role has access to all such that have been granted to PUBLIC: there is no Role object for this.
    /// Immutable
    /// </summary>
    internal class Role : DBObject
    {
        readonly static long
            DBObjects = --_uid, // BTree<string,long> Domain/Table/View etc by name
            Procedures = --_uid; // BTree<string,BList<long>> Procedure/Function by name and arity
        internal BTree<string, long> dbobjects => (BTree<string, long>)mem[DBObjects];
        internal BTree<string, BList<long>> procedures =>
            (BTree<string, BList<long>>)mem[Procedures];
        public const Grant.Privilege use = Grant.Privilege.UseRole, 
            admin = Grant.Privilege.UseRole | Grant.Privilege.AdminRole;
        internal BTree<long, object> objects => mem;
        /// <summary>
        /// Just to create the schema and guest roles
        /// </summary>
        /// <param name="nm"></param>
        /// <param name="u"></param>
        internal Role(string nm, long defpos, BTree<long, object> m)
            : base(nm, defpos,defpos,-1,m
                  +(DBObjects,BTree<string,long>.Empty)
                  +(Procedures,BTree<string, BList<long>>.Empty))
        { }
        public Role(PRole p,Database db,bool first)
            :base(p.name,p.ppos,p.ppos,db.role.defpos,
                 (first?db.schemaRole.mem:db.guestRole.mem)
                 +(DBObjects,first?db.schemaRole.dbobjects:db.guestRole.dbobjects)
                 +(Procedures,first?db.schemaRole.procedures:db.guestRole.procedures))
        { }
        protected Role(long defpos, BTree<long, object> m) : base(defpos, m) { }
        public static Role operator+(Role r,DBObject ob)
        {
            if (ob.defpos < 0 && ob.name == "")
                ob += (Name, ob.ToString());
            var m = r.mem + (ob.defpos, ob);
            if (ob.name != "")
                m += (DBObjects, r.dbobjects + (ob.name, ob.defpos));
            return new Role(r.defpos,m);
        }
        public static Role operator+(Role r,Procedure p)
        {
            var pa = r.procedures[p.name]??BList<long>.Empty;
            pa += (p.arity, p.defpos);
            return new Role(r.defpos, r.mem + (Procedures, r.procedures + (p.name, pa)));
        }
        public static Role operator -(Role r, DBObject ob)
        {
            return new Role(r.defpos, r.mem - ob.defpos);
        }
        internal DBObject GetObject(string nm)
        {
            if (!dbobjects.Contains(nm))
                return null;
            return (DBObject)objects[dbobjects[nm]];
        }
        internal Procedure GetProcedure(string nm,int ar)
        {
            if (procedures[nm] is BList<long> bl && bl.Count>ar)
                return (Procedure)objects[bl[ar]];
            return null;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" NSObjects:");
            var cm = '(';
            for (var b=mem.PositionAt(0)?.Next();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ','; sb.Append(b.value());
            }
            sb.Append(") Domains:");sb.Append(dbobjects);
            sb.Append(" Procedures:");sb.Append(procedures);
            return sb.ToString();
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Role(defpos,m);
        }
    }
}