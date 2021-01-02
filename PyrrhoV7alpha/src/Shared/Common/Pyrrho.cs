namespace Pyrrho.Common
{
    // Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
    // (c) Malcolm Crowe, University of the West of Scotland 2004-2021
    //
    // This software is without support and no liability for damage consequential to use.
    // You can view and test this code, and use it subject for any purpose.
    // You may incorporate any part of this code in other software if its origin 
    // and authorship is suitably acknowledged.
    // All other use or distribution or the construction of any product incorporating 
    // this technology requires a license from the University of the West of Scotland.
    /// <summary>
    /// For details of the Pyrrho protocol, see the manual Pyrrho.doc section 8.8, which
    /// gives the current list of codes.
    /// </summary>
    public enum Protocol
    {
        EoF = -1, ExecuteNonQuery = 2, SkipRows = 3, GetRow = 4, CloseReader = 5, BeginTransaction = 6,
        Commit = 7, Rollback = 8, CloseConnection = 9, GetFileNames = 10, Prepare = 11, Request = 12,
        Authority = 13, ResetReader = 14, Detach = 15, ReaderData = 16, Fetch = 17, DataWrite = 18, TypeInfo = 19,
        ExecuteReader = 21, RemoteBegin = 22, Mark = 23, DbGet = 24, DbSet = 25, Physical = 26,
        GetMaster = 27, ExecuteReaderCrypt = 28, DirectServers = 29, RePartition = 30, RemoteCommit = 31,
        CheckConflict = 32, Get = 33, CheckSerialisation = 34, IndexLookup = 35,
        CheckSchema = 36, GetTable = 37, IndexNext = 38, ExecuteNonQueryCrypt = 39, TableNext = 40,
        Mongo = 41, Check = 42, CommitAndReport = 43, RemoteCommitAndReport = 44, Post = 45, Put = 46,
        Get1 = 47, Delete = 48, Update = 49, Rest = 50, Subscribe = 51, Synchronise = 52, SetMaster = 53,
        GetInfo = 54, Execute = 55, Get2 = 56, ExecuteNonQueryTrace = 73, CommitTrace = 74,
        CommitAndReportTrace = 75, ExecuteTrace = 76, CommitAndReport1 = 77, CommitAndReportTrace1 = 78
    }
    /// <summary>
    /// For details of the Pyrrho protocol responses, see the manual Pyrrho.doc section 8.8,
    /// which gives the currently supported list of codes.
    /// </summary>
    public enum Responses
    {
        Acknowledged = 0, OobException = 1, ReaderData = 10, Done = 11, Exception = 12, Schema = 13, CellData = 14,
        NoData = 15, FatalError = 16, TransactionConflict = 17, Files = 18, RePartition = 30,
        Fetching = 42, Written = 43, Master = 45, NoMaster = 46,
        Servers = 47, IndexCursor = 48, LastSchema = 49, TableCursor = 50, IndexData = 51, IndexDone = 52,
        TableData = 53, TableDone = 54, Prepare = 55, Request = 56, Committed = 57, Serialisable = 58,
        Primary = 60, Secondary = 61, Begin = 62, Valid = 63, Invalid = 64, TransactionReport = 65,
        RemoteTransactionReport = 66, PostReport = 67, Warning = 68, TransactionReason = 69,
        DataLength = 70, Columns = 71, Schema1 = 72, DoneTrace = 76, TransactionReportTrace = 77
    }
    /// <summary>
    /// Connection strings are sent using a binary encrypted format. 
    /// User, Password, Base, BaseServer, Coordinator and Length are no longer supported
    /// </summary>
    public enum Connecting
    {
        Password = 20, User = 21, Files = 22, Role = 23, Done = 24, Stop = 25, Host = 26, Key = 27,
        Details = 28, Base = 29, Coordinator = 30, BaseServer = 31, Modify = 32, Length = 33
    }
}
