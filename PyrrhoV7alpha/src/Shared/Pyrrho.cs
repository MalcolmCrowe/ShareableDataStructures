﻿namespace PyrrhoBase
{
    /// <summary>
    /// For details of the Pyrrho protocol, see the manual Pyrrho.doc section 8.9
    /// </summary>
    public enum Protocol
    {
        EoF = -1, ExecuteNonQuery = 2, SkipRows = 3, GetRow = 4, CloseReader = 5, BeginTransaction = 6,
        Commit = 7, Rollback = 8, CloseConnection = 9, GetFileNames = 10, Prepare = 11, Request = 12,
        Authority = 13, ResetReader = 14, Detach = 15, ReaderData = 16, Fetch = 17, DataWrite = 18, TypeInfo = 19,
        GetSchema = 20, ExecuteReader = 21, RemoteBegin = 22, Mark = 23, DbGet = 24, DbSet = 25, Physical = 26,
        GetMaster = 27, ExecuteReaderCrypt = 28, DirectServers = 29, RePartition = 30, RemoteCommit = 31,
        CheckConflict = 32, Get = 33, CheckSerialisation = 34, IndexLookup = 35,
        CheckSchema = 36, GetTable = 37, IndexNext = 38, ExecuteNonQueryCrypt = 39, TableNext = 40,
        Mongo = 41, Check = 42, CommitAndReport = 43, RemoteCommitAndReport = 44, Post = 45, Put = 46,
        Get1 = 47, Delete = 48, Update = 49, Rest = 50
    }
    /// <summary>
    /// For details of the Pyrrho protocol responses, see the manual Pyrrho.doc section 8.9
    /// </summary>
    public enum Responses
    {
        Acknowledged = 0, OobException = 1, ReaderData = 10, Done = 11, Exception = 12, Schema = 13, CellData = 14,
        NoData = 15, FatalError = 16, TransactionConflict = 17, Files = 18, RePartition = 30,
        Fetching = 42, Written = 43, SchemaSegment = 44, Master = 45, NoMaster = 46,
        Servers = 47, IndexCursor = 48, LastSchema = 49, TableCursor = 50, IndexData = 51, IndexDone = 52,
        TableData = 53, TableDone = 54, Prepare = 55, Request = 56, Committed = 57, Serialisable = 58,
        Primary = 60, Secondary = 61, Begin = 62, Valid = 63, Invalid = 64, TransactionReport = 65,
        RemoteTransactionReport = 66, PostReport = 67, Warning = 68, TransactionReason = 69
    }
    /// <summary>
    /// Connection strings are sent using a binary encrypted format. 
    /// User, Password, Base, BaseServer, Coordinator and Length are reserved for server-server comms.
    /// </summary>
    public enum Connecting
    {
        Password = 20, User = 21, Files = 22, Role = 23, Done = 24, Stop = 25, Host = 26, Key = 27,
        Details = 28, Base = 29, Coordinator = 30, BaseServer = 31, Modify = 32, Length = 33
    }
}
