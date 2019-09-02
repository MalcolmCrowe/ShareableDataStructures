# Copyright 2015 University of the West of Scotland and Malcolm.Crowe@uws.ac.uk
#This software may be used by you for any legitimate purpose on a royalty free basis.
#Under no circumstances will the University of the West of Scotland 
#or Malcolm Crowe be held liable for any loss or damage consequential on the 
#use of this software. This does not affect your statutory rights under 
#applicable law.
# Version 5.3 (25 September 2015): for Python 3.4
from types import * 
from datetime import *
from socket import *
from io import *
from ipaddress import *
from decimal import *
from struct import *
from enum import IntEnum
from http.server import * # for class WebSvr etc
import urllib
from builtins import *
import os
import base64
import sys
from threading import *
class Protocol(IntEnum): 
    Eof = -1
    ExecuteNonQuery = 2
    SkipRows = 3
    GetRow = 4
    CloseReader = 5
    BeginTransaction = 6
    Commit = 7
    Rollback = 8
    CloseConnection = 9
    GetFileNames = 10
    Prepare = 11
    Request = 12
    Authority = 13
    ResetReader = 14
    Detach = 15
    ReaderData = 16
    Fetch = 17
    DataWrite = 18
    TypeInfo = 19
    GetSchema = 20
    ExecuteReader = 21
    RemoteBegin = 22
    Mark = 23
    DbGet = 24
    DbSet = 25
    Physical = 26
    GetMaster = 27
    ExecuteReaderCrypt = 28
    DirectServers = 29
    RePartition = 30
    RemoteCommit = 31
    CheckConflict = 32
    Get = 33
    CheckSerialisation = 34
    IndexLookup = 35
    CheckSchema = 36
    GetTable = 37
    IndexNext = 38
    ExecuteNonQueryCrypt = 39
    TableNext = 40
    Mongo = 41
    Check = 42
    CommitAndReport = 43
    RemoteCommitAndReport = 44
    Post = 45
    Put = 46
    Get1 = 47
    Delete = 48
    Update = 49
    Rest = 50
    Subscribe = 51
    Synchronise = 52
    SetMaster = 53
    GetInfo = 54
    Execute = 55
    Get2 = 56
class Responses(IntEnum): 
    Acknowledged = 0
    OobException = 1
    ReaderData = 10
    Done = 11
    Exception = 12
    Schema = 13
    CellData = 14
    NoData = 15
    FatalError = 16
    TransactionConflict = 17
    Files = 18
    RePartition = 30
    Fetching = 42
    Written = 43
    SchemaSegment = 44
    Master = 45
    NoMaster = 46
    Servers = 47
    IndexCursor = 48
    LastSchema = 49
    TableCursor = 50
    IndexData = 51
    IndexDone = 52
    TableData = 53
    TableDone = 54
    Prepare = 55
    Request = 56
    Committed = 57
    Serialisable = 58
    Primary = 60
    Secondary = 61
    Begin = 62
    Valid = 63
    Invalid = 64
    TransactionReport = 65
    RemoteTransactionReport = 66
    PostReport = 67
    Warning = 68
    TransactionReason = 69
    DataLength = 70
    Columns = 71
    Schema1 = 72
class PyrrhoDbType(IntEnum):
    DBNull = 0
    Integer = 1
    Decimal = 2
    String = 3
    Timestamp = 4
    Blob = 5
    Row = 6
    Array = 7
    Real = 8
    Bool = 9
    Interval = 10
    Time = 11
    Date = 12
    UDType = 13
    Multiset = 14
    Xml = 15
    Document = 16
class PyrrhoConnect:
    classes = []
    def __init__(self,cs):
        self.hostName = '127.0.0.1'
        self.socket = None
        self.client = None
        self.stream = None
        self.isOpen = False
        self.isClosed = False
        self.connectionString = cs
        self.posted = []
        self.warnings = []
        self.rowCheck = ''
        self.readCheck = ''
        self.transactionLock = RLock()
        self.commandLock = RLock()
    def find(self,tn):
        for t in self.classes:
            if t.__name__==tn:
                return t
        raise Exception("Unknown Type")
    def createCommand(self):
        return PyrrhoCommand(self)
    def act(self,sql):
        if not self.isOpen:
            self.open()
        self.commandLock.acquire()
        try:
            cmd = self.createCommand()
            cmd.commandText = sql
            r = cmd.executeNonQuery()
            return r
        finally:
            self.commandLock.release()
    def beginTransaction(self):
        self.transactionLock.acquire()
        try:
            return PyrrhoTransaction(self)
        finally:
            self.transactionLock.release()
    def close(self):
        self._send(Protocol.CloseConnection)
        self.stream.close()
        self.stream = None
        self.socket.close()
        self.isOpen = False
    def delete(self,ob):
        self.commandLock.acquire()
        try:
            s = 'delete from "'+ob.__class__.__name__+'"'
            c = " where "+ob.__class__.__name__+".check='"+ob.rowCheck+"'"
            self._send(Protocol.Delete)
            self._putLong(ob._schemakey)
            self._putString(s+c)
            b = self._receive()
            if b!=Responses.Done or self._getInt()==0:
                raise DatabaseError("0200")
        finally:
            self.commandLock.release()
    def findAll(self,cls):
        return self.get(cls,'/'+cls.__name__+'/')
    def findOne(self,cls,key):
        ws = ''
        c = ''
        for i in range(len(key)):
            v = key[i]
            if isinstance(v,str):
                v = "'"+v.replace("'","''")+"'"
            ws += c + v
            c = ','
        obs = self.get(cls,'/'+cls.__name__+'/'+urllib.parse.quote(ws))
        if obs==None or len(obs)==0:
            return None
        return obs[0] 
    def findWith(self,cls,cond):
        return self.get(cls,'/'+cls.__name__+'/'+urllib.parse.quote(cond))
    def get(self,cls,rurl):
        self.commandLock.acquire(True)
        try:
            self._send(Protocol.Get,rurl)
            r = self._get0(cls)
        except Exception as e:
            raise e
        finally:
            self.commandLock.release()
        return r
    def _get0(self,cls):
        r = []
        rdr = PyrrhoReader._new(PyrrhoCommand(self))
        if rdr==None:
            return r
        while rdr.read():
            ob = cls()
            for i in range(len(rdr.schema.columns)):
                setattr(ob,rdr.schema.columns[i].columnName,rdr.val(i))
            if isinstance(ob,Versioned):
                ob.rowCheck = rdr.row.check
                ob.readCheck = rdr.row.readCheck
            r.append(ob)
        rdr.close()
        return r
    def open(self):
        if self.isOpen:
            return
        names = self._getConnectionValues('Host')
        hostName = 'localhost'
        if names!=None:
            hostName = names[0]
        port = 5433
        ports = self._getConnectionValues('Port')
        if ports!=None:
            port = int(ports[0])
        self.socket = socket()
        self.socket.connect((hostName,port))
        self.stream = PPStream(self,self.socket)
        key = self._getLong() # nonce
        self.stream.write(bytes([0]))
        self.crypt = Crypt(self.stream,key) 
        self.stream = self.crypt.stream
        users = self._getConnectionValues('User')
        if users==None:
            user = ''
        else:
            user = users[0]
        self.crypt.sendConnectionString(self.connectionString,user)
        self.isOpen = True
        self.isClose = False
        b = self._receive()
        if b!=Responses.Primary:
            raise Exception('non primary connection not supported')
    def post(self,ob):
        self.commandLock.acquire()
        try:
            sc = 'insert into "'+ob.__class__.__name__+'"'
            sv = ') values '
            c = '('
            for f in dir(ob):
                if f[0]=='_' or f=='rowCheck' or f=='readCheck' or f in ob._key:
                    continue
                v = getattr(ob,f)
                if v==None:
                    continue
                elif isinstance(v,str):
                    v = "'"+v.replace("'","''")+"'"
                elif isinstance(v,bytearray):
                    v = self._hexits(v)
                elif isinstance(v,DocBase):
                    v = v.str()
                else:
                    s = str(v)
                    if len(s)>0 and s[0]=='<':
                        v = Document().build(v).str()
                    else:
                        v = s
                sc += c+'"'+f+'"'
                sv += c+v
                c = ','
            self._send(Protocol.Post)
            self._putLong(ob._schemakey)
            self._putString(sc+sv+')')
            b = self._receive()
            if b==Responses.Schema:
                self._fix(ob)
                b = self._receive()
            if b!=Responses.Done:
                raise DatabaseError("2E203")
        finally:
            self.commandLock.release()
    def put(self,ob):
        self.commandLock.acquire()
        try:
            sc = 'update "'+ob.__class__.__name__+'"'
            sw = 'where '
            c = ' set '
            wh = ''
            for f in dir(ob):
                if f[0]=='_' or f=='rowCheck' or f=='readCheck':
                    continue
                v = getattr(ob,f)
                if v==None:
                    continue
                if isinstance(v,str):
                    v = "'"+v.replace("'","''")+"'"
                if isinstance(v,bytearray):
                    v = self.hexits(v)
                if self.aclass(v):
                    v = Document().build(v)._str()
                else:
                    s = str(v)
                    if len(s)>0 and s[0]=='<':
                        v = Document().build(v).str()
                    else:
                        v = s
                sc += c+'"'+f+'"='+v
                c = ','
            if ob.rowCheck!=0:
                sw += wh+ob.__class__.__name__+".check='"+ob.rowCheck+"'"
            self._send(Protocol.Put)
            self._putLong(ob._schemakey)
            self._putString(sc+sw)
            b = self._receive()
            if b==Responses.NoData:
                raise DatabaseError("40001") # version no longer valid
            if b==Responses.Schema:
                self._fix(ob)
                b = self._receive()
            if b!=Responses.Done:
                raise DatabaseError("2E203")
        finally:
            self.commandLock.release()
    def aclass(self,v):
        for c in self.classes:
            if isinstance(v,c):
                return True
        return False
    def update(self,cls,w,u): # w,u are Documents
        self.commandLock.acquire()
        try:
            self._send(Protocol.Update,cls.__name__)
            wb = w.bytes()
            self._putInt(len(wb))
            self.stream.write(wb)
            ub = u.bytes()
            self._putInt(len(ub))
            self.stream.write(ub)
            r = self._get0(cls)
            return r
        finally:
            self.commandLock.release()
    def check(self,chk):
        self._send(Protocol.Check,chk)
        b = self._receive()
        return b==Responses.Valid
    def _close(self):
        self.isOpen = False
        self.isClosed = True
        self.stream = None
    def _fix(self,ob):
        n = self._getInt()
        if n==0:
            return
        t = PyrrhoTable(PyrrhoCommand(self),self._getString())
        self._getSchema(t,n)
        b = self._receive() # should be Responses.PutRow
        for i in range(n):
            c = t.columns[i]
            setattr(ob, c.columnName,self._getCell(i,c.datatypename,c.type).val)
        if self.rowCheck!='' and isinstance(ob,Versioned):
            ob.rowCheck = self.rowCheck
            ob.readCheck = self.readCheck
        return
    def _getConnectionValues(self,field):
        split = self.connectionString.split(';')
        for s in split:
            if s[0:len(field)+1]==field+'=':
                return s[len(field)+1:len(s)].split(',')
        return None
    def _getInt(self):
        b = self.stream.read(4)
        n = 0
        for i in range(4):
            n = (n<<8)+b[i]
        return n
    def _getLong(self):
        b = self.stream.read(8)
        n = 0
        for i in range(8):
            n = (n<<8)+b[i]
        return n
    def _getString(self):
        n = self._getInt()
        return str(self.stream.read(n),'utf-8')
    def _getStrings(self):
        n = self._getInt()
        s = list()
        for i in range(n):
            s.append(self._getString())
        return s
    def _getBlob(self):
        n = self._getInt();
        return self.stream.read(n)
    def _getDateTime(self):
        return datetime.fromtimestamp(self._getLong()/10000000-62135596800)
    def _getTimeSpan(self):
        return timedelta(microseconds=self._getLong()/10000)
    def _getInterval(self):
        pi = PyrrhoInterval()
        pi.years = self._getLong()
        pi.months = self._getLong()
        pi.ticks = self._getLong()
        return pi
    def _getRow(self,tn):
        n = self._getInt();
        c = self.find(tn)
        r = object.__new__(c)
        for i in range(n):
            cn = self._getString()
            dn = self._getString()
            fl = self._getInt()
            setattr(r,cn,self._getCell(i,dn,fl).val)
        return r
    def _getArray(self,r):
        r = PyrrhoArray()
        r.kind = self._getString()
        dn = self._getString()
        fl = self._getInt()
        n = self._getInt()
        for i in range(n):
            r.data.append(self._getCell(i,dn,fl).val)
        return r
    def _getSchema(self,t,n):
        for i in range(n):
            cn = self._getString()
            dn = self._getString()
            p = PyrrhoColumn()
            p._setType(cn,dn,self._getInt())
            t.columns.append(p)
            t.cols[cn]=i
            ki = (p.type>>4)&0xf
            if ki>0:
                for j in range(ki-len(t.primaryKey)):
                    t.primaryKey.append(0)
                t.primaryKey[ki-1] = p
    def _getTable(self,tn):
        n = self._getInt()
        if n==0:
            return None
        s = self._getString()
        t = PyrrhoTable(PyrrhoCommand(self),tn)
        self._getSchema(t,n)
        nr = self._getInt()
        for j in range(nr):
            r = PyrrhoRow(t)
            for i in range(n):
                c = t.columns[i]
                r.append(self._getCell(i,c.datatypename,c.type))
                if self.rowCheck!='':
                    r.rowCheck = self.rowCheck
                    r.readCheck = self.readCheck
            t.rows.append(r)
        return t
    def _getCell(self,j,tname,flag):
        cell = CellValue(flag)
        cell.subType = tname
        self.rowCheck = ''
        self.readCheck = 0
        b = self.stream.read(1)[0]
        if b==3:
            self.rowCheck = self._getString()
            b = self.stream.read(1)[0]
        if b==4:
            self.readCheck = self._getString()
            b = self.stream.read(1)[0]
        if b == 0:
            return cell
        elif b==2:
            tname = self._getString()
            flag = self._getInt()
        cell.subType = tname
        ft = flag&0xf
        if ft==0:
            return cell;
        elif ft==1:
            s = self._getString()
            try:
                cell.val = int(s)
            except:
                cell.val = s
        elif ft==2:
            s = self._getString()
            try:
                cell.val = float(s)
            except:
                cell.val = s
        elif ft==3:
            cell.val = self._getString()
        elif ft==4:
            cell.val = self._getDateTime()
        elif ft==5:
            bb = self._getBlob()
            if tname=='DOCUMENT':
                cell.val = Document().fromBson(bb)
            elif tname=='DOCARRAY':
                cell.val = DocArray().fromBson(bb)
            elif tname=='OBJECT':
                cell.val = bb
            else:
                cell.val = bb
        elif ft==6:
            cell.val = self._getRow()
        elif ft==7:
            cell.val = self._getArray(r)
        elif ft==8:
            cell.val = float(self._getString())
        elif ft==9:
            cell.val = self._getInt()!=0
        elif ft==10:
            cell.val = self._getInterval()
        elif ft==11:
            cell.val = self._getTimeSpan()
        elif ft==12:
            cell.val = self._getRow(tname)
        elif ft==13:
            cell.val = self._getDateTime().date()
        elif ft==14:
            cell.val = self._getTable(tname)
        else:
            raise Exception('unknown type '+ft)
        return cell
    def _hexits(self,bs):
        s = "X'"
        for i in range(len(bs)):
            s += '{:02X}'.format(bs[i])
        return s+"'"
    def _putInt(self,n):
        self.stream.write([(n>>24)&0xff,(n>>16)&0xff,(n>>8)&0xff,n&0xff])
    def _putLong(self,n):
        self.stream.write([(n>>56)&0xff,(n>>48)&0xff,(n>>40)&0xff,
                           (n>>32)&0xff,(n>>24)&0xff,(n>>16)&0xff,
                           (n>>8)&0xff,n&0xff])
    def _putString(self,s):
        b = bytes(s,'utf-8')
        self._putInt(len(b))
        self.stream.write(b)
    def _send(self,b,s=None):
        bi = int(b)
        self.stream._writeByte(bi)
        if s!=None:
            self._putString(s)
    def _receive(self):
        self.stream._flush()
        b = Responses(self.stream._readByte())
        while b==Responses.Warning:
            sig = self._getString();
            warnings += DatabaseError(sig,selg._getStrings())
            b = Responses(self.stream._readByte())
        return b
class SQLState:
    info = dict()
    info["02000"]= "Not found"
    info["08000"]= "Invalid connection name"
    info["08001"]= "SQL-client unable to establish SQL-connection"
    info["08004"]= "SQL-server rejected establishment of SQL-connection"
    info["08006"]= "Connection failure"
    info["08007"]= "Connection exception - transaction resolution unknown"
    info["21000"]= "Cardinality violation"
    info["22000"]= "Data and incompatible type errors"
    info["22003"]= "Out of bounds value"
    info["22004"]= "Illegal null value"
    info["22005"]= "Wrong types: expected {0} got {1}"
    info["22007"]= "DateTime format error: {0}"
    info["22008"]= "Minus sign expected (DateTime)"
    info["2200G"]= "Type mismatch: expected {0} got {1} at {2}"
    info["2200N"]= "Invalid XML content"
    info["22012"]= "Division by zero"
    info["22019"]= "Invalid escape character"
    info["2201B"]= "Invalid regular expression"
    info["2201M"]= "Namespace {0} not defined"
    info["22025"]= "Invalid escape sequence"
    info["22030"]= "Invalid RDF format"
    info["22102"]= "Type mismatch on concatenate"
    info["22103"]= "Multiset element not found"
    info["22104"]= "Incompatible multisets for union"
    info["22105"]= "Incompatible multisets for intersection"
    info["22106"]= "Incompatible multisets for except"
    info["22107"]= "Exponent expected"
    info["22108"]= "Type error in aggregation operation"
    info["22109"]= "Too few arguments"
    info["22110"]= "Too many arguments"
    info["22201"]= "Unexpected type {0} for comparison with Decimal"
    info["22202"]= "Incomparable types"
    info["22203"]= "Loss of precision on conversion"
    info["22204"]= "Query expected"
    info["22205"]= "Null value found in table {0}"
    info["22206"]= "Null value not allowed in column {0}"
    info["22207"]= "Row has incorrect length"
    info["22208"]= "Mixing named and unnamed columns is not supported"
    info["22209"]= "AutoKey is not available for {0}"
    info["22300"]= "Bad document format: {0}" 
    info["23000"]= "Integrity constraint: {0}"
    info["23001"]= "RESTRICT: {0} referenced in {1} {2}"
    info["23101"]= "Integrity constraint on referencing table {0} (delete)"
    info["23102"]= "Integrity constraint on referencing table {0} (update)"
    info["23103"]= "This record cannot be updated: {0}"
    info["24000"]= "Invalid cursor state"
    info["24101"]= "Cursor is not open"
    info["25000"]= "Invalid transaction state"
    info["25001"]= "A transaction is in progress"
    info["26000"]= "Invalid SQL statement name{0}"
    info["28000"]= "Invalid authorisation specification: no {0} in database {1}"
    info["28101"]= "Unknown grantee kind"
    info["28102"]= "Unknown grantee {0}"
    info["28104"]= "Users can only be added to roles"
    info["28105"]= "Grant of select: entire row is nullable"
    info["28106"]= "Grant of insert: must include all notnull columns"
    info["28107"]= "Grant of insert cannot include generated columns"
    info["28108"]= "Grant of update: column {0} is not updatable"
    info["2D000"]= "Invalid transaction termination"
    info["2E104"]= "Database is read-only"
    info["2E105"]= "Invalid user for database {1}"
    info["2E106"]= "This operation requires a single-database session"
    info["2E108"]= "Stop time was specified]= so database is read-only"
    info["2E111"]= "User {0} can access no columns of table {1}"
    info["2E201"]= "Connection is not open"
    info["2E202"]= "A reader is already open"
    info["2E203"]= "Unexpected reply"
    info["2E204"]= "Bad data type {0} (internal)"
    info["2E205"]= "Stream closed"
    info["2E206"]= "Internal error: {0}"
    info["2E208"]= "Badly formatted connection string {0}"
    info["2E209"]= "Unexpected element {0} in connection string"
    info["2E210"]= "Pyrrho DBMS service on {0} at port {1} not available (or not reachable)"
    info["2E213"]= "Unsupported configuration operation"
    info["2E214"]= "Schema change must be on base database"
    info["2E215"]= "Overlapping partitions"
    info["2E216"]= "Configuration update can only be for local server"
    info["2E217"]= "This server does not provide a Query service for {0}"
    info["2E218"]= "Index {0} is incompatible with the partitioning scheme"
    info["2E219"]= "Data and Schema changes cannot be mixed in a partitioned transaction"
    info["2E300"]= "The calling assembly does not have type {0}"
    info["2E301"]= "Type {0} does not have a default constructor"
    info["2E302"]= "Type {0} does not define public field {1}"
    info["2E303"]= "Types {0} and {1} do not match"
    info["2E304"]= "GET rurl should begin with /"
    info["2E305"]= "No data returned by rurl {0}"
    info["2E307"]= "Obtain an up-to-date schema for {0} from Role$Class"
    info["2F003"]= "Prohibited SQL-statement attempted"
    info["2H000"]= "Collation error: {0}"
    info["34000"]= "No such cursor: {0}"
    info["3D000"]= "Invalid catalog specification {0}"
    info["3D001"]= "Database {0} not open"
    info["3D003"]= "Remote database no longer accessible"
    info["3D004"]= "Exception reported by remote database: {0}"
    info["3D005"]= "Requested operation not supported by this edition of Pyrrho"
    info["3D006"]= "Database {0} incorrectly terminated or damaged"
    info["3D010"]= "Invalid Password"
    info["40000"]= "Transaction rollback"
    info["40001"]= "Transaction conflict"
    info["40003"]= "Transaction rollback – statement completion unknown"
    info["40005"]= "Transaction rollback - new key conflict with empty query"
    info["40006"]= "Transaction conflict: Read constraint for {0}"
    info["40007"]= "Transaction conflict: Read conflict for {0}"
    info["40008"]= "Transaction conflict: Read conflict for table {0}"
    info["40009"]= "Transaction conflict: Read conflict for record {0}"
    info["40010"]= "Transaction conflict: Object {0} has just been dropped"
    info["40011"]= "Transaction conflict: Supertype {0} has just been dropped"
    info["40012"]= "Table {0} has just been dropped"
    info["40013"]= "Column {0} has just been dropped"
    info["40014"]= "Record {0} has just been deleted"
    info["40015"]= "Type {0} has just been dropped"
    info["40016"]= "Domain {0} has just been dropped"
    info["40017"]= "Index {0} has just been dropped"
    info["40021"]= "Supertype {0} has just been changed"
    info["40022"]= "Another domain {0} has just been defined"
    info["40023"]= "Period {0} has just been changed"
    info["40024"]= "Versioning has just been defined"
    info["40025"]= "Table {0} has just been altered"
    info["40026"]= "Integrity constraint: {0} has just been added"
    info["40027"]= "Integrity constraint: {0} has just been referenced"
    info["40029"]= "Record {0} has just been updated"
    info["40030"]= "A conflicting table {0} has just been defined"
    info["40031"]= "A conflicting view {0} has just been defined"
    info["40032"]= "A conflicting object {0} has just been defined"
    info["40033"]= "A conflicting trigger for {0} has just been defined"
    info["40034"]= "Table {0} has just been renamed"
    info["40035"]= "A conflicting role {0} has just been defined"
    info["40036"]= "A conflicting routine {0} has just been defined"
    info["40037"]= "An ordering now uses function {0}"
    info["40038"]= "Type {0} has just been renamed"
    info["40039"]= "A conflicting method {0} for {0} has just been defined"
    info["40040"]= "A conflicting period for {0} has just been defined"
    info["40041"]= "Conflicting metadata for {0} has just been defined"
    info["40042"]= "A conflicting index for {0} has just been defined"
    info["40043"]= "Columns of table {0} have just been changed"
    info["40044"]= "Column {0} has just been altered"
    info["40045"]= "A conflicting column {0} has just been defined"
    info["40046"]= "A conflicting check {0} has just been defined"
    info["40047"]= "Target object {0} has just been renamed"
    info["40048"]= "A conflicting ordering for {0} has just been defined"
    info["40049"]= "Ordering definition conflicts with drop of {0}"
    info["40050"]= "A conflicting namespace change has occurred"
    info["40051"]= "Conflict with grant/revoke on {0}"
    info["40051"]= "Conflict with grant/revoke on {0}"
    info["40052"]= "Conflicting routine modify for {0}"
    info["40053"]= "Domain {0} has just been used for insert"
    info["40054"]= "Domain {0} has just been used for update"
    info["40055"]= "An insert conflicts with drop of {0}"
    info["40056"]= "An update conflicts with drop of {0"
    info["40057"]= "A delete conflicts with drop of {0}"
    info["40058"]= "An index change conflicts with drop of {0}"
    info["40059"]= "A constraint change conflicts with drop of {0}"
    info["40060"]= "A method change conflicts with drop of type {0}"
    info["40068"]= "Domain {0} has just been altered,conflicts with drop"
    info["40069"]= "Method {0} has just been changed, conflicts with drop"
    info["40070"]= "A new ordering conflicts with drop of type {0}"
    info["40071"]= "A period definition conflicts with drop of {0}"
    info["40072"]= "A versioning change conflicts with drop of period {0}"
    info["40073"]= "A read conflicts with drop of {0}"
    info["40074"]= "A delete conflicts with update of {0}"
    info["40075"]= "A new reference conflicts with deletion of {0}"
    info["40076"]= "A conflicting domain or type {0} has just been defined"
    info["40077"]= "A conflicting change on {0} has just been done"
    info["40078"]= "Read conflict with alter of {0}"
    info["40079"]= "Insert conflict with alter of {0}"
    info["40080"]= "Update conflict with alter of {0}"
    info["40081"]= "Alter conflicts with drop of {0}"
    info["40082"]= "ETag validation failure"
    info["40083"]= "Secondary connection clnflict on {0}"
    info["42000"]= "Syntax error at {0}"
    info["42101"]= "Illegal character {0}"
    info["42102"]= "Name cannot be null"
    info["42103"]= "Key must have at least one column"
    info["42104"]= "Proposed name conflicts with existing database object (e.g. table already exists)"
    info["42105"]= "Access denied"
    info["42107"]= "Table {0} undefined"
    info["42108"]= "Procedure {0} not found"
    info["42111"]= "The given key is not found in the referenced table"
    info["42112"]= "Column {0} not found"
    info["42113"]= "Multiset operand required]= not {0}"
    info["42115"]= "Unexpected object type {0} {1} for GRANT"
    info["42116"]= "Role revoke has ADMIN option not GRANT"
    info["42117"]= "Privilege revoke has GRANT option not ADMIN"
    info["42118"]= "Unsupported CREATE {0}"
    info["42119"]= "Domain {0} not found in database {1}"
    info["4211A"]= "Unknown privilege {0}"
    info["42120"]= "Domain or type must be specified for base column {0}"
    info["42123"]= "NO ACTION is not supported"
    info["42124"]= "Colon expected {0}"
    info["42125"]= "Unknown Alter type {0}"
    info["42126"]= "Unknown SET operation"
    info["42127"]= "Table expected"
    info["42128"]= "Illegal aggregation operation"
    info["42129"]= "WHEN expected"
    info["42131"]= "Invalid POSITION {0}"
    info["42132"]= "Method {0} not found in type {1}"
    info["42133"]= "Type {0} not found"
    info["42134"]= "FOR phrase is required"
    info["42135"]= "Object {0} not found"
    info["42138"]= "Field selector {0} not defined for {1}"
    info["42139"]= ":: on non-type"
    info["42140"]= ":: requires a static method"
    info["42142"]= "NEW requires a user-defined type constructor"
    info["42143"]= "{0} specified more than once"
    info["42146"]= "{0} specified on {1} trigger"
    info["42147"]= "Table {0} already has a primary key"
    info["42148"]= "FOR EACH ROW not specified"
    info["42149"]= "Cannot specify OLD/NEW TABLE for before trigger"
    info["42150"]= "Malformed SQL input (non-terminated string)"
    info["42151"]= "Bad join condition"
    info["42153"]= "Table {0} already exists"
    info["42154"]= "Unimplemented or illegal function {0}"
    info["42156"]= "Column {0} is already in table {1}"
    info["42157"]= "END label {0} does not match start label {1}"
    info["42158"]= "{0} is not the primary key for {1}"
    info["42159"]= "{0} is not a foreign key for {1}"
    info["42160"]= "{0} has no unique constraint"
    info["42161"]= "{0} expected at {1}"
    info["42162"]= "Table period definition for {0} has not been defined"
    info["42163"]= "Generated column {0} cannot be used in a constraint"
    info["42164"]= "Table {0} has no primary key"
    info["42166"]= "Domain {0} already exists"
    info["42167"]= "A routine with name {0} and arity {1} already exists"
    info["42168"]= "AS GET needs a schema definition"
    info["42169"]= "Ambiguous column name {0} needs alias"
    info["42170"]= "Column {0} must be aggregated or grouped"
    info["42171"]= "A table cannot be placed in a column"
    info["42172"]= "Identifier {0} already declared in this block"
    info["42173"]= "Method {0} has not been defined"
    info["44000"]= "Check condition {0} fails"
    info["44001"]= "Domain check {0} fails for column {1} in table {2}"
    info["44002"]= "Table check {0} fails for table {1}"
    info["44003"]= "Column check {0} fails for column {1} in table {2}"
    info["44004"]= "Column {0} in table {1} contains nulls, not null cannot be set"
    info["44005"]= "Column {0} in table {1} contains values, generation rule cannot be set"
class DatabaseError(Exception):
    def __init__(self,sig,obs):
        self.sig = sig
        fmt = SQLState.info[sig]
        if obs==None or len(obs)==0:
            self.message = fmt
        elif len(obs)==1:
            self.message = fmt.format(obs[0])
        elif len(obs)==2:
            self.message = fmt.format(obs[0],obs[1])
        elif len(obs)==3:
            self.message = fmt.format(obs[0],obs[1],obs[2])
        else:
            self.message = sig
        self.info = dict()
    def str(self):
        return self.message
class DocumentException(Exception):
    def __init(self,mess):
        self.message = mess
class PyrrhoColumn:
    def __init__(self):
        self.columnName = ''
        self.caption = ''
        self.type = 0
        self.datatypename = ''
    def _setType(self,n,dn,t):
        self.columnName = n
        self.datatypename = dn
        self.caption = n
        self.type = t
    def dataType(self):
        return ['object','int','numeric','str','datetime','bytearray',
                'row','array','float','bool','interval','time','type',
                'date'][self.type&0xf]
    def allowDBNull(self):
        return self.type&0x100!=0
    def readOnly(self):
        return self.type&0x200!=0
    def str(self):
        return self.caption
class PyrrhoRow:
    def __init__(self,t):
        self.cells = []
        self.schema = t
        self.version = 0
        self.check = ''
    def col(self,nm):
        i = self.schema.cols[nm]
        return self.cells[i].val
    def type(self,i):
        return self.cells[i].subType
    def val(self,i):
        return self.cells[i].val
class PyrrhoTable:
    def __init__(self,cm,tn):
        self.tableName = tn
        self.connectString = cm.conn.connectionString
        self.selectString = cm.commandText
        self.primaryKey = []
        self.columns = []
        self.rows = []
        self.cols = dict()
    def readOnly(self):
        return len(self.primaryKey)==0
    def getReader(self):
        r = PyrrhoReader('',self)
        r.local = self.rows
        return r
class PyrrhoReader:
    def __init__(self,cm,t):
        self.cmd = cm
        self.schema = t
        self.local = None
        self.row = PyrrhoRow(t)
        self.cells = []
        self.versions = dict()
        self.thread = current_thread()
        self.off = 0
    def _new(cmd):
        p = cmd.conn._receive()
        if p!=Responses.Schema:
            return None
        nc = cmd.conn._getInt()
        if nc==0:
            return None
        t = PyrrhoTable(cmd,cmd.conn._getString())
        cmd.conn._getSchema(t,nc)
        r = PyrrhoReader(cmd,t)
        return r
    def _getCell(self,cx):
        if self.off>=len(self.cells):
            self.off = 0
            self.cells = []
            self.versions = dict()
            self.cmd.conn._send(Protocol.ReaderData)
            p = self.cmd.conn._receive()
            if p!=Responses.ReaderData:
                return None
            n = self.cmd.conn._getInt()
            if n==0:
                return None
            j = cx
            for i in range(n):
                col = self.schema.columns[j]
                self.cells.append(self.cmd.conn._getCell(j,col.datatypename,col.type))
                if self.cmd.conn.rowCheck!='':
                    self.versions[i] = Versioned(self.cmd.conn.rowCheck,self.cmd.conn.readCheck)
                j += 1
                if j==len(self.schema.columns):
                    j = 0
        if self.off in self.versions:
            v = self.versions[self.off]
            self.row.check = v.rowCheck
            self.row.readCheck = v.readCheck
        c = self.cells[self.off]
        self.off += 1
        return c
    def close(self):
        self.cmd.conn._send(Protocol.CloseReader)
        self.cmd.conn.stream.flush()
    def col(self,nm):
        if self.thread!=current_thread():
            raise BaseException('This reader is for a different thread')
        return self.row.col(nm)
    def read(self):
        if self.thread!=current_thread():
            raise BaseException('This reader is for a different thread')
        if self.local!=None:
            self.row = next(self.local,[])
        else:
            self.row.cells = []
            for j in range(len(self.schema.columns)):
                c = self._getCell(j)
                if c==None:
                    break
                self.row.cells.append(c)
        return len(self.row.cells)!=0
    def type(self,i):
        if self.thread!=current_thread():
            raise BaseException('This reader is for a different thread')
        return self.row.type(i)
    def val(self,i):
        if self.thread!=current_thread():
            raise BaseException('This reader is for a different thread')
        return self.row.val(i)
class PyrrhoArray:
    def __init__(self):
        self.kind = ''
        self.data = []
    def str(self):
        sc = '('
        cm = ''
        for d in self.data:
            sc += cm+str(d)
            cm = ','
        return sc+')'
class PyrrhoCommand:
    def __init__(self,con):
        self.commandText = ''
        self.thread = current_thread()
        self.conn = con
    def executeReader(self):
        if not self.conn.isOpen:
            raise BaseException('Connection is not open')
        if self.thread!=current_thread():
            raise BaseException('This command is for a different thread')
        self.conn.commandLock.acquire()
        self.conn.commandLock.acquire()
        self.conn._send(Protocol.ExecuteReader,self.commandText)
        return PyrrhoReader._new(self) # will return None if no data
    def executeNonQuery(self):
        if not self.conn.isOpen:
            raise BaseException('Connection is not open')
        if self.thread!=current_thread():
            raise BaseException('This command is for a different thread')
        self.conn.commandLock.acquire()
        try:
            self.conn._send(Protocol.ExecuteNonQuery,self.commandText)
            p = self.conn._receive()
            if p!=Responses.Done:
                raise BaseException('Unexpected reply '+str(p))
            return self.conn._getInt()
        finally:
            self.conn.commandLock.release()
class PyrrhoTransaction:
    def __init__(self,con):
        self.conn = con
        self.active = True
        con._send(Protocol.BeginTransaction)
    def rollback(self):
        self.conn._send(Protocol.Rollback)
        self.conn.stream._flush()
        self.conn.stream._readByte()
        self.active = False
        self.conn.transactionThread = None
    def commit(self,obs=[]):
        self.conn._send(Protocol.CommitAndReport)
        self.conn._putInt(len(obs))
        for i in range(len(obs)):
            ss = obs[i].rowCheck.split(':')
            self.conn._putString(ss[0])
            self.conn._putLong(self.pos(ss[1]))
            self.conn._putLong(self.pos(ss[2]))
        self.active = False
        self.conn.transactionThread = None
        b = self.conn._receive()
        if b==Responses.TransactionReport:
            self.conn._getLong() # new schemaPos
            self.conn._getInt() # len(obs)
            for i in range(len(obs)):
                pa = self.conn._getString()
                d = self.conn._getLong()
                o = self.conn._getLong();
                obs[i].rowCheck = pa+':'+str(d)+':'+str(o)
    def pos(self,s):
        if s[0]=="'":
            return 0x400000000000+int(s[1:len(s)])
        return int(s)
class PyrrhoInterval:
    def __init__(self):
        self.years = 0
        self.months = 0
        self.ticks = 0
    def str(self):
        return str(self.years)+'Y'+str(self.months)+'M'+str(self.ticks)+'T'
class DocBase:
    def _getLength(bs,i):
        return unpack('i',bs[i:i+4])[0]
    def _bson(ob):
        if ob==None:
            return (10,bytes(0))
        if isinstance(ob,float):
            return (1,pack('d',ob))
        if isinstance(ob,str):
            b = bytes(ob,'utf-8')
            c = pack('i',len(b)+1)
            return (2,c+b+bytearray(1))
        if isinstance(ob,Document):
            return (3,ob.bytes())
        if isinstance(ob,DocArray):
            return (4,ob.bytes())
        if isinstance(ob,(bytes,bytearray)):
            return (5,bytes(ob))
        if isinstance(ob,ObjectId):
            return (7,ob.bytes())
        if isinstance(ob,bool):
            if ob:
                return (8,1)
            return (8,0)
        if isinstance(ob,(date,datetime)):
            return (9,pack('q',int(ob)))
        if isinstance(ob,int):
            return (18,pack('q',ob))
        return (6,bytes(ob))
    def _getValue(s,n,i):
        if i>=n:
            raise DocumentException('Value expected at' + str(i-1))
        c = s[i-1]
        if c=='"' or c=="'":
            return DocBase._getString(s,n,i)
        if c=='{':
            d = Document()
            i = d._fields(s,i,n)
            return (d,i)
        if c=='[':
            d = DocArray()
            i = d._items(s,i,n)
            return (d,i)
        if i+4<n and s[i:i+4]=='true':
            return (True,i+4)
        if i+5<n and s[i:i+5]=='false':
            return (False,i+5)
        if i+4<n and s[i:i+4]=='null':
            return (None,i+4)
        sg = c=='-'
        if sg and i<n:
            c = s[i]
            i += 1
        whole = 0
        if c.isdigit():
            i -= 1
            (whole,i) = DocBase._getHex(s,n,i)
            while i<n and s[i].isdigit():
                (a,i) = DocBase._getHex(s,n,i)
                whole = whole*10+a
        else:
            raise DocumentException('Value expected at '+str(i-1))
        if i>=n or (s[i]!='.' and s[i]!='e' and s[i]!='E'):
            if sg:
                whole = -whole
            return (whole,i)
        scale = 0
        if s[i]=='.':
            i+=1
            if i>=n or not s[i].isdigit():
                raise DocumentException('decimal part expected')
            while i<n and s[i].isdigit():
                (a,i) = DocBase._getHex(s,n,i)
                whole = whole*10+a
                scale += 1
        if i>=n or (s[i]!='e' and s[i]!='E'):
            m = Decimal(whole)
            while scale>0:
                scale -= 1
                m /= 10
            if sg:
                m = -m
            return (m,i)
        i += 1
        if i>=n:
            raise DocumentException('exponent part expected')
        esg = s[i]=='-'
        if s[i]=='-' or s[i]=='+':
            i += 1
        if i>=n or not s[i].isdigit():
            raise DocumentException('exponent part expected')
        exp = 0
        while i<n and s[i].isdigit():
            (a,i) = DocBase._getHex(s,n,i)
            exp = exp*10 + a
        if esg:
            exp = -exp
        dr = whole * pow(10.0,exp-scale)
        if sg:
            dr = -dr
        return (dr,i)
    def _getString(s,n,i):
        sb = ''
        q = s[i-1]
        while i<n:
            c = s[i]
            i += 1
            if c==q:
                return (sb,i)
            if c=='\\':
                (c,i) = DocBase._getEscape(s,n,i)
            sb += c
        raise DocumentException('Non-terminated string at '+str(i-1))
    def _getEscape(s,n,i):
        if i<n:
            c = s[i]
            i += 1
            if c=='"' or c=='\\' or c=='/':
                return (c,i)
            if c=='b':
                return ('\b',i)
            if c=='f':
                return ('\f',i)
            if c=='n':
                return ('\n',i)
            if c=='r':
                return ('\r',i)
            if c=='t':
                return ('\t',i)
            if c=='u' or c=='U':
                v = 0
                for j in range(4):
                    (a,i) = DocBase._getHex(s,n,i)
                    v = (v<<4)+a
                return (v,i)
        raise DocumentException('Illegal escape')
    def _getHex(s,n,i):
        if i<n:
            c = s[i]
            i += 1
            if (c>='0' and c<='9') or(c>='a' and c<='f') or (c>='A' and c<='F'):
                return (int(c,16),i)
        raise DocumentException('Hex digit expected at '+str(i-1))
    def _fromBson(t,bs,i):
        if t==1:
            return (unpack('d',bs[i:i+8])[0],i+8)
        if t==2:
            n = DocBase._getLength(bs,i)
            return (str(bs[i+4:i+3+n],'utf-8'),i+4+n)
        if t==3:
            n = DocBase._getLength(bs,i)
            return (Document().fromBson(bs,i),i+n)
        if t==4:
            n = DocBase._getLength(bs,i)
            return (DocArray().fromBson(bs,i),i+n)
        if t==5:
            n = DocBase._getLength(bs,i)
            return (bs[i:i+n],i+n)
        if t==7:
            return (ObjectId(bs[i:i+12]),i+12)
        if t==8:
            return (bs[i]!=0,i+1)
        if t==9:
            return (unpack('q',bs[i:i+8])[0],i+8)
        if t==10:
            return (None,i)
        if t==16:
            return (unpack('l',bs[i:i+4])[0],i+4)
        if t==18:
            return (unpack('q',bs[i:i+8])[0],i+8)
        if t==19: # Pyrrho
            n = DocBase._getLength(bs,i)
            return (Decimal(str(bs[i:i+n],'utf-8')),i+n)
        raise DocumentException('Unimplemented bson encoding '+str(t))
    def _str(v):
        if v==None:
            return 'null'
        if isinstance(v,str):
            return '"'+v+'"'
        if isinstance(v,list):
            c = ''
            r = '['
            for a in v:
                r += c+DocBase._str(a)
                c = ','
            return r
        if isinstance(v,DocBase):
            return v.str()
        s = str(v)
        if len(s)>0 and s[0]=='<':
            return JSON.stringify(v)
        return s
class ObjectId(DocBase):
    def __init__(self,bs):
        self.bytes = bs
    def str(self):
        s = '"'
        hex = '0123456789abcdef'
        for b in self.bytes:
            s += hex[(b>>4)&0xf] + hex[b&0xf]
        return s+'"'
class DocState(IntEnum): 
    StartKey = 0
    Key = 1
    Colon = 2
    StartValue = 3
    Comma = 4
class Document(DocBase):
    def __init__(self):
        self.fields = []
    def extract(self,cls,path,off=0):
        r = []
        if off>=len(path):
            r.append(self._extract(cls))
        else:
            for e in self.fields:
                if e[0]==path[off]:
                    s = []
                    if isinstance(e[1],[Document,DocArray]):
                        s = e[1].extract(cls,path,off+1)
                    for a in s:
                        r.append(a)
        return r
    def _extract(self,cls):
        r = cls()
        for e in self.fields:
          if e[0] in dir(r):
           if isinstance(e[1],Document):
             ob = getattr(r,e[0])
             setattr(r,e[0],e[1]._extract(ob.__class__))
           else:
             setattr(r,e[0],e[1]) 
        return r
    def parse(self,s):
        s = s.strip()
        n = len(s)
        if n==0 or s[0]!='{':
            raise DocumentException('{ expected')
        i = self._fields(s,1,n)
        if i!=n:
            raise DocumentException('unparsed input at '+str(i-1))
        return self
    def _getField(self,k):
        for p in self.fields:
            if p[0]==k:
                return p[1]
        return None
    def _setField(self,k,v):
        self.fields.append((k,v))
    def _fields(self,s,i,n):
        state = DocState.StartKey
        kb = ''
        kq = True
        while i<n:
            c = s[i]
            i += 1
            if state==DocState.StartKey:
                kb = ''
                kq = True
                if c.isspace():
                    continue
                if c=='}' and len(self.fields)==0:
                    return i
                if c!='"':
                    if (not c.isalpha()) and c!='_' and c!='$' and c!='.':
                        raise DocumentException('expected name at '+ str(i-1))
                    kq = False
                    kb += c
                state = DocState.Key
                continue
            elif state==DocState.Key:
                if c=='"':
                    state = DocState.Colon
                    continue
                if c==':' and not kq:
                    state = DocState.StartValue
                    continue
                if c=='\\':
                    (c,i) = DocBase._getEscape(s,n,i)
                kb += c
                continue
            elif state==DocState.Colon:
                if c.isspace():
                    continue
                if c!=':':
                    raise DocumentException('Expected : at '+str(i-1))
                state = DocState.StartValue;
                continue;
            elif state==DocState.StartValue:
                if c.isspace():
                    continue
                (v,i) = DocBase._getValue(s,n,i)
                self.fields.append((kb,v))
                state = DocState.Comma
                continue
            elif state==DocState.Comma:
                if c.isspace():
                    continue
                if c=='}':
                    return i
                if c!=',':
                    raise DocumentException('Expected , at '+str(i-1))
                state = DocState.StartKey
                continue
        raise DocumentException('Imcomplete syntax at '+str(i-1))
    def build(self,ob):
        if ob==None:
            return
        for f in dir(ob):
            if f[0]!='_':
                v = getattr(ob,f)
                if isinstance(v,list):
                    v = DocArray().build(v)
                elif not isinstance(v,(int,str,float,bool)):
                    v = Document().build(v)
                self.fields.append((f,v))
        return self
    def fromBson(self,bs,off=0):
        n = DocBase._getLength(bs,off)
        i = off+4
        while i<off+n-1:
            t = bs[i]
            i += 1
            c = 0
            s = i
            while i<off+n and bs[i]!=0:
                i += 1
                c += 1
            i+=1
            key = str(bs[s:s+c],'utf-8')
            (a,i) = DocBase._fromBson(t,bs,i)
            self.fields.append((key,a))
        return self
    def bytes(self):
        r = bytearray(4)
        for f in self.fields:
            a = DocBase._bson(f[1])
            r.append(a[0])
            r = r+bytes(f[0],'utf-8')
            r.append(0)
            r = r+a[1]
        bn = pack('i',len(r))
        for i in range(4):
            r[i] = bn[i]
        return r
    def str(self):
        r = '{'
        c = ''
        for f in self.fields:
            r += c+'"'+f[0]+'": '+DocBase._str(f[1])
            c = ','
        return r + '}'
class DocArray(DocBase):
    def __init__(self):
        self.items = []
    def build(self,ar):
        for a in ar:
            v = a
            if isinstance(a,list):
                v = DocArray().build(v)
            elif not isinstance(a,(int,str,float,bool)):
                v = Document().build(v)
            self.items.append(v)
        return self
    def parse(self,s):
        if s=='':
            return
        s = s.strip()
        n = len(s)
        if n<=2 or s[0]!='[' or s[n-1]!=']':
            raise DocumentException('[..] expected')
        i = self._items(s,1,n)
        if i!=n:
            raise DocumentException('bad DocArray format')
    def fromBson(self,bs,off=0):
        n = DocBase._getLength(bs,off)
        i = off+4
        while i<off+n-1: # ignore final \0
            t = bs[i]
            i += 1
            c = 0
            s = i
            while i<off+n and bs[i]!=0:
                i += 1
                c += 1
            i += 1
            (a,i) = DocBase._fromBson(t,bs,i)
            self.items.append(a)
        return self
    def extract(self,cls,path,off=0):
        r = []
        for e in self.items:
            if isinstance(e,Document):
                for a in e.extract(cls,path,off):
                    r.append(a)
        return r
    def _extract(self,cls):
        r = []
        for e in self.items:
            if isinstance(e,Document):
                r.append(e._extract(cls))
            elif isinstance(e,cls):
                r.append(e)
        return r
    def _items(self,s,i,n):
        state = DocState.StartValue
        while i<n:
            c = s[i]
            i+=1
            if c.isspace():
                continue
            if c==']' and len(self.items)==0:
                break
            if state==DocState.StartValue:
                (a,i) = DocBase._getValue(s,n,i)
                self.items.append(a)
                state = DocState.Comma
                continue
            else:
                if c==']':
                    break
                if c!=',':
                    raise DocumentException(', expected')
                state = DocState.StartValue
                continue
        return i
    def bytes(self):
        r = bytearray(4)
        for i in range(len(self.items)):
            a = bson(self.items[i])
            r.append(a[0])
            r+=pack('i',str(i,'utf-8'))
            r.append(0)
            r+=a[1]
            r.append(0)
        bn = pack('i',len(r))
        for i in range(4):
            r[i] = bn[i]
        return r
    def str(self):
        s = '['
        c = ''
        for a in self.items:
            s += c+DocBase._str(a)
            c = ','
        return s+']'
class CellValue:
    def __init__(self,t):
        self.subType = ''
        self.type = t
        self.val = None
    def str(self):
        if val==None:
            return ''
        return str(val)
class Versioned:
    def __init__(self,c,v):
        self.rowCheck = c
        self.readCheck = v
        self._schemakey = 0
        self._key = []
class PPStream(RawIOBase): # would be called AsyncStream if it was actually async
    def __init__(self,con,sock):
        self.rbuf = bytearray(2048)
        self.wbuf= bytearray(2048)
        self.rcount = 0
        self.rpos = 2
        self.wcount = 2
        self.connect = con
        self.client = sock
    def _flush(self):
        if self.wcount!=2:
            self.wcount -=2
            self.wbuf[0] = self.wcount>>7
            self.wbuf[1] = self.wcount &0x7f
            self.client.sendall(self.wbuf)
            self.wcount = 2
            self.rcount = 0
            self.rpos = 2
    def _readByte(self):
        if self.wcount!=2:
            self._flush()
        if self.rpos>=self.rcount+2:
            self.rpos = 2
            self.rcount = 0
            self.rbuf = self.client.recv(2048)
            if len(self.rbuf)==0:
                self.rcount = 0
                return -1
            rc = len(self.rbuf)
            while rc<2048:
                bx = self.client.recv(2048-rc)
                if len(bx)==0:
                    self.rcount = 0
                    return -1
                self.rbuf += bx
                rc = len(self.rbuf)
            self.rcount = (self.rbuf[0]<<7)+self.rbuf[1] 
            if self.rcount==2047:
                return self._getException()
        c = self.rbuf[self.rpos]
        self.rpos+=1
        return c
    def _getException(self):
        self.rcount = (self.rbuf[self.rpos]<<7) + self.rbuf[self.rpos+1] +2 
        proto = self.rbuf[self.rpos+2]
        self.rpos += 3
        if proto== Responses.OobException:
            e = DatabaseError("2E205",[])
        elif proto==Responses.Exception:
            sig = self.connect._getString()
            e = DatabaseError(sig,self.connect._getStrings())
        elif proto==Responses.FatalError:
            e = DatabaseError("2E206",[self.connect._getString()])
        elif proto==Responses.TransactionConflict:
            e = DatabaseError("40001",[self.connect.GetString()])
        else:
            return proto
        while self.rpos<self.rcount:
            k = self.connect._getString()
            v = self.connect._getString()
            e.info[k] = v
        e.info["CONDITION_NUMBER"] = e.message
        if "RETURNED_SQLSTATE" not in e.info:
            e.info["RETURNED_SQLSTATE"] = e.sig
        if "MESSAGE_TEXT" not in e.info:
            e.info["MESSAGE_TEXT"] = e.message
        if "MESSAGE_LENGTH" not in e.info:
            e.info["MESSAGE_LENGTH"] = str(len(e.message))
        if "MESSAGE_OCTET_LENGTH" not in e.info:
            e.info["MESAAGE_OCTET_LENGTH"] = str(len(bytes(e.message,'utf-8')))
        self.rcount = 0
        self.rpos = 2
        self.wcount = 2
        raise e
    def _writeByte(self,b):
        if self.wcount<2048:
            self.wbuf[self.wcount] = b
            self.wcount+=1
        if self.wcount>=2048:
            self.wcount -=2
            self.wbuf[0] = self.wcount>>7
            self.wbuf[1] = self.wcount &0x7f
            self.client.send(self.wbuf)
            self.wcount = 2
    def read(self,size):
        b = bytearray(size)
        for j in range(size):
            x = self._readByte()
            if x<0:
                break
            b[j] = x
        return b
    def readinto(self,buf):
        for j in range(len(buf)):
            x = self._readByte()
            if x<0:
                break
            buf[j] = x
    def write(self,buf):
        for j in range(len(buf)):
            self._writeByte(buf[j])
class Crypt:
    def __init__(self,strm,key):
        self.stream = strm
        Crypt.openSource = True
        self.state = key
        Crypt.mult = 73928681
    def sendConnectionString(self,cs,us):
        fields = cs.split(';')
        self._send(21,us)
        for f in fields:
            m = f.index('=')
            n = f[0:m]
            v = f[m+1:len(f)]
            if n=='Provider':
                pass
            elif n=='Host':
                self._send(26,v)
            elif n=='Port':
                pass
            elif n=='Files':
                self._send(22,v)
            elif n=='Role':
                self._send(23,v)
            elif n=='Stop':
                self._send(25,v)
            elif n=='User':
                self._send(21,v)
            elif n=='Base':
                self._send(29,v)
            elif n=='BaseServer':
                self._send(31,v)
            elif n=='Coordinator':
                self._send(30,v)
            elif n=='Password':
                self._send(20,v)
            elif n=='Modify':
                self._send(32,v)
            elif n=='User':
                pass
            else:
                raise Exception('bad connection string'+cs)
        self._send(24)
    def _setKey(self,k):
        self.state = k
    def _encrypt(self,b):
        if isinstance(b,str): # 2.7
            b = ord(b) # 2.7
        c = (b+self.state)&0xff 
        self.state = ((self.state*Crypt.mult)& 0xffffffffffffffff)>>8 # mimic 64 bit
        if self.state>0x7fffffffffffff:
            self.state |= 0xff00000000000000 # fix sign extend
        elif self.state==0:
            self.state = 1
        return c
    def _decrypt(self,c):
        b = (c-self.state)&0xff 
        self.state = ((self.state*Crypt.mult)&0xffffffffffffffff)>>8 # mimic 64 bit
        if self.state>0x7fffffffffffff:
            self.state |= 0xff00000000000000 # fix sign extend
        elif self.state==0:
            self.state = 1
        return b
    def _write(self,b):
        n = len(b)
        c = bytearray(n)
        for i in range(n):
            c[i] = self._encrypt(b[i])
        self.stream.write(c)
    def _readinto(self,b):
        n = self.stream.readinto(b)
        for i in range(n):
            b[i] = self._decrypt(b[i])
        return n
    def _read(self,n):
        b = bytearray(n)
        return self._readinto(b)
    def _getInt(self):
        b = self._read(4)
        n = 0
        for j in range(4):
            n = (n<<8)+b[j]
        return n
    def _getLong(strm): # cleatext
        b = strm.read(8)
        n = 0
        for j in range(8):
            n = (n<<8)+b[j]
        return n
    def _getString(self):
        return str(self._read(self._getInt()),'ascii')
    def _putInt(self,n):
        b = bytearray(4)
        b[0] = (n>>24)&0xf
        b[1] = (n>>16)&0xf
        b[2] = (n>>8)&0xf
        b[3] = n&0xf
        self._write(b)
    def _putString(self,s):
        b = bytes(s,'ascii') 
        self._putInt(len(b))
        self._write(b)
    def _send(self,p,text=None):
        self._write([p])
        if text!=None:
            self._putString(text)
        self.stream.flush()
    def close(self):
        self.stream.close
class WebCtlr:
    def allowAnonymous(self):
        return False
    def delete(self,ws,ps):
        return ''
    def get(self,ws,ps):
        return ''
    def post(self,ws,ps):
        return ''
    def put(self,ws,ps):
        return ''
class WebSvh(BaseHTTPRequestHandler):
    def __init__(self,req,cli,svc):
        self.svc = svc
        self.status = 200
        self.mime = 'text/plain'
        super().__init__(req,cli,svc)
    def handle_one_request(self):
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if len(self.raw_requestline) > 65536:
                self.requestline = ''
                self.request_version = ''
                self.command = ''
                self.send_error(414)
                return
            if not self.raw_requestline:
                self.close_connection = 1
                return
            if not self.parse_request():
                # An error code has been sent, just exit
                return
            self.svc._open(self)
            mess = self.svc.serve(self.command,self.svc.params)
            self.sendResponse(mess)
            self.wfile.flush() #actually send the response if not already done.
        except:
            return
    def log_request(self,code):
        return
    def sendResponse(self,mess):
         self.send_response(self.status)
         if self.status==401:
             self.send_header('WWW-Authenticate','Basic realm='+self.svc.__class__.__name__)
         if self.status!=200:
             self.mime = 'text/plain' 
         self.send_header('Content-type',self.mime)
         self.end_headers()
         if isinstance(mess,str):
             mess = bytes(mess,'utf-8')
         self.wfile.write(mess) 
    def getData(self):
        if not 'Content-Length' in self.headers: 
           return None         
        h = self.headers['Content-Length']
        n = int(h)
        return str(self.rfile.read(n),'utf-8')
class WebSvc:
    controllers = dict()
    def __init__(self):
        self.loginPage=''
        self.checkForLoginPage('Login.htm')
        self.checkForLoginPage('Login.html')
        self.logging = self.checkForLogFile()
        self.params = []
        self.user = None
        self.handler = None
        self.controller = None
        self.controllerName = 'Home'
        self.password = None
    def checkForLoginPage(self, v):
        if os.path.exists('Pages/'+v):
            self.loginPage = v
    def checkForLogFile(self):
        return os.path.exists('Log.txt')
    def _open(self,hdlr):
        self.params = []
        self.handler = hdlr
        u = hdlr.path.split('/')
        if len(u)>2:
           if u[2] in WebSvc.controllers:
                self.controller = WebSvc.controllers[u[2]]
           else:
                self.params.append(u[2])
        if len(u)>=4 and u[3]!='':
            n = urllib.parse.unquote(u[3])
            if len(n)>0 and n[0]=='{':
                self.params.append(Document().parse(n))
            else:
                self.params.append(n)
                for i in range(2,len(u)-2):
                    if u[i+2]!='':
                        self.params.append(urllib.parse.unquote(u[i+2]))
        gd = hdlr.getData()
        if gd!=None and gd!='' and gd!='undefined':
            self.params.append(Document().parse(gd))
        self.open()
    def open(self):
        pass
    def serve(self,m,ps):
        mc = ''
        ret = 'OK'
        postData = ''
        if m=='GET':
            n = len(ps)
            if n>=1 and isinstance(ps[0],str):
                v = ps[0]
                try:
                    if v.endswith('.js'):
                        self.handler.mime = 'application/javascript'
                        return open('Scripts/'+v,'rb',0).readall()
                    if v.endswith('.htm') or v.endswith('.html'):
                        self.handler.mime = 'text/html'
                        if self.loginPage!='':
                            if self.controller==None or not self.controller.allowAnonymous():
                                if not self.authenticated():
                                    if self.user==None:
                                        self.handler.status = 401
                                        return 'UNAUTHORISED'
                                    v = self.loginPage
                        return open('Pages/'+v,'rb',0).readall()
                    if v.endswith('.css'):
                        self.handler.mime = 'application/css'
                        return open('Styles/'+v,'rb',0).readall()
                except FileNotFoundError as e:
                    self.handler.status = 400
                    return e.filename+" not found"
            mc = 'get'
        elif m =='PUT':
            mc = 'put'
        elif m == 'POST':
            mc = 'post'
        elif m == 'DELETE':
            mc = 'delete'
        else:
            ret = 'Unsupported method '+m
        if self.controller==None:
            self.handler.status = 400
            if self.controllerName !='':
                return 'No controller for '+self.controllerName
            return 'NOT FOUND'
        if (not self.controller.allowAnonymous()) and (not self.authenticated()):
            self.handler.status = 401
            return 'UNAUTHORISED'
        try:
            mth = getattr(self.controller,mc)
            ret = mth(self,ps)
        except Exception as e:
            self.handler.status = 403
            return str(e)
        self.log(mc,self.controllerName,ps)
        return ret
    def close(self):
        pass
    def authenticated(self):
        if self.loginPage=='':
            return True
        if self.controller!=None and self.controller.allowAnonymous():
            return True
        if not 'Authorization' in self.handler.headers: 
            return False
        h = self.handler.headers['Authorization'] 
        d = str(base64.b64decode(h[6:len(h)]),'utf-8') 
        s = d.split(':')
        self.user = s[0]
        self.password = s[1]
        return True
    def log(self,m,u,ps):
        if not self.logging:
            return
        logFile = open('Log.txt','a')
        s = str(datetime.now())
        if self.user!=None:
            s += ' ('+self.user+')'
        s += ': '+m+' '+str(u)
        for p in ps:
            s += '|<'+p+'>|'
        logFile.writelines([s])
        logFile.close()
class WebSvl(HTTPServer):
    def __init__(self,hp,hc,sv):
        self.svc = sv
        super().__init__(hp,hc)
    def run(self,cli,req,ws):
        self.RequestHandlerClass(req,cli,ws)
        self.shutdown_request(req)
        ws.close()
    def serve_forever(self):
        while True:
            try: 
                req,cli = self.get_request()
                ws = self.svc.factory()
                if ws==self.svc:
                    self.run(cli,req,ws)
                else:
                    Thread(target=self.run,args=(cli,req,ws)).start()
            except OSError as e:
                break
            except Exception as e:
                pass
class WebSvr(WebSvc):
    def factory(self):
        return self
    def authenticated(self):
        return super().authenticated()
    def server(self,address,port):
        try:
            listener = WebSvl((address,port),WebSvh,self)
            listener.serve_forever()
        except KeyboardInterrupt:
            print('Exiting')
        listener.socket.close()
