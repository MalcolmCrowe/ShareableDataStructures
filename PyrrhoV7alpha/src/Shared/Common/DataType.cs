using System;
using System.CodeDom;
using System.Runtime.ExceptionServices;
using System.Text;
using Pyrrho.Level3;
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
/// Everything in the Common namespace is Immutable and Shareabl
/// </summary>
namespace Pyrrho.Common
{
    /// <summary>
    /// Sqlx enumerates the tokens of SQL2011, mostly defined in the standard
    /// The order is only roughly alphabetic
    /// </summary>
    public enum Sqlx
    {
        Null = 0,
        // reserved words (not case sensitive) SQL2011 vol2, vol4, vol14 + // entries
        // 3 alphabetical sequences: reserved words, token types, and non-reserved words
        ///===================RESERVED WORDS=====================
        // last reserved word must be YEAR (mentioned in code below); XML is a special case
        ABS = 1,
        ALL = 2,
        ALLOCATE = 3,
        ALTER = 4,
        AND = 5,
        ANY = 6,
        ARE = 7, // ARRAY see 11
        ARRAY_AGG = 8,
        ARRAY_MAX_CARDINALITY = 9,
        AS = 10,
        ARRAY = 11, // must be 11
        ASENSITIVE = 12,
        ASYMMETRIC = 13,
        ATOMIC = 14,
        AUTHORIZATION = 15,
        AVG = 16, //
        BEGIN = 17,
        BEGIN_FRAME = 18,
        BEGIN_PARTITION = 19,
        BETWEEN = 20,
        BIGINT = 21,
        BINARY = 22,
        BLOB = 23, // BOOLEAN see 27
        BOTH = 24,
        BY = 25,
        CALL = 26,
        BOOLEAN = 27, // must be 27
        CALLED = 28,
        CARDINALITY = 29,
        CASCADED = 30,
        CASE = 31,
        CAST = 32,
        CEIL = 33,
        CEILING = 34, // CHAR see 37
        CHAR_LENGTH = 35,
        CHARACTER = 36,
        CHAR = 37, // must be 37: see also CHARLITERAL for literal 
        CHARACTER_LENGTH = 38,
        CHECK = 39, // CLOB see 40
        CLOB = 40, // must be 40
        CLOSE = 41,
        COALESCE = 42,
        COLLATE = 43,
        COLLECT = 44,
        COLUMN = 45,
        COMMIT = 46,
        CONDITION = 47,
        CONNECT = 48,
        CONSTRAINT = 49,
        CONTAINS = 50,
        CONVERT = 51,
#if OLAP
        CORR =52,
#endif
        CORRESPONDING = 53,
        COUNT = 54, //
#if OLAP
        COVAR_POP =55,
        COVAR_SAMP =56,
#endif
        CREATE = 57,
        CROSS = 58,
#if OLAP
        CUBE =59,
        CUME_DIST =60,
#endif
        CURRENT = 61,
        CURRENT_CATALOG = 62,
        CURRENT_DATE = 63,
        CURRENT_DEFAULT_TRANSFORM_GROUP = 64,
        CURSOR = 65, // must be 65
        CURRENT_PATH = 66,
        DATE = 67, // must be 67	
        CURRENT_ROLE = 68,
        CURRENT_ROW = 69,
        CURRENT_SCHEMA = 70,
        CURRENT_TIME = 71,
        CURRENT_TIMESTAMP = 72,
        CURRENT_TRANSFORM_GROUP_FOR_TYPE = 73,
        CURRENT_USER = 74, // CURSOR see 65
        CYCLE = 75, // DATE see 67
        DAY = 76,
        DEALLOCATE = 77,
        DEC = 78,
        DECIMAL = 79,
        DECLARE = 80,
        DEFAULT = 81,
        DELETE = 82,
#if OLAP
        DENSE_RANK =83,
#endif
        DEREF = 84,
        DESCRIBE = 85,
        DETERMINISTIC = 86,
        DISCONNECT = 87,
        DISTINCT = 88,
        DO = 89, // from vol 4
        DOCARRAY = 90, // Pyrrho 5.1
        DOCUMENT = 91, // Pyrrho 5.1
        DOUBLE = 92,
        DROP = 93,
        DYNAMIC = 94,
        EACH = 95,
        ELEMENT = 96,
        ELSE = 97,
        ELSEIF = 98, // from vol 4
        END = 99,
        END_EXEC = 100, // misprinted in SQL2011 as END-EXEC
        END_FRAME = 101,
        END_PARTITION = 102,
        EOF = 103,	// Pyrrho 0.1
        EQUALS = 104,
        ESCAPE = 105,
        EVERY = 106,
        EXCEPT = 107,
        EXEC = 108,
        EXECUTE = 109,
        EXISTS = 110,
        EXIT = 111, // from vol 4
        EXP = 112,
        EXTERNAL = 113,
        EXTRACT = 114,
        FALSE = 115,
        FETCH = 116,
        FILTER = 117,
        FIRST_VALUE = 118,
        FLOAT = 119,
        FLOOR = 120,
        FOR = 121,
        FOREIGN = 122,
        FREE = 123,
        FROM = 124,
        FULL = 125,
        FUNCTION = 126,
        FUSION = 127,
        GET = 128,
        GLOBAL = 129,
        GRANT = 130,
        GROUP = 131,
        GROUPING = 132,
        GROUPS = 133,
        HANDLER = 134, // vol 4 
        INT = 136,  // deprecated: see also INTEGERLITERAL
        INTEGER = 135, // must be 135
        HAVING = 138,
        INTERVAL0 = 137,  // must be 137 (old version of INTERVAL)
        HOLD = 139,
        HOUR = 140,
        IDENTITY = 141,
        IF = 142,  // vol 4
        IN = 143,
        INDICATOR = 144,
        INNER = 145,
        INOUT = 146,
        INSENSITIVE = 147,
        INSERT = 148, // INT is 136, INTEGER is 135
        INTERSECT = 149,
        INTERSECTION = 150,
        INTO = 151,
        IS = 152,
        INTERVAL = 153, // must be 152 see also INTERVAL0
        ITERATE = 154, // vol 4
        JOIN = 155,
        LAG = 156,
        LANGUAGE = 157,
        LARGE = 158,
        LAST_DATA = 159, // Pyrrho v7
        LAST_VALUE = 160,
        LATERAL = 161,
        LEADING = 162,
        LEAVE = 163, // vol 4
        LEFT = 164,
        LIKE = 165,
        LN = 166,
        LOCAL = 167,
        MULTISET = 168, // must be 168
        LOCALTIME = 169,
        LOCALTIMESTAMP = 170,
        NCHAR = 171, // must be 171	
        NCLOB = 172, // must be 172
        LOOP = 173,  // vol 4
        LOWER = 174,
        MATCH = 175,
        MAX = 176,
        NULL = 177, // must be 177
        MEMBER = 178,
        NUMERIC = 179, // must be 179
        MERGE = 180,
        METHOD = 181,
        MIN = 182,
        MINUTE = 183,
        MOD = 184,
        MODIFIES = 185,
        MODULE = 186,
        MONTH = 187,	 // MULTISET is 168
        NATIONAL = 188,
        NATURAL = 189, // NCHAR 171 NCLOB 172
        NEW = 190,
        NO = 191,
        NONE = 192,
        NORMALIZE = 193,
        NOT = 194,
        NULLIF = 195, // NUMERIC 179, see also NUMERICLITERAL
        OBJECT = 196,
        OCCURRENCES_REGEX = 197, // alphabetical sequence is a misprint in SQL2008
        OCTET_LENGTH = 198,
        REAL0 = 199, // must be 199, previous version of REAL
        OF = 200,
        OFFSET = 201,
        OLD = 202,
        REAL = 203, // must be 203, see also REAL0 and REALLITERAL
        ON = 204,
        ONLY = 205,
        OPEN = 206,
        OR = 207,
        ORDER = 208,
        OUT = 209,
        OUTER = 210,
        OVER = 211,
        OVERLAPS = 212,
        OVERLAY = 213,
        PARAMETER = 214,
        PARTITION = 215,
        PERCENT = 216,
        PERIOD = 217,
        PASSWORD = 218, // must be 218, Pyrrho v5
        PORTION = 219,
        POSITION = 220,
        POWER = 221,
        PRECEDES = 222,
        PRECISION = 223,
        PREPARE = 224,
        PRIMARY = 225,
        PROCEDURE = 226,
        RANGE = 227,
        RANK = 228,
        READS = 229,    // REAL 203 (previously 199 see REAL0)
        RECURSIVE = 230,
        REF = 231,
        REFERENCES = 232,
        REFERENCING = 233,
        RELEASE = 234,
        REPEAT = 235, // vol 4
        RESIGNAL = 236, // vol 4
        RESULT = 237,
        RETURN = 238,
        RETURNS = 239,
        REVOKE = 240,
        RIGHT = 241,
        ROLLBACK = 242,
        ROW = 243,
        ROW_NUMBER = 244,
        ROWS = 245,
        SAVEPOINT = 246,
        SCOPE = 247,
        SCROLL = 248,
        SEARCH = 249,
        SECOND = 250,
        SELECT = 251,
        SENSITIVE = 252,    // has a different usage in Pyrrho
        SESSION_USER = 253,
        SET = 254,
        SIGNAL = 255, //vol 4
        SMALLINT = 256,
        TIME = 257, // must be 257
        TIMESTAMP = 258, // must be 258
        SOME = 259,
        SPECIFIC = 260,
        SPECIFICTYPE = 261,
        SQL = 262,
        SQLEXCEPTION = 263,
        SQLSTATE = 264,
        SQLWARNING = 265,
        SQRT = 266,
        TYPE = 267, // must be 267 but is not a reserved word
        START = 268,
        STATIC = 269,
        STDDEV_POP = 270,
        STDDEV_SAMP = 271,
        SUBMULTISET = 272,
        SUBSTRING = 273, //
        SUBSTRING_REGEX = 274,
        SUCCEEDS = 275,
        SUM = 276, //
        SYMMETRIC = 277,
        SYSTEM = 278,
        SYSTEM_TIME = 279,
        SYSTEM_USER = 280,  // TABLE is 297
        TABLESAMPLE = 281,
        THEN = 282,  // TIME is 257, TIMESTAMP is 258	
        TIMEZONE_HOUR = 283,
        TIMEZONE_MINUTE = 284,
        TO = 285,
        TRAILING = 286,
        TRANSLATE = 287,
        TRANSLATION = 288,
        TREAT = 289,
        TRIGGER = 290,
        TRIM = 291,
        TRIM_ARRAY = 292,
        TRUE = 293,
        TRUNCATE = 294,
        UESCAPE = 295,
        UNION = 296,
        TABLE = 297, // must be 297
        UNIQUE = 298,
        UNKNOWN = 299,
        UNNEST = 300,
        UNTIL = 301, // vol 4
        UPDATE = 302,
        UPPER = 303, //
        USER = 304,
        USING = 305,
        VALUE = 306,
        VALUE_OF = 307,
        VALUES = 308,
        VARBINARY = 309,
        VARCHAR = 310,
        VARYING = 311,
        VERSIONING = 312,
        WHEN = 313,
        WHENEVER = 314,
        WHERE = 315,
        WHILE = 316, // vol 4
        WINDOW = 317,
        WITH = 318,
        WITHIN = 319,
        WITHOUT = 320, // XML is 356 vol 14
        XMLAGG = 321, // Basic XML stuff + XMLAgg vol 14
        XMLATTRIBUTES = 322,
        XMLBINARY = 323,
        XMLCAST = 324,
        XMLCOMMENT = 325,
        XMLCONCAT = 326,
        XMLDOCUMENT = 327,
        XMLELEMENT = 328,
        XMLEXISTS = 329,
        XMLFOREST = 330,
        XMLITERATE = 331,
        XMLNAMESPACES = 332,
        XMLPARSE = 333,
        XMLPI = 334,
        XMLQUERY = 335,
        XMLSERIALIZE = 336,
        XMLTABLE = 337,
        XMLTEXT = 338,
        XMLVALIDATE = 339,
        YEAR = 340,	// last reserved word
        //====================TOKEN TYPES=====================
        ASSIGNMENT = 341, // := 
        BLOBLITERAL = 342, // 
        BOOLEANLITERAL = 343,
        CHARLITERAL = 344, //
        COLON = 345, // :
        COMMA = 346,  // ,        
        CONCATENATE = 347, // ||        
        DIVIDE = 348, // /        
        DOCUMENTLITERAL = 349, // v5.1
        DOT = 350, // . 5.2 was STOP
        DOUBLECOLON = 351, // ::        
        EMPTY = 352, // []        
        EQL = 353,  // =        
        GEQ = 354, // >=    
        GTR = 355, // >    
        XML = 356, // must be 356 (is a reserved word)
        ID = 357, // identifier
        INTEGERLITERAL = 358, // Pyrrho
        LBRACE = 359, // {
        LBRACK = 360, // [
        LEQ = 361, // <=
        LPAREN = 362, // (
        LSS = 363, // <
        MEXCEPT = 364, // 
        MINTERSECT = 365, //
        MINUS = 366, // -
        MUNION = 367, // 
        NEQ = 368, // <>
        NUMERICLITERAL = 369, // 
        PLUS = 370, // + 
        QMARK = 371, // ?
        RBRACE = 372, // } 
        RBRACK = 373, // ] 
        RDFDATETIME = 374, //
        RDFLITERAL = 375, // 
        RDFTYPE = 376, // Pyrrho 7.0
        REALLITERAL = 377, //
        RPAREN = 378, // ) 
        SEMICOLON = 379, // ; 
        TIMES = 380, // *
        VBAR = 381, // | 
        //=========================NON-RESERVED WORDS================
        A = 382, // first non-reserved word
        ABSOLUTE = 383,
        ACTION = 384,
        ADA = 385,
        ADD = 386,
        ADMIN = 387,
        AFTER = 388,
        ALWAYS = 389,
        APPLICATION = 390, // Pyrrho 4.6
        ASC = 391,
        ASSERTION = 392,
        ATTRIBUTE = 393,
        ATTRIBUTES = 394,
        BEFORE = 395,
        BERNOULLI = 396,
        BREADTH = 397,
        BREAK = 398, // Pyrrho
        C = 399,
        CAPTION = 400, // Pyrrho 4.5
        CASCADE = 401,
        CATALOG = 402,
        CATALOG_NAME = 403,
        CHAIN = 404,
        CHARACTER_SET_CATALOG = 405,
        CHARACTER_SET_NAME = 406,
        CHARACTER_SET_SCHEMA = 407,
        CHARACTERISTICS = 408,
        CHARACTERS = 409,
        CLASS_ORIGIN = 410,
        COBOL = 411,
        COLLATION = 412,
        COLLATION_CATALOG = 413,
        COLLATION_NAME = 414,
        COLLATION_SCHEMA = 415,
        COLUMN_NAME = 416,
        COMMAND_FUNCTION = 417,
        COMMAND_FUNCTION_CODE = 418,
        COMMITTED = 419,
        CONDITION_NUMBER = 420,
        CONNECTION = 421,
        CONNECTION_NAME = 422,
        CONSTRAINT_CATALOG = 423,
        CONSTRAINT_NAME = 424,
        CONSTRAINT_SCHEMA = 425,
        CONSTRAINTS = 426,
        CONSTRUCTOR = 427,
        CONTENT = 428,
        CONTINUE = 429,
        CSV = 430, // Pyrrho 5.5
        CURATED = 431, // Pyrrho
        CURSOR_NAME = 432,
        DATA = 433,
        DATABASE = 434, // Pyrrho
        DATETIME_INTERVAL_CODE = 435,
        DATETIME_INTERVAL_PRECISION = 436,
        DEFAULTS = 437,
        DEFERRABLE = 438,
        DEFERRED = 439,
        DEFINED = 440,
        DEFINER = 441,
        DEGREE = 442,
        DEPTH = 443,
        DERIVED = 444,
        DESC = 445,
        DESCRIPTOR = 446,
        DIAGNOSTICS = 447,
        DISPATCH = 448,
        DOMAIN = 449,
        DYNAMIC_FUNCTION = 450,
        DYNAMIC_FUNCTION_CODE = 451,
        ENFORCED = 452,
        ENTITY = 453, // Pyrrho 4.5
        ETAG = 454, // Pyrrho Metadata 7.0
        EXCLUDE = 455,
        EXCLUDING = 456,
        FINAL = 457,
        FIRST = 458,
        FLAG = 459,
        FOLLOWING = 460,
        FORTRAN = 461,
        FOUND = 462,
        G = 463,
        GENERAL = 464,
        GENERATED = 465,
        GO = 466,
        GOTO = 467,
        GRANTED = 468,
        HIERARCHY = 469,
        HISTOGRAM = 470, // Pyrrho 4.5
        HTTPDATE = 471, // Pyrrho 7 RFC 7231
        IGNORE = 472,
        IMMEDIATE = 473,
        IMMEDIATELY = 474,
        IMPLEMENTATION = 475,
        INCLUDING = 476,
        INCREMENT = 477,
        INITIALLY = 478,
        INPUT = 479,
        INSTANCE = 480,
        INSTANTIABLE = 481,
        INSTEAD = 482,
        INVERTS = 483, // Pyrrho Metadata 5.7
        INVOKER = 484,
        IRI = 485, // Pyrrho 7
        ISOLATION = 486,
        JSON = 487, // Pyrrho 5.5
        K = 488,
        KEY = 489,
        KEY_MEMBER = 490,
        KEY_TYPE = 491,
        LAST = 492,
        LEGEND = 493, // Pyrrho Metadata 4.8
        LENGTH = 494,
        LEVEL = 495,
        LINE = 496, // Pyrrho 4.5
        LOCATOR = 497,
        M = 498,
        MAP = 499,
        MATCHED = 500,
        MAXVALUE = 501,
        MESSAGE_LENGTH = 502,
        MESSAGE_OCTET_LENGTH = 503,
        MESSAGE_TEXT = 504,
        METADATA = 505, // Pyrrho 7
        MILLI = 506, // Pyrrho 7
        MIME = 507, // Pyrrho 7
        MINVALUE = 508,
        MONOTONIC = 509, // Pyrrho 5.7
        MORE = 510,
        MUMPS = 511,
        NAME = 512,
        NAMES = 513,
        NESTING = 514,
        NEXT = 515,
        NFC = 516,
        NFD = 517,
        NFKC = 518,
        NFKD = 519,
        NORMALIZED = 520,
        NULLABLE = 521,
        NULLS = 522,
        NUMBER = 523,
        OCCURRENCE = 524,
        OCTETS = 525,
        OPTION = 526,
        OPTIONS = 527,
        ORDERING = 528,
        ORDINALITY = 529,
        OTHERS = 530,
        OUTPUT = 531,
        OVERRIDING = 532,
        OWNER = 533, // Pyrrho
        P = 534,
        PAD = 535,
        PARAMETER_MODE = 536,
        PARAMETER_NAME = 537,
        PARAMETER_ORDINAL_POSITION = 538,
        PARAMETER_SPECIFIC_CATALOG = 539,
        PARAMETER_SPECIFIC_NAME = 540,
        PARAMETER_SPECIFIC_SCHEMA = 541,
        PARTIAL = 542,
        PASCAL = 543,
        PATH = 544,
        PIE = 545, // Pyrrho 4.5
        PLACING = 546,
        PL1 = 547,
        POINTS = 548, // Pyrrho 4.5
        PRECEDING = 549,
        PRESERVE = 550,
        PRIOR = 551,
        PRIVILEGES = 552,
        PROFILING = 553, // Pyrrho
        PROVENANCE = 554, // Pyrrho
        PUBLIC = 555,
        READ = 556,
        REFERRED = 557, // 5.2
        REFERS = 558, // 5.2
        RELATIVE = 559,
        REPEATABLE = 560,
        RESPECT = 561,
        RESTART = 562,
        RESTRICT = 563,
        RETURNED_CARDINALITY = 564,
        RETURNED_LENGTH = 565,
        RETURNED_OCTET_LENGTH = 566,
        RETURNED_SQLSTATE = 567,
        ROLE = 568,
        ROUTINE = 569,
        ROUTINE_CATALOG = 570,
        ROUTINE_NAME = 571,
        ROUTINE_SCHEMA = 572,
        ROW_COUNT = 573,
        SCALE = 574,
        SCHEMA = 575,
        SCHEMA_NAME = 576,
        SCOPE_CATALOG = 577,
        SCOPE_NAME = 578,
        SCOPE_SCHEMA = 579,
        SECTION = 580,
        SECURITY = 581,
        SELF = 582,
        SEQUENCE = 583,
        SERIALIZABLE = 584,
        SERVER_NAME = 585,
        SESSION = 586,
        SETS = 587,
        SIMPLE = 588,
        SIZE = 589,
        SOURCE = 590,
        SPACE = 591,
        SPECIFIC_NAME = 592,
        SQLAGENT = 593, // Pyrrho 7
        STANDALONE = 594, // vol 14
        STATE = 595,
        STATEMENT = 596,
        STRUCTURE = 597,
        STYLE = 598,
        SUBCLASS_ORIGIN = 599,
        T = 600,
        TABLE_NAME = 601,
        TEMPORARY = 602,
        TIES = 603,
        TIMEOUT = 604, // Pyrrho
        TOP_LEVEL_COUNT = 605,
        TRANSACTION = 606,
        TRANSACTION_ACTIVE = 607,
        TRANSACTIONS_COMMITTED = 608,
        TRANSACTIONS_ROLLED_BACK = 609,
        TRANSFORM = 610,
        TRANSFORMS = 611,
        TRIGGER_CATALOG = 612,
        TRIGGER_NAME = 613,
        TRIGGER_SCHEMA = 614, // TYPE  is 267 but is not a reserved word
        TYPE_URI = 615, // Pyrrho
        UNBOUNDED = 616,
        UNCOMMITTED = 617,
        UNDER = 618,
        UNDO = 619,
        UNNAMED = 620,
        URL = 621,  // Pyrrho 7
        USAGE = 622,
        USER_DEFINED_TYPE_CATALOG = 623,
        USER_DEFINED_TYPE_CODE = 624,
        USER_DEFINED_TYPE_NAME = 625,
        USER_DEFINED_TYPE_SCHEMA = 626,
        VIEW = 627,
        WORK = 628,
        WRITE = 629,
        X = 630, // Pyrrho 4.5
        Y = 631, // Pyrrho 4.5
        ZONE = 632
    }
    /// <summary>
    /// These are the underlying (physical) datatypes used  for values in the database
    /// The file format is not machine specific: the engine uses long for Integer where possible, etc
    /// </summary>
    public enum DataType
    {
        Null,
        TimeStamp,  // Integer(UTC ticks)
        Interval,   // Integer[3] (years,months,ticks)
        Integer,    // 1024-bit Integer
        Numeric,    // 1024-bit Integer, precision, scale
        String,     // string: Integer length, length x byte
        Date,       // Integer (UTC ticks)
        TimeSpan,   // Integer (UTC ticks)
        Boolean,    // byte 3 values: T=1,F=0,U=255
        DomainRef,  // typedefpos, Integer els, els x data 
        Blob,       // Integer length, length x byte: Opaque binary type (Clob is String)
        Row,        // spec, Integer cols, cols x data
        Multiset,   // Integer els, els x data
        Array,		// Integer els, els x data
        Password   // A more secure type of string (write-only)
    }
    /// <summary>
    /// These are the supported character repertoires in SQL2011
    /// </summary>
	public enum CharSet
    {
        UCS, SQL_IDENTIFIER, SQL_CHARACTER, GRAPHIC_IRV, // GRAPHIC_IRV is also known as ASCII_GRAPHIC
        LATIN1, ISO8BIT, // ISO8BIT is also known as ASCII_FULL
        SQL_TEXT
    };
    /// <summary>
    /// An Exception class for reporting client errors
    /// </summary>
    internal class DBException : Exception // Client error 
    {
        internal string signal; // Compatible with SQL2011
        internal object[] objects; // additional data for insertion in (possibly localised) message format
        // diagnostic info (there is an active transaction unless we have just done a rollback)
        internal ATree<Sqlx, TypedValue> info = new BTree<Sqlx, TypedValue>(Sqlx.TRANSACTION_ACTIVE, new TInt(1));
        readonly TChar iso = new TChar("ISO 9075");
        readonly TChar pyrrho = new TChar("Pyrrho");
        /// <summary>
        /// Raise an exception to be localised and formatted by the client
        /// </summary>
        /// <param name="sqlstate">The signal</param>
        /// <param name="obs">objects to be included in the message</param>
        public DBException(string sqlstate, params object[] obs)
            : base(sqlstate)
        {
            signal = sqlstate;
            objects = obs;
            if (PyrrhoStart.TutorialMode)
            {
                Console.Write("Exception " + sqlstate);
                foreach (var o in obs)
                    Console.Write("|" + o.ToString());
                Console.WriteLine();
            }
        }
        /// <summary>
        /// Add diagnostic information to the exception
        /// </summary>
        /// <param name="k">diagnostic key as in SQL2011</param>
        /// <param name="v">value of this diagnostic</param>
        /// <returns>this (so we can chain diagnostics)</returns>
        internal DBException Add(Sqlx k, TypedValue v)
        {
            ATree<Sqlx, TypedValue>.Add(ref info, k, v ?? TNull.Value);
            return this;
        }
        internal DBException AddType(ObInfo t)
        {
            Add(Sqlx.TYPE, new TChar(t.ToString()));
            return this;
        }
        internal DBException AddType(Domain t)
        {
            Add(Sqlx.TYPE, new TChar(t.ToString()));
            return this;
        }
        internal DBException AddValue(TypedValue v)
        {
            Add(Sqlx.VALUE, v);
            return this;
        }
        internal DBException AddValue(Domain t)
        {
            Add(Sqlx.VALUE, new TChar(t.ToString()));
            return this;
        }
        /// <summary>
        /// Helper for SQL2011-defined exceptions
        /// </summary>
        /// <returns>this (so we can chain diagnostics)</returns>
        internal DBException ISO()
        {
            Add(Sqlx.CLASS_ORIGIN, iso);
            Add(Sqlx.SUBCLASS_ORIGIN, pyrrho);
            return this;
        }
        /// <summary>
        /// Helper for Pyrrho-defined exceptions
        /// </summary>
        /// <returns>this (so we can chain diagnostics)</returns>
        internal DBException Pyrrho()
        {
            Add(Sqlx.CLASS_ORIGIN, pyrrho);
            Add(Sqlx.SUBCLASS_ORIGIN, pyrrho);
            return this;
        }
        /// <summary>
        /// Helper for Pyrrho-defined exceptions in SQL-2011 class
        /// </summary>
        /// <returns>this (so we can chain diagnostics)</returns>
        internal DBException Mix()
        {
            Add(Sqlx.CLASS_ORIGIN, iso);
            Add(Sqlx.SUBCLASS_ORIGIN, pyrrho);
            return this;
        }
    }

    /// <summary>
    /// Supports the SQL2011 Interval data type. 
    /// Note that Intervals cannot have both year-month and day-second fields.
    /// Shareable
    /// </summary>
	internal class Interval
    {
        internal readonly int years = 0, months = 0;
        internal readonly long ticks = 0;
        internal readonly bool yearmonth = true;
        public Interval(int y, int m, long t = 0) { years = y; months = m; ticks = t; }
        public Interval(long t) { ticks = t; yearmonth = false; }
        public override string ToString()
        {
            if (yearmonth)
                return "" + years + "Y" + months + "M";
            return "" + ticks;
        }
    }
    /// <summary>
    /// Row Version cookie (Sqlx.CHECK). See Laiho/Laux 2010.
    /// Check allows transactions to find out if another transaction has overritten the row.
    /// RVV is calculated only when required: see affected in Context.
    /// Modified in V7 to conform to RFC 7232.
    /// Shareable
    /// </summary>
    internal class Rvv : CTree<long, CTree<long, long>>
    {
        internal const long
            RVV = -193; // Rvv
        internal new static Rvv Empty = new Rvv();
        Rvv() : base()
        { }
        protected Rvv(CTree<long, CTree<long, long>> t) : base(t.root) { }
        public static Rvv operator +(Rvv r, (long, Level4.Cursor) x)
        {
            var (rp, cu) = x;
            return new Rvv(r + (rp, (cu._defpos, cu._ppos)));
        }
        public static Rvv operator +(Rvv r, (long, (long, long)) x)
        {
            var (t, p) = x;
            if (r[t]?.Contains(-1L) == true)
                return r;
            var (d, o) = p;
            var s = ((d == -1L) ? null : r[t]) ?? CTree<long, long>.Empty;
            return new Rvv(r + (t, s + (d, o)));
        }
        public static Rvv operator +(Rvv r, Rvv s)
        {
            if (r == null || r == Empty)
                return s;
            if (s == null || s == Empty)
                return r;
            // Implement a wild-card -1L
            var a = (CTree<long, CTree<long, long>>)r;
            var b = (CTree<long, CTree<long, long>>)s;
            for (var bb = b.First(); bb != null; bb = bb.Next())
            {
                var k = bb.key();
                var bt = bb.value();
            /* If we read the whole table, then any change will be a conflict, 
             * so we record -1, lastData; */ 
                if (bt.Contains(-1L))
                    a += (k, bt);
                else
            /* we override a previous -1,lastData entry with specific information. */
                if (a[k] is CTree<long, long> at)
                {
                    if (at.Contains(-1L))
                        at -= -1L;
                    for (var cb = bt.First(); cb != null; cb = cb.Next())
                    {
                        var bk = cb.key();
                        var bv = cb.value();
                        if (at.Contains(bk))
                            at += (bk, Math.Max(at[bk], bv));
                        else
                            at += (bk, bv);
                    }
                    a += (k, at);
                }
                else
                    a += (k, bt);
            }
            return new Rvv(a);
        }
        /// <summary>
        /// Validate an RVV string
        /// </summary>
        /// <param name="s">the string</param>
        /// <returns>the rvv</returns>
        internal bool Validate(Database db)
        {
            for (var b = First(); b != null; b = b.Next())
            {
                var t = (Table)db.objects[b.key()];
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (c.key() < 0L)
                    {
                        if (t.lastData > c.value())
                            return false;
                    }
                    else if (t.tableRows[c.key()]?.ppos != c.value())
                        return false;
            }
            return true;
        }
        public static Rvv Parse(string s)
        {
            var r = Empty;
            if (s == "*")
                return Empty;
            var ss = s.Trim('"').Split(new string[] { "\",\"" }, StringSplitOptions.None);
            foreach (var t in ss)
            {
                var tt = t.Split(',');
                if (tt.Length > 2)
                    r += (long.Parse(tt[0]), (long.Parse(tt[1]), long.Parse(tt[2])));
            }
            return r;
        }
        /// <summary>
        /// String version of an rvv
        /// </summary>
        /// <returns>the string version</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            var sc = "\"";
            for (var b = First(); b != null; b = b.Next())
            {
                sb.Append(sc); sc = "\",\"";
                for (var c = b.value().First(); c != null; c = c.Next())
                {
                    sb.Append(b.key()); sb.Append(",");
                    sb.Append(c.key()); sb.Append(",");
                    sb.Append(c.value());
                }
                sb.Append("\"");
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// Supports the SQL2003 Date data type
    /// </summary>
    public class Date : IComparable
    {
        public readonly DateTime date;
        internal Date(DateTime d)
        {
            date = d;
        }
        public override string ToString()
        {
            return date.ToString("dd/MM/yyyy");
        }
        #region IComparable Members

        public int CompareTo(object obj)
        {
            if (obj is Date dt)
                obj = dt.date;
            return date.CompareTo(obj);
        }

        #endregion
    }
}
