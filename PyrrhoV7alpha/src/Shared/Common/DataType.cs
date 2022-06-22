using System;
using System.CodeDom;
using System.Runtime.ExceptionServices;
using System.Text;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
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
        LEAD = 162,
        LEADING = 163,
        LEAVE = 164, // vol 4
        LEFT = 165,
        LIKE = 166,
        LN = 167,
        MULTISET = 168, // must be 168
        LOCAL = 169,
        LOCALTIME = 170,
        NCHAR = 171, // must be 171	
        NCLOB = 172, // must be 172
        LOCALTIMESTAMP = 173,
        LOOP = 174,  // vol 4
        LOWER = 175,
        MATCH = 176,
        NULL = 177, // must be 177
        MAX = 178,
        NUMERIC = 179, // must be 179
        MEMBER = 180,
        MERGE = 181,
        METHOD = 182,
        MIN = 183,
        MINUTE = 184,
        MOD = 185,
        MODIFIES = 186,
        MODULE = 187,
        MONTH = 188,	 // MULTISET is 168
        NATIONAL = 189,
        NATURAL = 190, // NCHAR 171 NCLOB 172
        NEW = 191,
        NO = 192,
        NONE = 193,
        NORMALIZE = 194,
        NOT = 195,
        NULLIF = 196, // NUMERIC 179, see also NUMERICLITERAL
        OBJECT = 197,
        OCCURRENCES_REGEX = 198, // alphabetical sequence is a misprint in SQL2008
        REAL0 = 199, // must be 199, previous version of REAL
        OCTET_LENGTH = 200,
        OF = 201,
        OFFSET = 202,
        REAL = 203, // must be 203, see also REAL0 and REALLITERAL
        OLD = 204,
        ON = 205,
        ONLY = 206,
        OPEN = 207,
        OR = 208,
        ORDER = 209,
        OUT = 210,
        OUTER = 211,
        OVER = 212,
        OVERLAPS = 213,
        OVERLAY = 214,
        PARAMETER = 215,
        PARTITION = 216,
        PERCENT = 217,
        PASSWORD = 218, // must be 218, Pyrrho v5
        PERIOD = 219,
        PORTION = 220,
        POSITION = 221,
        POWER = 222,
        PRECEDES = 223,
        PRECISION = 224,
        PREPARE = 225,
        PRIMARY = 226,
        PROCEDURE = 227,
        RANGE = 228,
        RANK = 229,
        READS = 230,    // REAL 203 (previously 199 see REAL0)
        RECURSIVE = 231,
        REF = 232,
        REFERENCES = 233,
        REFERENCING = 234,
        RELEASE = 235,
        REPEAT = 236, // vol 4
        RESIGNAL = 237, // vol 4
        RESULT = 238,
        RETURN = 239,
        RETURNS = 240,
        REVOKE = 241,
        RIGHT = 242,
        ROLLBACK = 243,
        ROW = 244,
        ROW_NUMBER = 245,
        ROWS = 246,
        SAVEPOINT = 247,
        SCOPE = 248,
        SCROLL = 249,
        SEARCH = 250,
        SECOND = 251,
        SELECT = 252,
        SENSITIVE = 253,    
        SESSION_USER = 254,
        SET = 255,
        SIGNAL = 256, //vol 4
        TIME = 257, // must be 257
        TIMESTAMP = 258, // must be 258
        SMALLINT = 259,
        SOME = 260,
        SPECIFIC = 261,
        SPECIFICTYPE = 262,
        SQL = 263,
        SQLEXCEPTION = 264,
        SQLSTATE = 265,
        SQLWARNING = 266,
        TYPE = 267, // must be 267 but is not a reserved word
        SQRT = 268,
        START = 269,
        STATIC = 270,
        STDDEV_POP = 271,
        STDDEV_SAMP = 272,
        SUBMULTISET = 273,
        SUBSTRING = 274, //
        SUBSTRING_REGEX = 275,
        SUCCEEDS = 276,
        SUM = 277, //
        SYMMETRIC = 278,
        SYSTEM = 279,
        SYSTEM_TIME = 280,
        SYSTEM_USER = 281,  // TABLE is 297
        TABLESAMPLE = 282,
        THEN = 283,  // TIME is 257, TIMESTAMP is 258	
        TIMEZONE_HOUR = 284,
        TIMEZONE_MINUTE = 285,
        TO = 286,
        TRAILING = 287,
        TRANSLATE = 288,
        TRANSLATION = 289,
        TREAT = 290,
        TRIGGER = 291,
        TRIM = 292,
        TRIM_ARRAY = 293,
        TRUE = 294,
        TRUNCATE = 295,
        UESCAPE = 296,
        TABLE = 297, // must be 297
        UNION = 298,
        UNIQUE = 299,
        UNKNOWN = 300,
        UNNEST = 301,
        UNTIL = 302, // vol 4
        UPDATE = 303,
        UPPER = 304, //
        USER = 305,
        USING = 306,
        VALUE = 307,
        VALUE_OF = 308,
        VALUES = 309,
        VARBINARY = 310,
        VARCHAR = 311,
        VARYING = 312,
        VERSIONING = 313,
        WHEN = 314,
        WHENEVER = 315,
        WHERE = 316,
        WHILE = 317, // vol 4
        WINDOW = 318,
        WITH = 319,
        WITHIN = 320,
        WITHOUT = 321, // XML is 356 vol 14
        XMLAGG = 322, // Basic XML stuff + XMLAgg vol 14
        XMLATTRIBUTES = 323,
        XMLBINARY = 324,
        XMLCAST = 325,
        XMLCOMMENT = 326,
        XMLCONCAT = 327,
        XMLDOCUMENT = 328,
        XMLELEMENT = 329,
        XMLEXISTS = 330,
        XMLFOREST = 331,
        XMLITERATE = 332,
        XMLNAMESPACES = 333,
        XMLPARSE = 334,
        XMLPI = 335,
        XMLQUERY = 336,
        XMLSERIALIZE = 337,
        XMLTABLE = 338,
        XMLTEXT = 339,
        XMLVALIDATE = 340,
        YEAR = 341,	// last reserved word
        //====================TOKEN TYPES=====================
        ASSIGNMENT = 342, // := 
        BLOBLITERAL = 343, // 
        BOOLEANLITERAL = 344,
        CHARLITERAL = 345, //
        COLON = 346, // :
        COMMA = 347,  // ,        
        CONCATENATE = 348, // ||        
        DIVIDE = 349, // /        
        DOCUMENTLITERAL = 350, // v5.1
        DOT = 351, // . 5.2 was STOP
        DOUBLECOLON = 352, // ::        
        EMPTY = 353, // []        
        EQL = 354,  // =        
        GEQ = 355, // >=    
        XML = 356, // must be 356 (is a reserved word)
        GTR = 357, // >    
        ID = 358, // identifier
        INTEGERLITERAL = 359, // Pyrrho
        LBRACE = 360, // {
        LBRACK = 361, // [
        LEQ = 362, // <=
        LPAREN = 363, // (
        LSS = 364, // <
        MEXCEPT = 365, // 
        MINTERSECT = 366, //
        MINUS = 367, // -
        MUNION = 368, // 
        NEQ = 369, // <>
        NUMERICLITERAL = 370, // 
        PLUS = 371, // + 
        QMARK = 372, // ?
        RBRACE = 373, // } 
        RBRACK = 374, // ] 
        RDFDATETIME = 375, //
        RDFLITERAL = 376, // 
        RDFTYPE = 377, // Pyrrho 7.0
        REALLITERAL = 378, //
        RPAREN = 379, // ) 
        SEMICOLON = 380, // ; 
        TIMES = 381, // *
        VBAR = 382, // | 
        //=========================NON-RESERVED WORDS================
        A = 383, // first non-reserved word
        ABSOLUTE = 384,
        ACTION = 385,
        ADA = 386,
        ADD = 387,
        ADMIN = 388,
        AFTER = 389,
        ALWAYS = 390,
        APPLICATION = 391, // Pyrrho 4.6
        ASC = 392,
        ASSERTION = 393,
        ATTRIBUTE = 394,
        ATTRIBUTES = 395,
        BEFORE = 396,
        BERNOULLI = 397,
        BREADTH = 398,
        BREAK = 399, // Pyrrho
        C = 400,
        CAPTION = 401, // Pyrrho 4.5
        CASCADE = 402,
        CATALOG = 403,
        CATALOG_NAME = 404,
        CHAIN = 405,
        CHARACTER_SET_CATALOG = 406,
        CHARACTER_SET_NAME = 407,
        CHARACTER_SET_SCHEMA = 408,
        CHARACTERISTICS = 409,
        CHARACTERS = 410,
        CLASS_ORIGIN = 411,
        COBOL = 412,
        COLLATION = 413,
        COLLATION_CATALOG = 414,
        COLLATION_NAME = 415,
        COLLATION_SCHEMA = 416,
        COLUMN_NAME = 417,
        COMMAND_FUNCTION = 418,
        COMMAND_FUNCTION_CODE = 419,
        COMMITTED = 420,
        CONDITION_NUMBER = 421,
        CONNECTION = 422,
        CONNECTION_NAME = 423,
        CONSTRAINT_CATALOG = 424,
        CONSTRAINT_NAME = 425,
        CONSTRAINT_SCHEMA = 426,
        CONSTRAINTS = 427,
        CONSTRUCTOR = 428,
        CONTENT = 429,
        CONTINUE = 430,
        CSV = 431, // Pyrrho 5.5
        CURATED = 432, // Pyrrho
        CURSOR_NAME = 433,
        DATA = 434,
        DATABASE = 435, // Pyrrho
        DATETIME_INTERVAL_CODE = 436,
        DATETIME_INTERVAL_PRECISION = 437,
        DEFAULTS = 438,
        DEFERRABLE = 439,
        DEFERRED = 440,
        DEFINED = 441,
        DEFINER = 442,
        DEGREE = 443,
        DEPTH = 444,
        DERIVED = 445,
        DESC = 446,
        DESCRIPTOR = 447,
        DIAGNOSTICS = 448,
        DISPATCH = 449,
        DOMAIN = 450,
        DYNAMIC_FUNCTION = 451,
        DYNAMIC_FUNCTION_CODE = 452,
        ENFORCED = 453,
        ENTITY = 454, // Pyrrho 4.5
        ETAG = 455, // Pyrrho Metadata 7.0
        EXCLUDE = 456,
        EXCLUDING = 457,
        FINAL = 458,
        FIRST = 459,
        FLAG = 460,
        FOLLOWING = 461,
        FORTRAN = 462,
        FOUND = 463,
        G = 464,
        GENERAL = 465,
        GENERATED = 466,
        GO = 467,
        GOTO = 468,
        GRANTED = 469,
        HIERARCHY = 470,
        HISTOGRAM = 471, // Pyrrho 4.5
        HTTPDATE = 472, // Pyrrho 7 RFC 7231
        IGNORE = 473,
        IMMEDIATE = 474,
        IMMEDIATELY = 475,
        IMPLEMENTATION = 476,
        INCLUDING = 477,
        INCREMENT = 478,
        INITIALLY = 479,
        INPUT = 480,
        INSTANCE = 481,
        INSTANTIABLE = 482,
        INSTEAD = 483,
        INVERTS = 484, // Pyrrho Metadata 5.7
        INVOKER = 485,
        IRI = 486, // Pyrrho 7
        ISOLATION = 487,
        JSON = 488, // Pyrrho 5.5
        K = 489,
        KEY = 490,
        KEY_MEMBER = 491,
        KEY_TYPE = 492,
        LAST = 493,
        LEGEND = 494, // Pyrrho Metadata 4.8
        LENGTH = 495,
        LEVEL = 496,
        LINE = 497, // Pyrrho 4.5
        LOCATOR = 498,
        M = 499,
        MAP = 500,
        MATCHED = 501,
        MAXVALUE = 502,
        MESSAGE_LENGTH = 503,
        MESSAGE_OCTET_LENGTH = 504,
        MESSAGE_TEXT = 505,
        METADATA = 506, // Pyrrho 7
        MILLI = 507, // Pyrrho 7
        MIME = 508, // Pyrrho 7
        MINVALUE = 509,
        MONOTONIC = 510, // Pyrrho 5.7
        MORE = 511,
        MUMPS = 512,
        NAME = 513,
        NAMES = 514,
        NESTING = 515,
        NEXT = 516,
        NFC = 516,
        NFD = 518,
        NFKC = 519,
        NFKD = 520,
        NORMALIZED = 521,
        NULLABLE = 522,
        NULLS = 523,
        NUMBER = 524,
        OCCURRENCE = 525,
        OCTETS = 526,
        OPTION = 527,
        OPTIONS = 528,
        ORDERING = 529,
        ORDINALITY = 530,
        OTHERS = 531,
        OUTPUT = 532,
        OVERRIDING = 533,
        OWNER = 534, // Pyrrho
        P = 535,
        PAD = 536,
        PARAMETER_MODE = 537,
        PARAMETER_NAME = 538,
        PARAMETER_ORDINAL_POSITION = 539,
        PARAMETER_SPECIFIC_CATALOG = 540,
        PARAMETER_SPECIFIC_NAME = 541,
        PARAMETER_SPECIFIC_SCHEMA = 542,
        PARTIAL = 543,
        PASCAL = 544,
        PATH = 545,
        PIE = 546, // Pyrrho 4.5
        PLACING = 547,
        PL1 = 548,
        POINTS = 549, // Pyrrho 4.5
        PRECEDING = 550,
        PRESERVE = 551,
        PRIOR = 552,
        PRIVILEGES = 553,
        PROFILING = 554, // Pyrrho
        PROVENANCE = 555, // Pyrrho
        PUBLIC = 556,
        READ = 557,
        REFERRED = 558, // 5.2
        REFERS = 559, // 5.2
        RELATIVE = 560,
        REPEATABLE = 561,
        RESPECT = 562,
        RESTART = 563,
        RESTRICT = 564,
        RETURNED_CARDINALITY = 565,
        RETURNED_LENGTH = 566,
        RETURNED_OCTET_LENGTH = 567,
        RETURNED_SQLSTATE = 568,
        ROLE = 569,
        ROUTINE = 570,
        ROUTINE_CATALOG = 571,
        ROUTINE_NAME = 572,
        ROUTINE_SCHEMA = 573,
        ROW_COUNT = 574,
        SCALE = 575,
        SCHEMA = 576,
        SCHEMA_NAME = 577,
        SCOPE_CATALOG = 578,
        SCOPE_NAME = 579,
        SCOPE_SCHEMA = 580,
        SECTION = 581,
        SECURITY = 582,
        SELF = 583,
        SEQUENCE = 584,
        SERIALIZABLE = 585,
        SERVER_NAME = 586,
        SESSION = 587,
        SETS = 588,
        SIMPLE = 589,
        SIZE = 590,
        SOURCE = 591,
        SPACE = 592,
        SPECIFIC_NAME = 593,
        SQLAGENT = 594, // Pyrrho 7
        STANDALONE = 595, // vol 14
        STATE = 596,
        STATEMENT = 597,
        STRUCTURE = 598,
        STYLE = 599,
        SUBCLASS_ORIGIN = 600,
        T = 601,
        TABLE_NAME = 602,
        TEMPORARY = 603,
        TIES = 604,
        TIMEOUT = 605, // Pyrrho
        TOP_LEVEL_COUNT = 606,
        TRANSACTION = 607,
        TRANSACTION_ACTIVE = 608,
        TRANSACTIONS_COMMITTED = 609,
        TRANSACTIONS_ROLLED_BACK = 610,
        TRANSFORM = 611,
        TRANSFORMS = 612,
        TRIGGER_CATALOG = 613,
        TRIGGER_NAME = 614,
        TRIGGER_SCHEMA = 615, // TYPE  is 267 but is not a reserved word
        TYPE_URI = 616, // Pyrrho
        UNBOUNDED = 617,
        UNCOMMITTED = 618,
        UNDER = 619,
        UNDO = 620,
        UNNAMED = 621,
        URL = 622,  // Pyrrho 7
        USAGE = 623,
        USER_DEFINED_TYPE_CATALOG = 624,
        USER_DEFINED_TYPE_CODE = 625,
        USER_DEFINED_TYPE_NAME = 626,
        USER_DEFINED_TYPE_SCHEMA = 627,
        VIEW = 628,
        WORK = 629,
        WRITE = 630,
        X = 631, // Pyrrho 4.5
        Y = 632, // Pyrrho 4.5
        ZONE = 633
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
        DomainRef,  // typedefpos, Integer els, els x obs 
        Blob,       // Integer length, length x byte: Opaque binary type (Clob is String)
        Row,        // spec, Integer cols, cols x obs
        Multiset,   // Integer els, els x obs
        Array,		// Integer els, els x obs
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
        internal object[] objects; // additional obs for insertion in (possibly localised) message format
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
                    Console.Write("|" + (o?.ToString()??"$Null"));
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
    /// Supports the SQL2011 Interval obs type. 
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
    /// Row Version cookie (Sqlx.VERSIONING). See Laiho/Laux 2010.
    /// Check allows transactions to find out if another transaction has overritten the row.
    /// RVV is calculated only when required: see affected in Context.
    /// Modified in V7 to conform to RFC 7232.
    /// Rvv format
    /// tabledefpos-> (defpos,ppos)  Record
    ///               (defpos,-1)    Delete
    ///               (-1, lastData) Table
    /// Shareable
    /// </summary>
    internal class Rvv : CTree<long, CTree<long, long>>
    {
        internal const long
            RVV = -193; // Rvv
        internal new static Rvv Empty = new Rvv();
        internal long version => First()?.value()?.Last()?.value() ?? 0L;
        Rvv() : base()
        { }
        protected Rvv(CTree<long, CTree<long, long>> t) : base(t.root) { }
        public static Rvv operator +(Rvv r, (long, Level4.Cursor) x)
        {
            var (rp, cu) = x;
            if (cu == null)
                return r;
            return new Rvv(r + (rp, cu._ds[rp]));
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
        internal static bool Validate(Database db,string es,string eu)
        {
            var e = Parse(es);
            for (var b = e?.First();b!=null;b=b.Next())
            {
                var tk = b.key();
                var tb = (Table)db.objects[tk];
                if (tb == null)
                    return false;
                for (var c=b.value().First();c!=null;c=c.Next())
                {
                    var dp = c.key();
                    if (dp==-1L)
                    {
                        if (tb.lastChange > c.value())
                            return false;
                        continue;
                    }
                    var tr = tb.tableRows[dp];
                    if (tr == null)
                        return false;
                    if (tr.ppos > c.value())
                        return false;
                }
            }
            if (eu!=null)
            {
                var ck = THttpDate.Parse(eu);
                if (ck.value != null && db.lastModified > ck.value.Value)
                    return false;
            }
            return true;
        }
        /// <summary>
        /// This is from the context, st is the time from If-Unmodified-Since
        /// </summary>
        /// <param name="db"></param>
        /// <param name="st"></param>
        /// <returns></returns>
        internal bool Validate(Database db, THttpDate st)
        {
            var eps = (st==null)?0: st.milli ? 10000000 : 10000;
            var tt = (st==null)? 0: st.value.Value.Ticks - eps;
            for (var b = First(); b != null; b = b.Next())
            {
                var t = (Table)db.objects[b.key()];
                for (var c = b.value().First(); c != null; c = c.Next())
                {
                    if (c.key() < 0)
                    {
                        if (t.lastData > c.value())
                            return false;
                    }
                    else
                    {
                        var tr = t.tableRows[c.key()];
                        if (tr == null)
                        {
                            if (c.value() > 0)
                                return false;
                        }
                        else if (tr.ppos > c.value())
                            return false;
                    }
                }
            }
            return true;
        }
        public static Rvv Parse(string s)
        {
            if (s == null)
                return null;
            var r = Empty;
            if (s == "*")
                return Empty;
            var ss = s.Trim('"').Split(new string[] { "\",\"" }, StringSplitOptions.None);
            foreach (var t in ss)
            {
                var tt = t.Split(',');
                if (tt.Length > 2)
                    r += (UidParse(tt[0]), (UidParse(tt[1]), UidParse(tt[2])));
            }
            return r;
        }
        static long UidParse(string u)
        {
            if (u.Length == 0)
                return -1L; // should not occur
            if (char.IsDigit(u[0]))
                return long.Parse(u);
            if (u.Length == 1) // happens with _
                return -1L;
            var v = long.Parse(u.Substring(1));
            switch(u[0])
            {
                case '%': return v + Transaction.HeapStart;
                case '`': return v + Transaction.Executables;
                case '#': return v + Transaction.Analysing;
                case '!': return v + Transaction.TransPos;
            }
            return -1L; // should not occur
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
                sb.Append(DBObject.Uid(b.key())); 
                for (var c = b.value().First(); c != null; c = c.Next())
                {
                    sb.Append(",");
                    sb.Append(DBObject.Uid(c.key())); sb.Append(",");
                    sb.Append(DBObject.Uid(c.value()));
                }
                sb.Append("\"");
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// Supports the SQL2003 Date obs type
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
