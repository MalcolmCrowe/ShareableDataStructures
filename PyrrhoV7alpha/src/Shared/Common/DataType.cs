using System;
using System.CodeDom;
using System.Runtime.ExceptionServices;
using System.Text;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
        FLOAT = 118,
        FLOOR = 119,
        FOR = 120,
        FOREIGN = 121,
        FREE = 122,
        FROM = 123,
        FULL = 124,
        FUNCTION = 125,
        FUSION = 126,
        GET = 127,
        GLOBAL = 128,
        GRANT = 129,
        GROUP = 130,
        GROUPING = 131,
        GROUPS = 132,
        HANDLER = 133, // vol 4 
        INT = 134,  // deprecated: see also INTEGERLITERAL
        INTEGER = 135, // must be 135
        HAVING = 136,
        INTERVAL0 = 137,  // must be 137 (old version of INTERVAL)
        HOLD = 138,
        HOUR = 139,
        IDENTITY = 140,
        IF = 141,  // vol 4
        IN = 142,
        INDICATOR = 143,
        INNER = 144,
        INOUT = 145,
        INSENSITIVE = 146,
        INSERT = 147, // INT is 136, INTEGER is 135
        INTERSECT = 148,
        INTERSECTION = 149,
        INTO = 150,
        IS = 151,
        INTERVAL = 152, // must be 152 see also INTERVAL0
        ITERATE = 153, // vol 4
        JOIN = 154,
        LAG = 155,
        LANGUAGE = 156,
        LARGE = 157,
        LAST_VALUE = 158,
        LATERAL = 159,
        LEADING = 160,
        LEAVE = 161, // vol 4
        LEFT = 162,
        LIKE = 163,
        LN = 164,
        LOCAL = 165,
        LOCALTIME = 166,
        LOCALTIMESTAMP = 167,
        MULTISET = 168, // must be 168
        LOOP = 169,  // vol 4
        LOWER = 170,
        NCHAR = 171, // must be 171	
        NCLOB = 172, // must be 172
        MATCH = 173,
        MAX = 174, 
        MEMBER = 175,
        MERGE = 176,
        NULL = 177, // must be 177
        METHOD = 178,
        NUMERIC = 179, // must be 179
        MIN = 180, 
        MINUTE = 181,
        MOD = 182,
        MODIFIES = 183,
        MODULE = 184,
        MONTH = 185,	 // MULTISET is 168
        NATIONAL = 186,
        NATURAL = 187, // NCHAR 171 NCLOB 172
        NEW = 188,
        NO = 189,
        NONE = 190,
        NORMALIZE = 191,
        NOT = 192,
        NULLIF = 193, // NUMERIC 179, see also NUMERICLITERAL
        OBJECT = 194,
        OCCURRENCES_REGEX = 195, // alphabetical sequence is a misprint in SQL2008
        OCTET_LENGTH = 196,
        OF = 197,
        OFFSET = 198,
        REAL0 = 199, // must be 199, previous version of REAL
        OLD = 200,
        ON = 201,
        ONLY = 202,
        REAL = 203, // must be 203, see also REAL0 and REALLITERAL
        OPEN = 204,
        OR = 205,
        ORDER = 206,
        OUT = 207,
        OUTER = 208,
        OVER = 209,
        OVERLAPS = 210,
        OVERLAY = 211,
        PARAMETER = 212,
        PARTITION = 213,
        PERCENT = 214,
        PERIOD = 215,
        PORTION = 216,
        POSITION = 217,
        PASSWORD = 218, // must be 218, Pyrrho v5
        POWER = 219,
        PRECEDES = 220,
        PRECISION = 221,
        PREPARE = 222,
        PRIMARY = 223,
        PROCEDURE = 224,
        RANGE = 225,
        RANK = 226,
        READS = 227,    // REAL 203 (previously 199 see REAL0)
        RECURSIVE = 228,
        REF = 229,
        REFERENCES = 230,
        REFERENCING = 231,
        RELEASE = 232,
        REPEAT = 233, // vol 4
        RESIGNAL = 234, // vol 4
        RESULT = 235,
        RETURN = 236,
        RETURNS = 237,
        REVOKE = 238,
        RIGHT = 239,
        ROLLBACK = 240,
        ROW = 241,
        ROW_NUMBER = 242,
        ROWS = 243,
        SAVEPOINT = 244,
        SCOPE = 245,
        SCROLL = 246,
        SEARCH = 247,
        SECOND = 248,
        SELECT = 249,
        SENSITIVE = 250,    // has a different usage in Pyrrho
        SESSION_USER = 251,
        SET = 252,
        SIGNAL = 253, //vol 4
        SMALLINT = 254,
        SOME = 255,
        SPECIFIC = 256,
        TIME = 257, // must be 257
        TIMESTAMP = 258, // must be 258
        SPECIFICTYPE = 259,
        SQL = 260,
        SQLEXCEPTION = 261,
        SQLSTATE = 262,
        SQLWARNING = 263,
        SQRT = 264,
        START = 265,
        STATIC = 266,
        TYPE = 267, // must be 267 but is not a reserved word
        STDDEV_POP = 268,
        STDDEV_SAMP = 269,
        SUBMULTISET = 270,
        SUBSTRING = 271, //
        SUBSTRING_REGEX = 272,
        SUCCEEDS = 273,
        SUM = 274, //
        SYMMETRIC = 275,
        SYSTEM = 276,
        SYSTEM_TIME = 277,
        SYSTEM_USER = 278,  // TABLE is 297
        TABLESAMPLE = 279,
        THEN = 280,  // TIME is 257, TIMESTAMP is 258	
        TIMEZONE_HOUR = 281,
        TIMEZONE_MINUTE = 282,
        TO = 283,
        TRAILING = 284,
        TRANSLATE = 285,
        TRANSLATION = 286,
        TREAT = 287,
        TRIGGER = 288,
        TRIM = 289,
        TRIM_ARRAY = 290,
        TRUE = 291,
        TRUNCATE = 292,
        UESCAPE = 293,
        UNION = 294,
        UNIQUE = 295,
        UNKNOWN = 296,
        TABLE = 297, // must be 297
        UNNEST = 298,
        UNTIL = 299, // vol 4
        UPDATE = 300,
        UPPER = 301, //
        USER = 302,
        USING = 303,
        VALUE = 304,
        VALUE_OF = 305,
        VALUES = 306,
        VARBINARY = 307,
        VARCHAR = 308,
        VARYING = 309,
        VERSIONING = 310,
        WHEN = 311,
        WHENEVER = 312,
        WHERE = 313,
        WHILE = 314, // vol 4
        WINDOW = 315,
        WITH = 316,
        WITHIN = 317,
        WITHOUT = 318, // XML is 356 vol 14
        XMLAGG = 319, // Basic XML stuff + XMLAgg vol 14
        XMLATTRIBUTES = 320,
        XMLBINARY = 321,
        XMLCAST = 322,
        XMLCOMMENT = 323,
        XMLCONCAT = 324,
        XMLDOCUMENT = 325,
        XMLELEMENT = 326,
        XMLEXISTS = 327,
        XMLFOREST = 328,
        XMLITERATE = 329,
        XMLNAMESPACES = 330,
        XMLPARSE = 331,
        XMLPI = 332,
        XMLQUERY = 333,
        XMLSERIALIZE = 334,
        XMLTABLE = 335,
        XMLTEXT = 336,
        XMLVALIDATE = 337,
        YEAR = 338,	// last reserved word
        //====================TOKEN TYPES=====================
        ASSIGNMENT = 339, // := 
        BLOBLITERAL = 340, // 
        BOOLEANLITERAL = 341,
        CHARLITERAL = 342, //
        COLON = 343, // :
        COMMA = 344,  // ,        
        CONCATENATE = 345, // ||        
        DIVIDE = 346, // /        
        DOCUMENTLITERAL = 347, // v5.1
        DOT = 348, // . 5.2 was STOP
        DOUBLECOLON = 349, // ::        
        EMPTY = 350, // []        
        EQL = 351,  // =        
        GEQ = 352, // >=    
        GTR = 353, // >    
        ID = 354, // identifier
        INTEGERLITERAL = 355, // Pyrrho
        XML = 356, // must be 356 (is a reserved word)
        LBRACE = 357, // {
        LBRACK = 358, // [
        LEQ = 359, // <=
        LPAREN = 360, // (
        LSS = 361, // <
        MEXCEPT = 362, // 
        MINTERSECT = 363, //
        MINUS = 364, // -
        MUNION = 365, // 
        NEQ = 366, // <>
        NUMERICLITERAL = 367, // 
        PLUS = 368, // + 
        QMARK = 369, // ?
        RBRACE = 370, // } 
        RBRACK = 371, // ] 
        RDFDATETIME = 372, //
        RDFLITERAL = 373, // 
        RDFTYPE = 374, // Pyrrho 7.0
        REALLITERAL = 375, //
        RPAREN = 376, // ) 
        SEMICOLON = 377, // ; 
        TIMES = 378, // *
        VBAR = 379, // | 
        //=========================NON-RESERVED WORDS================
        A = 380, // first non-reserved word
        ABSOLUTE = 381,
        ACTION = 382,
        ADA = 383,
        ADD = 384,
        ADMIN = 385,
        AFTER = 386,
        ALWAYS = 387,
        APPLICATION = 388, // Pyrrho 4.6
        ASC = 389,
        ASSERTION = 390,
        ATTRIBUTE = 391,
        ATTRIBUTES = 392,
        BEFORE = 393,
        BERNOULLI = 394,
        BREADTH = 395,
        BREAK = 396, // Pyrrho
        C = 397,
        CAPTION = 398, // Pyrrho 4.5
        CASCADE = 399,
        CATALOG = 400,
        CATALOG_NAME = 401,
        CHAIN = 402,
        CHARACTER_SET_CATALOG = 403,
        CHARACTER_SET_NAME = 404,
        CHARACTER_SET_SCHEMA = 405,
        CHARACTERISTICS = 406,
        CHARACTERS = 407,
        CLASS_ORIGIN = 408,
        COBOL = 409,
        COLLATION = 410,
        COLLATION_CATALOG = 411,
        COLLATION_NAME = 412,
        COLLATION_SCHEMA = 413,
        COLUMN_NAME = 414,
        COMMAND_FUNCTION = 415,
        COMMAND_FUNCTION_CODE = 416,
        COMMITTED = 417,
        CONDITION_NUMBER = 418,
        CONNECTION = 419,
        CONNECTION_NAME = 420,
        CONSTRAINT_CATALOG = 421,
        CONSTRAINT_NAME = 422,
        CONSTRAINT_SCHEMA = 423,
        CONSTRAINTS = 424,
        CONSTRUCTOR = 425,
        CONTENT = 426,
        CONTINUE = 427,
        CSV = 428, // Pyrrho 5.5
        CURATED = 429, // Pyrrho
        CURSOR_NAME = 430,
        DATA = 431,
        DATABASE = 432, // Pyrrho
        DATETIME_INTERVAL_CODE = 433,
        DATETIME_INTERVAL_PRECISION = 434,
        DEFAULTS = 435,
        DEFERRABLE = 436,
        DEFERRED = 437,
        DEFINED = 438,
        DEFINER = 439,
        DEGREE = 440,
        DEPTH = 441,
        DERIVED = 442,
        DESC = 443,
        DESCRIPTOR = 444,
        DIAGNOSTICS = 445,
        DISPATCH = 446,
        DOMAIN = 447,
        DYNAMIC_FUNCTION = 448,
        DYNAMIC_FUNCTION_CODE = 449,
        ENFORCED = 450,
        ENTITY = 451, // Pyrrho 4.5
        EXCLUDE = 452,
        EXCLUDING = 453,
        FINAL = 454,
        FIRST = 455,
        FLAG = 456, // unused (SIMILAR)
        FOLLOWING = 457,
        FORTRAN = 458,
        FOUND = 459,
        G = 460,
        GENERAL = 461,
        GENERATED = 462,
        GO = 463,
        GOTO = 464,
        GRANTED = 465,
        HIERARCHY = 466,
        HISTOGRAM = 467, // Pyrrho 4.5
        IGNORE = 468,
        IMMEDIATE = 469,
        IMMEDIATELY = 470,
        IMPLEMENTATION = 471,
        INCLUDING = 472,
        INCREMENT = 473,
        INITIALLY = 474,
        INPUT = 475,
        INSTANCE = 476,
        INSTANTIABLE = 477,
        INSTEAD = 478,
        INVERTS = 479, // Pyrrho Metadata 5.7
        INVOKER = 480,
        IRI = 481, // Pyrrho 7
        ISOLATION = 482,
        JSON = 483, // Pyrrho 5.5
        K = 484,
        KEY = 485,
        KEY_MEMBER = 486,
        KEY_TYPE = 487,
        LAST = 488,
        LEGEND = 489, // Pyrrho Metadata 4.8
        LENGTH = 490,
        LEVEL = 491,
        LINE = 492, // Pyrrho 4.5
        LOCATOR = 493,
        M = 494,
        MAP = 495,
        MATCHED = 496,
        MAXVALUE = 497,
        MESSAGE_LENGTH = 498,
        MESSAGE_OCTET_LENGTH = 499,
        MESSAGE_TEXT = 500,
        MIME = 501, // Pyrrho 7
        MINVALUE = 502,
        MONOTONIC = 503, // Pyrrho 5.7
        MORE = 504,
        MUMPS = 505,
        NAME = 506,
        NAMES = 507,
        NESTING = 508,
        NEXT = 509,
        NFC = 510,
        NFD = 511,
        NFKC = 512,
        NFKD = 513,
        NORMALIZED = 514,
        NULLABLE = 515,
        NULLS = 516,
        NUMBER = 517,
        OCCURRENCE = 518,
        OCTETS = 519,
        OPTION = 520,
        OPTIONS = 521,
        ORDERING = 522,
        ORDINALITY = 523,
        OTHERS = 524,
        OUTPUT = 525,
        OVERRIDING = 526,
        OWNER = 527, // Pyrrho
        P = 528,
        PAD = 529,
        PARAMETER_MODE = 530,
        PARAMETER_NAME = 531,
        PARAMETER_ORDINAL_POSITION = 532,
        PARAMETER_SPECIFIC_CATALOG = 533,
        PARAMETER_SPECIFIC_NAME = 534,
        PARAMETER_SPECIFIC_SCHEMA = 535,
        PARTIAL = 536,
        PASCAL = 537,
        PATH = 538,
        PIE = 539, // Pyrrho 4.5
        PLACING = 540,
        PL1 = 541,
        POINTS = 542, // Pyrrho 4.5
        PRECEDING = 543,
        PRESERVE = 544,
        PRIOR = 545,
        PRIVILEGES = 546,
        PROFILING = 547, // Pyrrho
        PROVENANCE = 548, // Pyrrho
        PUBLIC = 549,
        READ = 550,
        REFERRED = 551, // 5.2
        REFERS = 552, // 5.2
        RELATIVE = 553,
        REPEATABLE = 554,
        RESPECT = 555,
        RESTART = 556,
        RESTRICT = 557,
        RETURNED_CARDINALITY = 558,
        RETURNED_LENGTH = 559,
        RETURNED_OCTET_LENGTH = 560,
        RETURNED_SQLSTATE = 561,
        ROLE = 562,
        ROUTINE = 563,
        ROUTINE_CATALOG = 564,
        ROUTINE_NAME = 565,
        ROUTINE_SCHEMA = 566,
        ROW_COUNT = 567,
        SCALE = 568,
        SCHEMA = 569,
        SCHEMA_NAME = 570,
        SCOPE_CATALOG = 571,
        SCOPE_NAME = 572,
        SCOPE_SCHEMA = 573,
        SECTION = 574,
        SECURITY = 575,
        SELF = 576,
        SEQUENCE = 577,
        SERIALIZABLE = 578,
        SERVER_NAME = 579,
        SESSION = 580,
        SETS = 581,
        SIMPLE = 582,
        SIZE = 583,
        SOURCE = 584,
        SPACE = 585,
        SPECIFIC_NAME = 586,
        SQLAGENT = 587, // Pyrrho 7
        STANDALONE = 588, // vol 14
        STATE = 589,
        STATEMENT = 590,
        STRUCTURE = 591,
        STYLE = 592,
        SUBCLASS_ORIGIN = 593,
        T = 594,
        TABLE_NAME = 595,
        TEMPORARY = 596,
        TIES = 597,
        TIMEOUT = 598, // Pyrrho
        TOP_LEVEL_COUNT = 599,
        TRANSACTION = 600,
        TRANSACTION_ACTIVE = 601,
        TRANSACTIONS_COMMITTED = 602,
        TRANSACTIONS_ROLLED_BACK = 603,
        TRANSFORM = 604,
        TRANSFORMS = 605,
        TRIGGER_CATALOG = 606,
        TRIGGER_NAME = 607,
        TRIGGER_SCHEMA = 608, // TYPE  is 267 but is not a reserved word
        TYPE_URI = 609, // Pyrrho
        UNBOUNDED = 610,
        UNCOMMITTED = 611,
        UNDER = 612,
        UNDO = 613,
        UNNAMED = 614,
        URL = 615,  // Pyrrho 7
        USAGE = 616,
        USER_DEFINED_TYPE_CATALOG = 617,
        USER_DEFINED_TYPE_CODE = 618,
        USER_DEFINED_TYPE_NAME = 619,
        USER_DEFINED_TYPE_SCHEMA = 620,
        VIEW = 621,
        WORK = 622,
        WRITE = 623,
        X = 624, // Pyrrho 4.5
        Y = 625, // Pyrrho 4.5
        ZONE = 626
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
    /// </summary>
	internal class Interval
	{
		internal int years = 0, months = 0;
        internal long ticks = 0;
        internal bool yearmonth = true;
        public Interval(int y,int m) { years = y; months = m; }
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
    /// Row Versions are about durable data; but experimentally we extend this notion for incomplete transactions.
    /// Check allows transactions to find out if another transaction has overritten the row.
    /// Fields are modified only during commit serialisation
    /// </summary>
    internal class Rvv : BTree<long,(long,long?)>,IComparable
    {
        internal new static Rvv Empty = new Rvv();
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="d">the local database if any</param>
        /// <param name="t">the table defpos if local</param>
        /// <param name="r">remote database if any</param>
        /// <param name="df">the row defpos</param>
        Rvv() :base()
        { }
        protected Rvv(BTree<long,(long,long?)> t) : base(t.root) { }
        public static Rvv operator+(Rvv r,TableRow x)
        {
            return new Rvv(r + (x.tabledefpos, (x.defpos, x.time)));
        }
        public static Rvv operator+(Rvv r,(long,long,long)x)
        {
            var (t, d, o) = x;
            return new Rvv(r + (t, (d, o)));
        }
        public static Rvv operator+(Rvv r,Rvv s)
        {
            return new Rvv(r + s);
        }
        /// <summary>
        /// Validate an RVV string
        /// </summary>
        /// <param name="s">the string</param>
        /// <returns>the rvv</returns>
        internal bool Validate(Database db)
        {
            for (var b=First();b!=null;b=b.Next())
            {
                var t = (Table)db.objects[b.key()];
                var (d, o) = b.value();
                if (t.tableRows[d]?.time != o)
                    return false;
            }
            return true;
        }
        public static Rvv Parse(string s)
        {
            var r = Empty;
            var ss = s.Split(';');
            foreach(var t in ss)
            {
                var tt = t.Split(',');
                r += (long.Parse(tt[0]), long.Parse(tt[1]), long.Parse(tt[2]));
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
            var sc = "";
            for (var b=First();b!=null;b=b.Next())
            {
                sb.Append(sc); sc = ";";
                sb.Append(b.key()); sb.Append(",");
                var (d, o) = b.value();
                sb.Append(d); sb.Append(","); sb.Append(o);
            }
            return sb.ToString();
        }
        /// <summary>
        /// IComparable implementation
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public int CompareTo(object obj)
        {
            var that = obj as Rvv;
            if (that == null)
                return 1;
            int c = 0;
            var tb = that.First();
            var b = First();
            for (; c==0 && b != null && tb!=null; b = b.Next(),tb=tb.Next())
            {
                c = b.key().CompareTo(tb.key());
                if (c == 0)
                    c = b.value().Item1.CompareTo(tb.value().Item1);
                if (c == 0)
                    c = b.value().Item2.Value.CompareTo(tb.value().Item2.Value);
            }
            if (b != null)
                return 1;
            if (tb != null)
                return -1;
            return c;
        }

    }
    /// <summary>
    /// Supports the SQL2003 Date data type
    /// </summary>
	public class Date : IComparable
	{
		public DateTime date;
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
