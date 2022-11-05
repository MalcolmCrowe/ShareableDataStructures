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
        HTTP = 141,  // Pyrrho 7.01
        IDENTITY = 142,
        IF = 143,  // vol 4
        IN = 144,
        INDICATOR = 145,
        INNER = 146,
        INOUT = 147,
        INSENSITIVE = 148,
        INSERT = 149, // INT is 136, INTEGER is 135
        INTERSECT = 150,
        INTERSECTION = 151,
        INTERVAL = 152, // must be 152 see also INTERVAL0
        INTO = 153,
        IS = 154,
        ITERATE = 155, // vol 4
        JOIN = 156,
        LAG = 157,
        LANGUAGE = 158,
        LARGE = 159,
        LAST_DATA = 160, // Pyrrho v7
        LAST_VALUE = 161,
        LATERAL = 162,
        LEAD = 163,
        LEADING = 164,
        LEAVE = 165, // vol 4
        LEFT = 166,
        LIKE = 167,
        MULTISET = 168, // must be 168
        LN = 169,
        LOCAL = 170,
        NCHAR = 171, // must be 171	
        NCLOB = 172, // must be 172
        LOCALTIME = 173,
        LOCALTIMESTAMP = 174,
        LOOP = 175,  // vol 4
        LOWER = 176,
        NULL = 177, // must be 177
        MATCH = 178,
        NUMERIC = 179, // must be 179
        MAX = 180,
        MEMBER = 181,
        MERGE = 182,
        METHOD = 183,
        MIN = 184,
        MINUTE = 185,
        MOD = 186,
        MODIFIES = 187,
        MODULE = 188,
        MONTH = 189,	 // MULTISET is 168
        NATIONAL = 190,
        NATURAL = 191, // NCHAR 171 NCLOB 172
        NEW = 192,
        NO = 193,
        NONE = 194,
        NORMALIZE = 195,
        NOT = 196,    // NULL 177 
        NULLIF = 197, // NUMERIC 179, see also NUMERICLITERAL
        OBJECT = 198,
        REAL0 = 199, // must be 199, previous version of REAL
        OCCURRENCES_REGEX = 200, // alphabetical sequence is a misprint in SQL2008
        OCTET_LENGTH = 201,
        OF = 202,
        REAL = 203, // must be 203, see also REAL0 and REALLITERAL
        OFFSET = 204,
        OLD = 205,
        ON = 206,
        ONLY = 207,
        OPEN = 208,
        OR = 209,
        ORDER = 210,
        OUT = 211,
        OUTER = 212,
        OVER = 213,
        OVERLAPS = 214,
        OVERLAY = 215,
        PARAMETER = 216,
        PARTITION = 217,
        PASSWORD = 218, // must be 218, Pyrrho v5
        PERCENT = 219,
        PERIOD = 220,
        PORTION = 221,
        POSITION = 222,
        POWER = 223,
        PRECEDES = 224,
        PRECISION = 225,
        PREPARE = 226,
        PRIMARY = 227,
        PROCEDURE = 228,
        RANGE = 229,
        RANK = 230,
        READS = 231,    // REAL 203 (previously 199 see REAL0)
        RECURSIVE = 232,
        REF = 233,
        REFERENCES = 234,
        REFERENCING = 235,
        RELEASE = 236,
        REPEAT = 237, // vol 4
        RESIGNAL = 238, // vol 4
        RESULT = 239,
        RETURN = 240,
        RETURNS = 241,
        REVOKE = 242,
        RIGHT = 243,
        ROLLBACK = 244,
        ROW = 245,
        ROW_NUMBER = 246,
        ROWS = 247,
        SAVEPOINT = 248,
        SCOPE = 249,
        SCROLL = 250,
        SEARCH = 251,
        SECOND = 252,
        SELECT = 253,
        SENSITIVE = 254,    
        SESSION_USER = 255,
        SET = 256,
        TIME = 257, // must be 257
        TIMESTAMP = 258, // must be 258
        SIGNAL = 259, //vol 4
        SMALLINT = 260,
        SOME = 261,
        SPECIFIC = 262,
        SPECIFICTYPE = 263,
        SQL = 264,
        SQLEXCEPTION = 265,
        SQLSTATE = 266,
        TYPE = 267, // must be 267 but is not a reserved word
        SQLWARNING = 268,
        SQRT = 269,
        START = 270,
        STATIC = 271,
        STDDEV_POP = 272,
        STDDEV_SAMP = 273,
        SUBMULTISET = 274,
        SUBSTRING = 275, //
        SUBSTRING_REGEX = 276,
        SUCCEEDS = 277,
        SUM = 278, //
        SYMMETRIC = 279,
        SYSTEM = 280,
        SYSTEM_TIME = 281,
        SYSTEM_USER = 282,  // TABLE is 297
        TABLESAMPLE = 283,
        THEN = 284,  // TIME is 257, TIMESTAMP is 258	
        TIMEZONE_HOUR = 285,
        TIMEZONE_MINUTE = 286,
        TO = 287,
        TRAILING = 288,
        TRANSLATE = 289,
        TRANSLATION = 290,
        TREAT = 291,
        TRIGGER = 292,
        TRIM = 293,
        TRIM_ARRAY = 294,
        TRUE = 295,
        TRUNCATE = 296,
        TABLE = 297, // must be 297
        UESCAPE = 298,
        UNION = 299,
        UNIQUE = 300,
        UNKNOWN = 301,
        UNNEST = 302,
        UNTIL = 303, // vol 4
        UPDATE = 304,
        UPPER = 305, //
        USER = 306,
        USING = 307,
        VALUE = 308,
        VALUE_OF = 309,
        VALUES = 310,
        VARBINARY = 311,
        VARCHAR = 312,
        VARYING = 313,
        VERSIONING = 314,
        WHEN = 315,
        WHENEVER = 316,
        WHERE = 317,
        WHILE = 318, // vol 4
        WINDOW = 319,
        WITH = 320,
        WITHIN = 321,
        WITHOUT = 322, // XML is 356 vol 14
        XMLAGG = 323, // Basic XML stuff + XMLAgg vol 14
        XMLATTRIBUTES = 324,
        XMLBINARY = 325,
        XMLCAST = 326,
        XMLCOMMENT = 327,
        XMLCONCAT = 328,
        XMLDOCUMENT = 329,
        XMLELEMENT = 330,
        XMLEXISTS = 331,
        XMLFOREST = 332,
        XMLITERATE = 333,
        XMLNAMESPACES = 334,
        XMLPARSE = 335,
        XMLPI = 336,
        XMLQUERY = 337,
        XMLSERIALIZE = 338,
        XMLTABLE = 339,
        XMLTEXT = 340,
        XMLVALIDATE = 341,
        YEAR = 342,	// last reserved word
        //====================TOKEN TYPES=====================
        ASSIGNMENT = 343, // := 
        BLOBLITERAL = 344, // 
        BOOLEANLITERAL = 345,
        CHARLITERAL = 346, //
        COLON = 347, // :
        COMMA = 348,  // ,        
        CONCATENATE = 349, // ||        
        DIVIDE = 350, // /        
        DOCUMENTLITERAL = 351, // v5.1
        DOT = 352, // . 5.2 was STOP
        DOUBLECOLON = 353, // ::        
        EMPTY = 354, // []        
        EQL = 355,  // =        
        XML = 356, // must be 356 (is a reserved word)
        GEQ = 357, // >=    
        GTR = 358, // >    
        ID = 359, // identifier
        INTEGERLITERAL = 360, // Pyrrho
        LBRACE = 361, // {
        LBRACK = 362, // [
        LEQ = 363, // <=
        LPAREN = 364, // (
        LSS = 365, // <
        MEXCEPT = 366, // 
        MINTERSECT = 367, //
        MINUS = 368, // -
        MUNION = 369, // 
        NEQ = 370, // <>
        NUMERICLITERAL = 371, // 
        PLUS = 372, // + 
        QMARK = 373, // ?
        RBRACE = 374, // } 
        RBRACK = 375, // ] 
        RDFDATETIME = 376, //
        RDFLITERAL = 377, // 
        RDFTYPE = 378, // Pyrrho 7.0
        REALLITERAL = 379, //
        RPAREN = 380, // ) 
        SEMICOLON = 381, // ; 
        TIMES = 382, // *
        VBAR = 383, // | 
        //=========================NON-RESERVED WORDS================
        A = 384, // first non-reserved word
        ABSOLUTE = 385,
        ACTION = 386,
        ADA = 387,
        ADD = 388,
        ADMIN = 389,
        AFTER = 390,
        ALWAYS = 391,
        APPLICATION = 392, // Pyrrho 4.6
        ASC = 393,
        ASSERTION = 394,
        ATTRIBUTE = 395,
        ATTRIBUTES = 396,
        BEFORE = 397,
        BERNOULLI = 398,
        BREADTH = 399,
        BREAK = 400, // Pyrrho
        C = 401,
        CAPTION = 402, // Pyrrho 4.5
        CASCADE = 403,
        CATALOG = 404,
        CATALOG_NAME = 405,
        CHAIN = 406,
        CHARACTER_SET_CATALOG = 407,
        CHARACTER_SET_NAME = 408,
        CHARACTER_SET_SCHEMA = 409,
        CHARACTERISTICS = 410,
        CHARACTERS = 411,
        CLASS_ORIGIN = 412,
        COBOL = 413,
        COLLATION = 414,
        COLLATION_CATALOG = 415,
        COLLATION_NAME = 416,
        COLLATION_SCHEMA = 417,
        COLUMN_NAME = 418,
        COMMAND_FUNCTION = 419,
        COMMAND_FUNCTION_CODE = 420,
        COMMITTED = 421,
        CONDITION_NUMBER = 422,
        CONNECTION = 423,
        CONNECTION_NAME = 424,
        CONSTRAINT_CATALOG = 425,
        CONSTRAINT_NAME = 426,
        CONSTRAINT_SCHEMA = 427,
        CONSTRAINTS = 428,
        CONSTRUCTOR = 429,
        CONTENT = 430,
        CONTINUE = 431,
        CSV = 432, // Pyrrho 5.5
        CURATED = 433, // Pyrrho
        CURSOR_NAME = 434,
        DATA = 435,
        DATABASE = 436, // Pyrrho
        DATETIME_INTERVAL_CODE = 437,
        DATETIME_INTERVAL_PRECISION = 438,
        DEFAULTS = 439,
        DEFERRABLE = 440,
        DEFERRED = 441,
        DEFINED = 442,
        DEFINER = 443,
        DEGREE = 444,
        DEPTH = 445,
        DERIVED = 446,
        DESC = 447,
        DESCRIPTOR = 448,
        DIAGNOSTICS = 449,
        DISPATCH = 450,
        DOMAIN = 451,
        DYNAMIC_FUNCTION = 452,
        DYNAMIC_FUNCTION_CODE = 453,
        ENFORCED = 454,
        ENTITY = 455, // Pyrrho 4.5
        ETAG = 456, // Pyrrho Metadata 7.0
        EXCLUDE = 457,
        EXCLUDING = 458,
        FINAL = 459,
        FIRST = 460,
        FLAG = 461,
        FOLLOWING = 462,
        FORTRAN = 463,
        FOUND = 464,
        G = 465,
        GENERAL = 466,
        GENERATED = 467,
        GO = 468,
        GOTO = 469,
        GRANTED = 470,
        HIERARCHY = 471,
        HISTOGRAM = 472, // Pyrrho 4.5
        HTTPDATE = 473, // Pyrrho 7 RFC 7231
        IGNORE = 474,
        IMMEDIATE = 475,
        IMMEDIATELY = 476,
        IMPLEMENTATION = 477,
        INCLUDING = 478,
        INCREMENT = 479,
        INITIALLY = 480,
        INPUT = 481,
        INSTANCE = 482,
        INSTANTIABLE = 483,
        INSTEAD = 484,
        INVERTS = 485, // Pyrrho Metadata 5.7
        INVOKER = 486,
        IRI = 487, // Pyrrho 7
        ISOLATION = 488,
        JSON = 489, // Pyrrho 5.5
        K = 490,
        KEY = 491,
        KEY_MEMBER = 492,
        KEY_TYPE = 493,
        LAST = 494,
        LEGEND = 495, // Pyrrho Metadata 4.8
        LENGTH = 496,
        LEVEL = 497,
        LINE = 498, // Pyrrho 4.5
        LOCATOR = 499,
        M = 500,
        MAP = 501,
        MATCHED = 502,
        MAXVALUE = 503,
        MESSAGE_LENGTH = 504,
        MESSAGE_OCTET_LENGTH = 505,
        MESSAGE_TEXT = 506,
        METADATA = 507, // Pyrrho 7
        MILLI = 508, // Pyrrho 7
        MIME = 509, // Pyrrho 7
        MINVALUE = 510,
        MONOTONIC = 511, // Pyrrho 5.7
        MORE = 512,
        MUMPS = 513,
        NAME = 514,
        NAMES = 515,
        NESTING = 516,
        NEXT = 517,
        NFC = 518,
        NFD = 519,
        NFKC = 520,
        NFKD = 521,
        NORMALIZED = 522,
        NULLABLE = 523,
        NULLS = 524,
        NUMBER = 525,
        OCCURRENCE = 526,
        OCTETS = 527,
        OPTION = 528,
        OPTIONS = 529,
        ORDERING = 530,
        ORDINALITY = 531,
        OTHERS = 532,
        OUTPUT = 533,
        OVERRIDING = 534,
        OWNER = 535, // Pyrrho
        P = 536,
        PAD = 537,
        PARAMETER_MODE = 538,
        PARAMETER_NAME = 539,
        PARAMETER_ORDINAL_POSITION = 540,
        PARAMETER_SPECIFIC_CATALOG = 541,
        PARAMETER_SPECIFIC_NAME = 542,
        PARAMETER_SPECIFIC_SCHEMA = 543,
        PARTIAL = 544,
        PASCAL = 545,
        PATH = 546,
        PIE = 547, // Pyrrho 4.5
        PLACING = 548,
        PL1 = 549,
        POINTS = 550, // Pyrrho 4.5
        PRECEDING = 551,
        PREFIX = 552, // Pyrrho 7.01
        PRESERVE = 553,
        PRIOR = 554,
        PRIVILEGES = 555,
        PROFILING = 556, // Pyrrho
        PROVENANCE = 557, // Pyrrho
        PUBLIC = 558,
        READ = 559,
        REFERRED = 560, // 5.2
        REFERS = 561, // 5.2
        RELATIVE = 562,
        REPEATABLE = 563,
        RESPECT = 564,
        RESTART = 565,
        RESTRICT = 566,
        RETURNED_CARDINALITY = 567,
        RETURNED_LENGTH = 568,
        RETURNED_OCTET_LENGTH = 569,
        RETURNED_SQLSTATE = 570,
        ROLE = 571,
        ROUTINE = 572,
        ROUTINE_CATALOG = 573,
        ROUTINE_NAME = 574,
        ROUTINE_SCHEMA = 575,
        ROW_COUNT = 576,
        SCALE = 577,
        SCHEMA = 578,
        SCHEMA_NAME = 579,
        SCOPE_CATALOG = 580,
        SCOPE_NAME = 581,
        SCOPE_SCHEMA = 582,
        SECTION = 583,
        SECURITY = 584,
        SELF = 585,
        SEQUENCE = 586,
        SERIALIZABLE = 587,
        SERVER_NAME = 588,
        SESSION = 589,
        SETS = 590,
        SIMPLE = 591,
        SIZE = 592,
        SOURCE = 593,
        SPACE = 594,
        SPECIFIC_NAME = 595,
        SQLAGENT = 596, // Pyrrho 7
        STANDALONE = 597, // vol 14
        STATE = 598,
        STATEMENT = 599,
        STRUCTURE = 600,
        STYLE = 601,
        SUBCLASS_ORIGIN = 602,
        SUFFIX = 603, // Pyrrho 7.01
        T = 604,
        TABLE_NAME = 605,
        TEMPORARY = 606,
        TIES = 607,
        TIMEOUT = 608, // Pyrrho
        TOP_LEVEL_COUNT = 609,
        TRANSACTION = 610,
        TRANSACTION_ACTIVE = 611,
        TRANSACTIONS_COMMITTED = 612,
        TRANSACTIONS_ROLLED_BACK = 613,
        TRANSFORM = 614,
        TRANSFORMS = 615,
        TRIGGER_CATALOG = 616,
        TRIGGER_NAME = 617,
        TRIGGER_SCHEMA = 618, // TYPE  is 267 but is not a reserved word
        TYPE_URI = 619, // Pyrrho
        UNBOUNDED = 620,
        UNCOMMITTED = 621,
        UNDER = 622,
        UNDO = 623,
        UNNAMED = 624,
        URL = 625,  // Pyrrho 7
        USAGE = 626,
        USER_DEFINED_TYPE_CATALOG = 627,
        USER_DEFINED_TYPE_CODE = 628,
        USER_DEFINED_TYPE_NAME = 629,
        USER_DEFINED_TYPE_SCHEMA = 630,
        VIEW = 631,
        WORK = 632,
        WRITE = 633,
        X = 634, // Pyrrho 4.5
        Y = 635, // Pyrrho 4.5
        ZONE = 636
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
