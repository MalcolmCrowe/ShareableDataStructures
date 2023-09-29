using System.Text;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
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
        SET = 255,  // must be 255
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
        ARROW = 342, // ]-> 7.03
        ARROWBASE = 343, // -[ 7.03 see RARROWBASE etc
        ASSIGNMENT = 344, // := 
        BLOBLITERAL = 345, // 
        BOOLEANLITERAL = 346,
        CHARLITERAL = 347, //
        COLON = 348, // :
        COMMA = 349,  // ,        
        CONCATENATE = 350, // ||        
        DIVIDE = 351, // /        
        DOCUMENTLITERAL = 352, // unused as of v7
        DOT = 353, // . 5.2 was STOP
        DOUBLECOLON = 354, // ::        
        EMPTY = 355, // []        
        EQL = 356,  // =        
        GEQ = 357, // >=    
        XML = 358, // must be 356 (is a reserved word)
        GTR = 359, // >    
        Id = 360, // identifier
        INTEGERLITERAL = 361, // Pyrrho
        LBRACE = 362, // {
        LBRACK = 363, // [
        LEQ = 364, // <=
        LPAREN = 365, // (
        LSS = 366, // <
        MEXCEPT = 367, // 
        MINTERSECT = 368, //
        MINUS = 369, // -
        MUNION = 370, // 
        NEQ = 371, // <>
        NUMERICLITERAL = 372, // 
        PLUS = 373, // + 
        QMARK = 374, // ?
        RARROW = 375, // <-[ 7.03 see ARROW
        RARROWBASE = 376, // ]- 7.03
        RBRACE = 377, // } 
        RBRACK = 378, // ] 
        RDFDATETIME = 379, //
        RDFLITERAL = 380, // 
        RDFTYPE = 381, // Pyrrho 7.0
        REALLITERAL = 382, //
        RPAREN = 383, // ) 
        SEMICOLON = 384, // ; 
        TIMES = 385, // *
        VBAR = 386, // | 
        //=========================NON-RESERVED WORDS================
        A = 387, // first non-reserved word
        ABSOLUTE = 388,
        ACTION = 389,
        ACYCLIC = 390,
        ADA = 391,
        ADD = 392,
        ADMIN = 393,
        AFTER = 394,
        ALWAYS = 395,
        APPLICATION = 396, // Pyrrho 4.6
        ASC = 397,
        ASSERTION = 398,
        ATTRIBUTE = 399,
        ATTRIBUTES = 400,
        BEFORE = 401,
        BERNOULLI = 402,
        BREADTH = 403,
        BREAK = 404, // Pyrrho
        C = 405,
        CAPTION = 406, // Pyrrho 4.5
        CASCADE = 407,
        CATALOG = 408,
        CATALOG_NAME = 409,
        CHAIN = 410,
        CHARACTER_SET_CATALOG = 411,
        CHARACTER_SET_NAME = 412,
        CHARACTER_SET_SCHEMA = 413,
        CHARACTERISTICS = 414,
        CHARACTERS = 415,
        CLASS_ORIGIN = 416,
        COBOL = 417,
        COLLATION = 418,
        COLLATION_CATALOG = 419,
        COLLATION_NAME = 420,
        COLLATION_SCHEMA = 421,
        COLUMN_NAME = 422,
        COMMAND_FUNCTION = 423,
        COMMAND_FUNCTION_CODE = 424,
        COMMITTED = 425,
        CONDITION_NUMBER = 426,
        CONNECTION = 427,
        CONNECTION_NAME = 428,
        CONSTRAINT_CATALOG = 429,
        CONSTRAINT_NAME = 430,
        CONSTRAINT_SCHEMA = 431,
        CONSTRAINTS = 432,
        CONSTRUCTOR = 433,
        CONTENT = 434,
        CONTINUE = 435,
        CSV = 436, // Pyrrho 5.5
        CURATED = 437, // Pyrrho
        CURSOR_NAME = 438,
        DATA = 439,
        DATABASE = 440, // Pyrrho
        DATETIME_INTERVAL_CODE = 441,
        DATETIME_INTERVAL_PRECISION = 442,
        DEFAULTS = 443,
        DEFERRABLE = 444,
        DEFERRED = 445,
        DEFINED = 446,
        DEFINER = 447,
        DEGREE = 448,
        DEPTH = 449,
        DERIVED = 450,
        DESC = 451,
        DESCRIPTOR = 452,
        DETACH = 453, // Pyrrho 7.05
        DIAGNOSTICS = 454,
        DISPATCH = 455,
        DOMAIN = 456,
        DYNAMIC_FUNCTION = 457,
        DYNAMIC_FUNCTION_CODE = 458,
        EDGETYPE = 459, // Metadata 7.03 must be 459
        EDGE = 460, // 7.03
        ELEMENTID = 461, // Pyrrho 7.05
        ENFORCED = 462,
        ENTITY = 463, // Pyrrho 4.5
        ETAG = 464, // Pyrrho Metadata 7.0
        EXCLUDE = 465,
        EXCLUDING = 466,
        FINAL = 467,
        FIRST = 468,
        FLAG = 469,
        FOLLOWING = 470,
        FORTRAN = 471,
        FOUND = 472,
        G = 473,
        GENERAL = 474,
        GENERATED = 475,
        GO = 476,
        GOTO = 477,
        GRANTED = 478,
        GRAPH = 479, //7.03
        HIERARCHY = 480,
        HISTOGRAM = 481, // Pyrrho 4.5
        HTTPDATE = 482, // Pyrrho 7 RFC 7231
        ID = 483, // Distinguished from the token type Id from Pyrrho 7.05
        IGNORE = 484,
        IMMEDIATE = 485,
        IMMEDIATELY = 486,
        IMPLEMENTATION = 487,
        INCLUDING = 488,
        INCREMENT = 489,
        INITIALLY = 490,
        INPUT = 491,
        INSTANCE = 492,
        INSTANTIABLE = 493,
        INSTEAD = 494,
        INVERTS = 495, // Pyrrho Metadata 5.7
        INVOKER = 496,
        IRI = 497, // Pyrrho 7
        ISOLATION = 498,
        JSON = 499, // Pyrrho 5.5
        K = 500,
        KEY = 501,
        KEY_MEMBER = 502,
        KEY_TYPE = 503,
        LABELS = 504, // Pyrrho 7.05
        LAST = 505,
        LEGEND = 506, // Pyrrho Metadata 4.8
        LENGTH = 507,
        LEVEL = 508,
        LINE = 509, // Pyrrho 4.5
        LOCATOR = 510,
        M = 511,
        MAP = 512,
        MATCHED = 513,
        METADATA = 514, // Pyrrho 7 must be 514
        MAXVALUE = 515,
        MESSAGE_LENGTH = 516,
        MESSAGE_OCTET_LENGTH = 517,
        MESSAGE_TEXT = 518,
        MILLI = 519, // Pyrrho 7
        MIME = 520, // Pyrrho 7
        MINVALUE = 521,
        MONOTONIC = 522, // Pyrrho 5.7
        MORE = 523,
        MULTIPLICITY = 524, // Pyrrho 7.03
        MUMPS = 525,
        NAME = 526,
        NAMES = 527,
        NESTING = 528,
        NEXT = 529,
        NFC = 530,
        NODETYPE = 531, // Metadata 7.03 must be 531
        NFD = 532,
        NFKC = 533,
        NFKD = 534,
        NODE = 535, // 7.03
        NORMALIZED = 536,
        NULLABLE = 537,
        NULLS = 538,
        NUMBER = 539,
        OCCURRENCE = 540,
        OCTETS = 541,
        OPTION = 542,
        OPTIONS = 543,
        ORDERING = 544,
        ORDINALITY = 545,
        OTHERS = 546,
        OUTPUT = 547,
        OVERRIDING = 548,
        OWNER = 549, // Pyrrho
        P = 550,
        PAD = 551,
        PARAMETER_MODE = 552,
        PARAMETER_NAME = 553,
        PARAMETER_ORDINAL_POSITION = 554,
        PARAMETER_SPECIFIC_CATALOG = 555,
        PARAMETER_SPECIFIC_NAME = 556,
        PARAMETER_SPECIFIC_SCHEMA = 557,
        PARTIAL = 558,
        PASCAL = 559,
        PATH = 560,
        PIE = 561, // Pyrrho 4.5
        PLACING = 562,
        PL1 = 563,
        POINTS = 564, // Pyrrho 4.5
        PRECEDING = 565,
        PREFIX = 566, // Pyrrho 7.01
        PRESERVE = 567,
        PRIOR = 568,
        PRIVILEGES = 569,
        PROFILING = 570, // Pyrrho
        PUBLIC = 571,
        READ = 572,
        REFERRED = 573, // 5.2
        REFERS = 574, // 5.2
        RELATIVE = 575,
        REPEATABLE = 576,
        RESPECT = 577,
        RESTART = 578,
        RESTRICT = 579,
        RETURNED_CARDINALITY = 580,
        RETURNED_LENGTH = 581,
        RETURNED_OCTET_LENGTH = 582,
        RETURNED_SQLSTATE = 583,
        ROLE = 584,
        ROUTINE = 585,
        ROUTINE_CATALOG = 586,
        ROUTINE_NAME = 587,
        ROUTINE_SCHEMA = 588,
        ROW_COUNT = 589,
        SCALE = 590,
        SCHEMA = 591,
        SCHEMA_NAME = 592,
        SCOPE_CATALOG = 593,
        SCOPE_NAME = 594,
        SCOPE_SCHEMA = 595,
        SECTION = 596,
        SECURITY = 597,
        SELF = 598,
        SEQUENCE = 599,
        SERIALIZABLE = 600,
        SERVER_NAME = 601,
        SESSION = 602,
        SETS = 603,
        SHORTEST = 604,
        SHORTESTPATH = 605,
        SIMPLE = 606,
        SIZE = 607,
        SOURCE = 608,
        SPACE = 609,
        SPECIFIC_NAME = 610,
        SQLAGENT = 611, // Pyrrho 7
        STANDALONE = 612, // vol 14
        STATE = 613,
        STATEMENT = 614,
        STRUCTURE = 615,
        STYLE = 616,
        SUBCLASS_ORIGIN = 617,
        SUFFIX = 618, // Pyrrho 7.01
        T = 619,
        TABLE_NAME = 630,
        TEMPORARY = 621,
        TIES = 622,
        TIMEOUT = 623, // Pyrrho
        TOP_LEVEL_COUNT = 624,
        TRAIL = 625,
        TRANSACTION = 626,
        TRANSACTION_ACTIVE = 627,
        TRANSACTIONS_COMMITTED = 628,
        TRANSACTIONS_ROLLED_BACK = 629,
        TRANSFORM = 630,
        TRANSFORMS = 631,
        TRIGGER_CATALOG = 632,
        TRIGGER_NAME = 633,
        TRIGGER_SCHEMA = 634, // TYPE  is 267 but is not a reserved word
        TYPE_URI = 635, // Pyrrho
        UNBOUNDED = 636,
        UNCOMMITTED = 637,
        UNDER = 638,
        UNDO = 639,
        UNNAMED = 640,
        URL = 641,  // Pyrrho 7
        USAGE = 642,
        USER_DEFINED_TYPE_CATALOG = 643,
        USER_DEFINED_TYPE_CODE = 644,
        USER_DEFINED_TYPE_NAME = 645,
        USER_DEFINED_TYPE_SCHEMA = 646,
        VIEW = 647,
        WORK = 648,
        WRITE = 649,
        X = 650, // Pyrrho 4.5
        Y = 651, // Pyrrho 4.5
        ZONE = 652
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
        Multiset,   // Integer els, els x obs (also for Set)
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
        readonly TChar iso = new ("ISO 9075");
        readonly TChar pyrrho = new ("Pyrrho");
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
	internal class Interval : IComparable
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
        public int CompareTo(object? obj)
        {
            if (obj is Interval that && yearmonth == that.yearmonth)
            {
                var c = years.CompareTo(that.years);
                if (c != 0)
                    return c;
                c = months.CompareTo(that.months);
                if (c != 0)
                    return c;
                return ticks.CompareTo(that.ticks);
            }
            else throw new DBException("22006");
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
    internal class Rvv : BTree<long, BTree<long, long?>>
    {
        internal const long
            RVV = -193; // Rvv
        internal new static Rvv Empty = new ();
        internal long version => First()?.value()?.Last()?.value() ?? 0L;
        Rvv() : base()
        { }
        protected Rvv(BTree<long, BTree<long, long?>> t) : base(t.root ?? throw new PEException("PE925")) { }
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
            var s = ((d == -1L) ? null : r[t]) ?? BTree<long, long?>.Empty;
            return new Rvv(r + (t, s + (d, o)));
        }
        public static Rvv operator +(Rvv r, Rvv s)
        {
            if (r == null || r == Empty)
                return s;
            if (s == null || s == Empty)
                return r;
            // Implement a wild-card -1L
            var a = (BTree<long, BTree<long, long?>>)r;
            var b = (BTree<long, BTree<long, long?>>)s;
            for (var bb = b.First(); bb != null; bb = bb.Next())
                if (bb.value() is BTree<long, long?> bt)
                {
                    var k = bb.key();
                    /* If we read the whole table, then any change will be a conflict, 
                     * so we record -1, lastData; */
                    if (bt.Contains(-1L))
                        a += (k, bt);
                    else
                    /* we override a previous -1,lastData entry with specific information. */
                    if (a[k] is BTree<long, long?> at)
                    {
                        if (at.Contains(-1L))
                            at -= -1L;
                        for (var cb = bt.First(); cb != null; cb = cb.Next())
                            if (cb.value() is long bv)
                            {
                                var bk = cb.key();
                                if (at[bk] is long ap)
                                    at += (bk, Math.Max(ap, bv));
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
        internal static bool Validate(Database db,string? es,string? eu)
        {
            if (es != null)
            {
                var e = Parse(es);
                for (var b = e?.First(); b != null; b = b.Next())
                    if (db.objects[b.key()] is Table tb)
                    {
                        for (var c = b.value()?.First(); c != null; c = c.Next())
                        {
                            var dp = c.key();
                            if (dp == -1L)
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
                    else return false;
            }
            if (eu is not null)
            {
                var ck = THttpDate.Parse(eu);
                if (ck != null && db.lastModified > ck.value)
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
            var tt = (st?.value is DateTime dt)? dt.Ticks - eps : 0;
            for (var b = First(); b != null; b = b.Next())
                if (db.objects[b.key()] is Table t)
                {
                    for (var c = b.value()?.First(); c != null; c = c.Next())
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
                else return false;
            return true;
        }
        public static Rvv Parse(string s)
        {
            if (s == null)
                return Empty;
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
            var v = long.Parse(u[1..]);
            return u[0] switch
            {
                '%' => v + Transaction.HeapStart,
                '`' => v + Transaction.Executables,
                '#' => v + Transaction.Analysing,
                '!' => v + Transaction.TransPos,
                _ => -1L,// should not occur
            };
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
                for (var c = b.value()?.First(); c != null; c = c.Next())
                if (c.value() is long p){
                    sb.Append(",");
                    sb.Append(DBObject.Uid(c.key())); sb.Append(",");
                    sb.Append(DBObject.Uid(p));
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
            return date.ToString("d",Thread.CurrentThread.CurrentUICulture);
        }
        #region IComparable Members

        public int CompareTo(object? obj)
        {
            if (obj is Date dt)
                obj = dt.date;
            return date.CompareTo(obj);
        }

        #endregion
    }
}
