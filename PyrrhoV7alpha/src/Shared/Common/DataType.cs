using System;
using System.Text;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland
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
        AT = 14,
        ATOMIC = 15,
        AUTHORIZATION = 16,
        AVG = 17, //
        BEGIN = 18,
        BEGIN_FRAME = 19,
        BEGIN_PARTITION = 20,
        BETWEEN = 21,
        BIGINT = 22,
        BINARY = 23,
        BLOB = 24, // BOOLEAN see 27
        BOTH = 25,
        BY = 26,
        BOOLEAN = 27, // must be 27
        CALL = 28,
        CALLED = 29,
        CARDINALITY = 30,
        CASCADED = 31,
        CASE = 32,
        CAST = 33,
        CEIL = 34,
        CEILING = 35, // CHAR see 37
        CHAR_LENGTH = 36,
        CHAR = 37, // must be 37: see also CHARLITERAL for literal 
        CHARACTER = 38,
        CHARACTER_LENGTH = 39,
        CLOB = 40, // must be 40
        CHECK = 41, // CLOB see 40
        CLOSE = 42,
        COALESCE = 43,
        COLLATE = 44,
        COLLECT = 45,
        COLUMN = 46,
        COMMIT = 47,
        CONDITION = 48,
        CONNECT = 49,
        CONSTRAINT = 50,
        CONTAINS = 51,
        CONVERT = 52,
#if OLAP
        CORR =53,
#endif
        CORRESPONDING = 54,
        COUNT = 55, //
#if OLAP
        COVAR_POP =56,
        COVAR_SAMP =57,
#endif
        CREATE = 58,
        CROSS = 59,
#if OLAP
        CUBE =60,
        CUME_DIST =61,
#endif
        CURRENT = 62,
        CURRENT_CATALOG = 63,
        CURRENT_DATE = 64,
        CURSOR = 65, // must be 65
        CURRENT_DEFAULT_TRANSFORM_GROUP = 66,
        DATE = 67, // must be 67	
        CURRENT_PATH = 68,
        CURRENT_ROLE = 69,
        CURRENT_ROW = 70,
        CURRENT_SCHEMA = 71,
        CURRENT_TIME = 72,
        CURRENT_TIMESTAMP = 73,
        CURRENT_TRANSFORM_GROUP_FOR_TYPE = 74,
        CURRENT_USER = 75, // CURSOR see 65
        CYCLE = 76, // DATE see 67
        DAY = 77,
        DEALLOCATE = 78,
        DEC = 79,
        DECIMAL = 80,
        DECLARE = 81,
        DEFAULT = 82,
        DELETE = 83,
#if OLAP
        DENSE_RANK =84,
#endif
        DEREF = 85,
        DESCRIBE = 86,
        DETERMINISTIC = 87,
        DISCONNECT = 88,
        DISTINCT = 89,
        DO = 90, // from vol 4
        DOCARRAY = 91, // Pyrrho 5.1
        DOCUMENT = 92, // Pyrrho 5.1
        DOUBLE = 93,
        DROP = 94,
        DYNAMIC = 95,
        EACH = 96,
        ELEMENT = 97,
        ELSE = 98,
        ELSEIF = 99, // from vol 4
        END = 100,
        END_EXEC = 101, // misprinted in SQL2011 as END-EXEC
        END_FRAME = 102,
        END_PARTITION = 103,
        EOF = 104,	// Pyrrho 0.1
        EQUALS = 105,
        ESCAPE = 106,
        EVERY = 107,
        EXCEPT = 108,
        EXEC = 109,
        EXECUTE = 110,
        EXISTS = 111,
        EXIT = 112, // from vol 4
        EXP = 113,
        EXTERNAL = 114,
        EXTRACT = 115,
        FALSE = 116,
        FETCH = 117,
        FILTER = 118,
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
        INTEGER = 135, // must be 135
        INT = 136,  // deprecated: see also INTEGERLITERAL
        INTERVAL0 = 137,  // must be 137 (old version of INTERVAL)
        HAVING = 138,
        HOLD = 139,
        HOUR = 140,
        HTTP = 141, // Pyrrho 4.5
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
        LAST_VALUE = 160,
        LATERAL = 161,
        LEADING = 162,
        LEAVE = 163, // vol 4
        LEFT = 164,
        LIKE = 165,
#if SIMILAR
        LIKE_REGEX =166,
#endif
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
        MAX = 178, //
        NUMERIC = 179, // must be 179
        MEMBER = 180,
        MERGE = 181,
        METHOD = 182,
        MIN = 183, //
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
#if OLAP
        NTH_VALUE = 196,
        NTILE = 197, // NULL 177
#endif
        NULLIF = 198, // NUMERIC 179, see also NUMERICLITERAL
        REAL0 = 199, // must be 199, previous version of REAL
        OBJECT = 200,
        OCCURRENCES_REGEX = 201, // alphabetical sequence is a misprint in SQL2008
        OCTET_LENGTH = 202,
        REAL = 203, // must be 203, see also REAL0 and REALLITERAL
        OF = 204,
        OFFSET = 205,
        OLD = 206,
        ON = 207,
        ONLY = 208,
        OPEN = 209,
        OR = 210,
        ORDER = 211,
        OUT = 212,
        OUTER = 213,
        OVER = 214,
        OVERLAPS = 215,
        OVERLAY = 216,
        PARAMETER = 217, // PASSWORD is 218
        PARTITION = 219,
        PASSWORD = 218, // must be 218, Pyrrho v5
        PERCENT = 220,
#if OLAP
        PERCENT_RANK =221,
        PERCENTILE_CONT =222,
        PERCENTILE_DISC = 223,
#endif
        PERIOD = 224,
        PORTION = 225,
        POSITION = 226,
#if OLAP || MONGO || SIMILAR
        POSITION_REGEX =227,
#endif
        POWER = 228,
        PRECEDES = 229,
        PRECISION = 230,
        PREPARE = 231,
        PRIMARY = 232,
        PROCEDURE = 233,
        RANGE = 234,
        RANK = 235,
        READS = 236,    // REAL 203 (previously 199 see REAL0)
        RECURSIVE = 237,
        REF = 238,
        REFERENCES = 239,
        REFERENCING = 240,
#if OLAP
        REGR_AVGX =241, 
        REGR_AVGY =242, 
        REGR_COUNT =243, 
        REGR_INTERCEPT =244,
        REGR_R2 =245, 
        REGR_SLOPE =246,
        REGR_SXX =247, 
        REGR_SXY =248, 
        REGR_SYY =249,
#endif
        RELEASE = 250,
        REPEAT = 251, // vol 4
        RESIGNAL = 252, // vol 4
        RESULT = 253,
        RETURN = 254,
        RETURNS = 255,
        REVOKE = 256,
        TIME = 257, // must be 257
        TIMESTAMP = 258, // must be 258
        RIGHT = 259,
        ROLLBACK = 260,
#if OLAP
        ROLLUP =261,
#endif
        ROW = 262,
        ROW_NUMBER = 263,
        ROWS = 264,
        SAVEPOINT = 265,
        SCOPE = 266,
        TYPE = 267, // must be 267 but is not a reserved word
        SCROLL = 268,
        SEARCH = 269,
        SECOND = 270,
        SELECT = 271,
        SENSITIVE = 272,    // has a different usage in Pyrrho
        SESSION_USER = 273,
        SET = 274,
        SIGNAL = 275, //vol 4
#if SIMILAR
        SIMILAR =276,
#endif
        SMALLINT = 277,
        SOME = 278,
        SPECIFIC = 279,
        SPECIFICTYPE = 280,
        SQL = 281,
        SQLEXCEPTION = 282,
        SQLSTATE = 283,
        SQLWARNING = 284,
        SQRT = 285,
        START = 286,
        STATIC = 287,
        STDDEV_POP = 288,
        STDDEV_SAMP = 289,
        SUBMULTISET = 290,
        SUBSTRING = 291, //
        SUBSTRING_REGEX = 292,
        SUCCEEDS = 293,
        SUM = 294, //
        SYMMETRIC = 295,
        SYSTEM = 296,
        TABLE = 297, // must be 297
        SYSTEM_TIME = 298,
        SYSTEM_USER = 299,  // TABLE is 297
        TABLESAMPLE = 300,
        THEN = 301,  // TIME is 257, TIMESTAMP is 258	
        TIMEZONE_HOUR = 302,
        TIMEZONE_MINUTE = 303,
        TO = 304,
        TRAILING = 305,
        TRANSLATE = 306,
#if SIMILAR
        TRANSLATE_REGEX =307,
#endif
        TRANSLATION = 308,
        TREAT = 309,
        TRIGGER = 310,
        TRIM = 311,
        TRIM_ARRAY = 312,
        TRUE = 313,
        TRUNCATE = 314,
        UESCAPE = 315,
        UNION = 316,
        UNIQUE = 317,
        UNKNOWN = 318,
        UNNEST = 319,
        UNTIL = 320, // vol 4
        UPDATE = 321,
        UPPER = 322, //
        USER = 323,
        USING = 324,
        VALUE = 325,
        VALUE_OF = 326,
        VALUES = 327,
#if OLAP
        VAR_POP =328,
        VAR_SAMP =329,
#endif
        VARBINARY = 330,
        VARCHAR = 331,
        VARYING = 332,
        VERSIONING = 333,
        WHEN = 334,
        WHENEVER = 335,
        WHERE = 336,
        WHILE = 337, // vol 4
#if OLAP
        WIDTH_BUCKET =338,
#endif
        WINDOW = 339,
        WITH = 340,
        WITHIN = 341,
        WITHOUT = 342, // XML is 356 vol 14
        XMLAGG = 343, // Basic XML stuff + XMLAgg vol 14
        XMLATTRIBUTES = 344,
        XMLBINARY = 345,
        XMLCAST = 346,
        XMLCOMMENT = 347,
        XMLCONCAT = 348,
        XMLDOCUMENT = 349,
        XMLELEMENT = 350,
        XMLEXISTS = 351,
        XMLFOREST = 352,
        XMLITERATE = 353,
        XMLNAMESPACES = 354,
        XMLPARSE = 355,
        XML = 356, // must be 356 
        XMLPI = 357,
        XMLQUERY = 358,
        XMLSERIALIZE = 359,
        XMLTABLE = 360,
        XMLTEXT = 361,
        XMLVALIDATE = 362,
        YEAR = 363,	// last reserved word
        //====================TOKEN TYPES=====================
        ASSIGNMENT = 364, // := 
        BLOBLITERAL = 365, // 
        BOOLEANLITERAL = 366,
        CHARLITERAL = 367, //
        COLON = 368, // :
        COMMA = 369,  // ,        
        CONCATENATE = 370, // ||        
        DIVIDE = 371, // /        
        DOCUMENTLITERAL = 372, // v5.1
        DOT = 373, // . 5.2 was STOP
        DOUBLECOLON = 374, // ::        
        EMPTY = 375, // []        
        EQL = 376,  // =        
        GEQ = 377, // >=    
        GTR = 378, // >    
        ID = 379, // identifier
        INTEGERLITERAL = 380, // Pyrrho
        LBRACE = 381, // {
        LBRACK = 382, // [
        LEQ = 383, // <=
        LPAREN = 384, // (
        LSS = 385, // <
        MEXCEPT = 386, // 
        MINTERSECT = 387, //
        MINUS = 388, // -
        MUNION = 389, // 
        NEQ = 390, // <>
        NUMERICLITERAL = 391, // 
        PLUS = 392, // + 
        QMARK = 393, // ?
        RBRACE = 394, // } 
        RBRACK = 395, // ] 
        RDFDATETIME = 396, //
        RDFLITERAL = 397, // 
        RDFTYPE = 398, // Pyrrho 7.0
        REALLITERAL = 399, //
        RPAREN = 400, // ) 
        SEMICOLON = 401, // ; 
        TIMES = 402, // *
        VBAR = 403, // | 
        //=========================NON-RESERVED WORDS================
        A = 404, // first non-reserved word
        ABSOLUTE = 405,
        ACTION = 406,
        ADA = 407,
        ADD = 408,
        ADMIN = 409,
        AFTER = 410,
        ALWAYS = 411,
        APPLICATION = 412, // Pyrrho 4.6
        ASC = 413,
        ASSERTION = 414,
        ATTRIBUTE = 415,
        ATTRIBUTES = 416,
        BEFORE = 417,
        BERNOULLI = 418,
        BREADTH = 419,
        BREAK = 420, // Pyrrho
        C = 421,
#if MONGO
        CAPPED = 422, // Pyrrho 5.1 for MongoDB
#endif
        CAPTION = 423, // Pyrrho 4.5
        CASCADE = 424,
        CATALOG = 425,
        CATALOG_NAME = 426,
        CHAIN = 427,
        CHARACTER_SET_CATALOG = 428,
        CHARACTER_SET_NAME = 429,
        CHARACTER_SET_SCHEMA = 430,
        CHARACTERISTICS = 431,
        CHARACTERS = 432,
        CLASS_ORIGIN = 433,
        COBOL = 434,
        COLLATION = 435,
        COLLATION_CATALOG = 436,
        COLLATION_NAME = 437,
        COLLATION_SCHEMA = 438,
        COLUMN_NAME = 439,
        COMMAND_FUNCTION = 440,
        COMMAND_FUNCTION_CODE = 441,
        COMMITTED = 442,
        CONDITION_NUMBER = 443,
        CONNECTION = 444,
        CONNECTION_NAME = 445,
        CONSTRAINT_CATALOG = 446,
        CONSTRAINT_NAME = 447,
        CONSTRAINT_SCHEMA = 448,
        CONSTRAINTS = 449,
        CONSTRUCTOR = 450,
        CONTENT = 451,
        CONTINUE = 452,
        CSV = 453, // Pyrrho 5.5
        CURATED = 454, // Pyrrho
        CURSOR_NAME = 455,
        DATA = 456,
        DATABASE = 457, // Pyrrho
        DATETIME_INTERVAL_CODE = 458,
        DATETIME_INTERVAL_PRECISION = 459,
        DEFAULTS = 460,
        DEFERRABLE = 461,
        DEFERRED = 462,
        DEFINED = 463,
        DEFINER = 464,
        DEGREE = 465,
        DEPTH = 466,
        DERIVED = 467,
        DESC = 468,
        DESCRIPTOR = 469,
        DIAGNOSTICS = 470,
        DISPATCH = 471,
        DOMAIN = 472,
#if MONGO
        DROPDUPS = 473, // Pyrrho 5.1 for MongoDB
#endif
        DYNAMIC_FUNCTION = 474,
        DYNAMIC_FUNCTION_CODE = 475,
        ENFORCED = 476,
        ENTITY = 477, // Pyrrho 4.5
        EXCLUDE = 478,
        EXCLUDING = 479,
        FINAL = 480,
        FIRST = 481,
        FLAG = 482,
        FOLLOWING = 483,
        FORTRAN = 484,
        FOUND = 485,
        G = 486,
        GENERAL = 487,
        GENERATED = 488,
        GO = 489,
        GOTO = 490,
        GRANTED = 491,
        HIERARCHY = 492,
        HISTOGRAM = 493, // Pyrrho 4.5
#if MONGO
        INDEX = 494, // Pyrrho 5.1 for MongoDB :(
#endif
        IGNORE = 495,
        IMMEDIATE = 496,
        IMMEDIATELY = 497,
        IMPLEMENTATION = 498,
        INCLUDING = 499,
        INCREMENT = 500,
        INITIALLY = 501,
        INPUT = 502,
        INSTANCE = 503,
        INSTANTIABLE = 504,
        INSTEAD = 505,
        INVERTS = 506, // Pyrrho Metadata 5.7
        INVOKER = 507,
        ISOLATION = 508,
        JSON = 509, // Pyrrho 5.5
        K = 510,
        KEY = 511,
        KEY_MEMBER = 512,
        KEY_TYPE = 513,
        LAST = 514,
        LEGEND = 515, // Pyrrho Metadata 4.8
        LENGTH = 516,
        LEVEL = 517,
        LINE = 518, // Pyrrho 4.5
        LOCATOR = 519,
        M = 520,
        MAP = 521,
        MATCHED = 522,
        MAXVALUE = 523,
        MESSAGE_LENGTH = 524,
        MESSAGE_OCTET_LENGTH = 525,
        MESSAGE_TEXT = 526,
        MINVALUE = 527,
        MONOTONIC = 528, // Pyrrho 5.7
        MORE = 529,
        MUMPS = 530,
        NAME = 531,
        NAMES = 532,
        NESTING = 532,
        NEXT = 534,
        NFC = 535,
        NFD = 536,
        NFKC = 537,
        NFKD = 538,
        NORMALIZED = 539,
        NULLABLE = 540,
        NULLS = 541,
        NUMBER = 542,
        OCCURRENCE = 543,
        OCTETS = 544,
        OPTION = 545,
        OPTIONS = 546,
        ORDERING = 547,
        ORDINALITY = 548,
        OTHERS = 549,
        OUTPUT = 550,
        OVERRIDING = 551,
        OWNER = 552, // Pyrrho
        P = 553,
        PAD = 554,
        PARAMETER_MODE = 555,
        PARAMETER_NAME = 556,
        PARAMETER_ORDINAL_POSITION = 557,
        PARAMETER_SPECIFIC_CATALOG = 558,
        PARAMETER_SPECIFIC_NAME = 559,
        PARAMETER_SPECIFIC_SCHEMA = 560,
        PARTIAL = 561,
        PASCAL = 562,
        PATH = 563,
        PIE = 564, // Pyrrho 4.5
        PLACING = 565,
        PL1 = 566,
        POINTS = 567, // Pyrrho 4.5
        PRECEDING = 568,
        PRESERVE = 569,
        PRIOR = 570,
        PRIVILEGES = 571,
        PROFILING = 572, // Pyrrho
        PROVENANCE = 573, // Pyrrho
        PUBLIC = 574,
        READ = 575,
        REFERRED = 576, // 5.2
        REFERS = 577, // 5.2
#if MONGO || OLAP || SIMILAR
        REGULAR_EXPRESSION = 578, // Pyrrho 5.1
#endif
        RELATIVE = 579,
        REPEATABLE = 580,
        RESPECT = 581,
        RESTART = 582,
        RESTRICT = 583,
        RETURNED_CARDINALITY = 584,
        RETURNED_LENGTH = 585,
        RETURNED_OCTET_LENGTH = 586,
        RETURNED_SQLSTATE = 587,
        ROLE = 588,
        ROUTINE = 589,
        ROUTINE_CATALOG = 590,
        ROUTINE_NAME = 591,
        ROUTINE_SCHEMA = 592,
        ROW_COUNT = 593,
        SCALE = 594,
        SCHEMA = 595,
        SCHEMA_NAME = 596,
        SCOPE_CATALOG = 597,
        SCOPE_NAME = 598,
        SCOPE_SCHEMA = 599,
        SECTION = 600,
        SECURITY = 601,
        SELF = 602,
        SEQUENCE = 603,
        SERIALIZABLE = 604,
        SERVER_NAME = 605,
        SESSION = 606,
        SETS = 607,
        SIMPLE = 608,
        SIZE = 609,
        SOURCE = 610,
        SPACE = 611,
#if MONGO
        SPARSE = 612,  // Pyrrho 51 for MongoDB
#endif
        SPECIFIC_NAME = 613,
        STANDALONE = 614, // vol 14
        STATE = 615,
        STATEMENT = 616,
        STRUCTURE = 617,
        STYLE = 618,
        SUBCLASS_ORIGIN = 619,
        T = 620,
        TABLE_NAME = 621,
        TEMPORARY = 622,
        TIES = 623,
        TIMEOUT = 624, // Pyrrho
        TOP_LEVEL_COUNT = 625,
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
        USAGE = 641,
#if MONGO
        USEPOWEROF2SIZES = 642, // Pyrrho 5.1 for Mongo
#endif
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
		TimeStamp,	// Integer(UTC ticks)
		Interval,	// Integer[3] (years,months,ticks)
		Integer,	// 1024-bit Integer
		Numeric,	// 1024-bit Integer, precision, scale
		String,		// string: Integer length, length x byte
		Date,		// Integer (UTC ticks)
		TimeSpan,	// Integer (UTC ticks)
		Boolean,	// byte 3 values: T=1,F=0,U=255
		DomainRef,		// typedefpos, Integer els, els x data (preserved for compatibility)
		Blob,		// Integer length, length x byte: Opaque binary type (Clob is String)
		Row,		// spec, Integer cols, cols x data
		Multiset,	// Integer els, els x data
		Array,		// Integer els, els x data
        Password    // A more secure type of string (write-only)
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
            ATree<Sqlx, TypedValue>.Add(ref info, k, v??TNull.Value);
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
    internal class Rvv : IComparable
    {
        long tbpos = 0; // the defining position for a table in the transaction file
        internal long def; // the defining position for a row in the transaction file
        internal long off; // the current row version
        const long t0 = Transaction.TransPos; // "positions" over this value have not been committed
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="d">the local database if any</param>
        /// <param name="t">the table defpos if local</param>
        /// <param name="r">remote database if any</param>
        /// <param name="df">the row defpos</param>
        internal Rvv(long t,long df,long o)
        {
            tbpos = t;
            def = df;
            off = o;
        }
        /// <summary>
        /// Construct an RVV for a given row
        /// </summary>
        /// <param name="dn">The database name</param>
        /// <param name="df">The row defining position</param>
        /// <param name="o">The row current position</param>
        /// <returns></returns>
        internal static Rvv For(TableRow rc)
        {
            return new Rvv(rc.tabledefpos, rc.defpos,rc.ppos);
        }
        internal static Rvv For(Transaction tr,long tp,long rp)
        {
            var rc = (tr.role.objects[tp] as Table)?.tableRows[rp];
            return (rc != null) ? new Rvv(tp, rp, rc.ppos) : null;
        }
        /// <summary>
        /// Validate an RVV string
        /// </summary>
        /// <param name="s">the string</param>
        /// <returns>the rvv</returns>
        internal static bool Validate(Transaction tr,string s)
        {
            if (s == null)
                return false;
            var ss = s.Split(':','=');
            if (ss.Length < 3)
                return false;
            var tb = tr.role.objects[Unq(ss[0])] as Table;
            var rc = tb?.tableRows[Unq(ss[1])];
            return (rc?.ppos == Unq(ss[2])) == true;
        }
        /// <summary>
        /// Add this rvv to a list of strings
        /// </summary>
        /// <param name="rs"></param>
        internal void Add(ref ATree<string,bool> rs)
        {
            ATree<string,bool>.Add(ref rs, ToString(), true);
        }
        /// <summary>
        /// helper: parse a long
        /// </summary>
        /// <param name="s">a position component from the rvv</param>
        /// <returns>the long value</returns>
        static long Unq(string s)
        {
            if (s.Length == 0)
                return 0;
            return long.TryParse(s,out long r)?r:0;
        }
        /// <summary>
        /// helper: add a position, using '4 etc for uncommitted "position"s
        /// </summary>
        /// <param name="sb">the string builder</param>
        /// <param name="p">the long</param>
        void Add(StringBuilder sb,long p)
        {
            sb.Append(':');
            if (p > t0)
            {
                sb.Append('\''); sb.Append(p - t0);
            }
            else
                sb.Append(p);
        }
        /// <summary>
        /// String version of an rvv
        /// </summary>
        /// <returns>the string version</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(tbpos);
            sb.Append(':');
            Add(sb, def);
            sb.Append('=');
            Add(sb, off);
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
            var c = tbpos.CompareTo(that.tbpos);
            if (c == 0)
                c = def.CompareTo(that.def);
            if (c == 0)
                c = off.CompareTo(that.off);
            return c;
        }

    }
    /// <summary>
    /// ETags are as in RFC 7232, and should be supplied along with the results of an HTTP GET.
    /// They should be strings that validate a cached value held by the client.
    /// This class is a list of cookies that will be used to create an ETag when required.
    /// So we look to see what has changed since then.
    /// </summary>
    internal class ETag
    {
        public readonly long dfpos; // a record defining position or 0
        public readonly long ptrans;  // a transaction log position
        public readonly ETag next = null; // we allow them to be chained
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="off"></param>
        /// <param name="n"></param>
        ETag(long def, long off,ETag n = null)
        { dfpos = def; ptrans = off; next = n; }
        /// <summary>
        /// Convert an RVV to an ETag
        /// </summary>
        /// <param name="rs"></param>
        /// <returns></returns>
        public static ETag Make(Rvv rs)
        {
            return new ETag(rs.def, rs.off);
        }
        /// <summary>
        /// Merge two ETags
        /// </summary>
        /// <param name="et"></param>
        /// <param name="to"></param>
        /// <returns></returns>
        public static ETag Add(ETag et,ETag to)
        {
            for (; et != null; et = et.next)
                to = new ETag(et.dfpos, et.ptrans, to);
            return to;
        }
        /// <summary>
        /// Generate an ETag string from this data
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var s = dfpos + ":" + ptrans;
            if (next != null)
                s += "," + next.ToString();
            return s;
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
