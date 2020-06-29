using System;
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
        HTTP = 140, // Pyrrho 4.5
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
        INTERVAL = 152, // must be 152 see also INTERVAL0
        IS = 153,
        ITERATE = 154, // vol 4
        JOIN = 155,
        LAG = 156,
        LANGUAGE = 157,
        LARGE = 158,
        LAST_VALUE = 159,
        LATERAL = 160,
        LEADING = 161,
        LEAVE = 162, // vol 4
        LEFT = 163,
        LIKE = 164,
#if SIMILAR
        LIKE_REGEX =165,
#endif
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
        MAX = 176, //
        NULL = 177, // must be 177
        MEMBER = 178,
        NUMERIC = 179, // must be 179
        MERGE = 180,
        METHOD = 181,
        MIN = 182, //
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
#if OLAP
        NTH_VALUE = 195,
        NTILE = 196, // NULL 177
#endif
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
#if OLAP
        PERCENT_RANK =220,
        PERCENTILE_CONT =221,
        PERCENTILE_DISC = 222,
#endif
        PERIOD = 223,
        PORTION = 224,
        POSITION = 225,
#if OLAP || MONGO || SIMILAR
        POSITION_REGEX =226,
#endif
        POWER = 227,
        PRECEDES = 228,
        PRECISION = 229,
        PREPARE = 230,
        PRIMARY = 231,
        PROCEDURE = 232,
        RANGE = 233,
        RANK = 234,
        READS = 235,    // REAL 203 (previously 199 see REAL0)
        RECURSIVE = 236,
        REF = 237,
        REFERENCES = 238,
        REFERENCING = 239,
#if OLAP
        REGR_AVGX =240, 
        REGR_AVGY =241, 
        REGR_COUNT =242, 
        REGR_INTERCEPT =243,
        REGR_R2 =244, 
        REGR_SLOPE =245,
        REGR_SXX =246, 
        REGR_SXY =247, 
        REGR_SYY =248,
#endif
        RELEASE = 249,
        REPEAT = 250, // vol 4
        RESIGNAL = 251, // vol 4
        RESULT = 252,
        RETURN = 253,
        RETURNS = 254,
        REVOKE = 255,
        RIGHT = 256,
        TIME = 257, // must be 257
        TIMESTAMP = 258, // must be 258
        ROLLBACK = 259,
#if OLAP
        ROLLUP =260,
#endif
        ROW = 261,
        ROW_NUMBER = 262,
        ROWS = 263,
        SAVEPOINT = 264,
        SCOPE = 265,
        SCROLL = 266,
        TYPE = 267, // must be 267 but is not a reserved word
        SEARCH = 268,
        SECOND = 269,
        SELECT = 270,
        SENSITIVE = 271,    // has a different usage in Pyrrho
        SESSION_USER = 272,
        SET = 273,
        SIGNAL = 274, //vol 4
#if SIMILAR
        SIMILAR =275,
#endif
        SMALLINT = 276,
        SOME = 277,
        SPECIFIC = 278,
        SPECIFICTYPE = 279,
        SQL = 280,
        SQLEXCEPTION = 281,
        SQLSTATE = 282,
        SQLWARNING = 283,
        SQRT = 284,
        START = 285,
        STATIC = 286,
        STDDEV_POP = 287,
        STDDEV_SAMP = 288,
        SUBMULTISET = 289,
        SUBSTRING = 290, //
        SUBSTRING_REGEX = 291,
        SUCCEEDS = 292,
        SUM = 293, //
        SYMMETRIC = 294,
        SYSTEM = 295,
        SYSTEM_TIME = 296,
        TABLE = 297, // must be 297
        SYSTEM_USER = 298,  // TABLE is 297
        TABLESAMPLE = 299,
        THEN = 300,  // TIME is 257, TIMESTAMP is 258	
        TIMEZONE_HOUR = 301,
        TIMEZONE_MINUTE = 302,
        TO = 303,
        TRAILING = 304,
        TRANSLATE = 305,
#if SIMILAR
        TRANSLATE_REGEX =306,
#endif
        TRANSLATION = 307,
        TREAT = 308,
        TRIGGER = 309,
        TRIM = 310,
        TRIM_ARRAY = 311,
        TRUE = 312,
        TRUNCATE = 313,
        UESCAPE = 314,
        UNION = 315,
        UNIQUE = 316,
        UNKNOWN = 317,
        UNNEST = 318,
        UNTIL = 319, // vol 4
        UPDATE = 320,
        UPPER = 321, //
        USER = 322,
        USING = 323,
        VALUE = 324,
        VALUE_OF = 325,
        VALUES = 326,
#if OLAP
        VAR_POP =327,
        VAR_SAMP =328,
#endif
        VARBINARY = 329,
        VARCHAR = 330,
        VARYING = 331,
        VERSIONING = 332,
        WHEN = 333,
        WHENEVER = 334,
        WHERE = 335,
        WHILE = 336, // vol 4
#if OLAP
        WIDTH_BUCKET =337,
#endif
        WINDOW = 338,
        WITH = 339,
        WITHIN = 340,
        WITHOUT = 341, // XML is 356 vol 14
        XMLAGG = 342, // Basic XML stuff + XMLAgg vol 14
        XMLATTRIBUTES = 343,
        XMLBINARY = 344,
        XMLCAST = 345,
        XMLCOMMENT = 346,
        XMLCONCAT = 347,
        XMLDOCUMENT = 348,
        XMLELEMENT = 349,
        XMLEXISTS = 350,
        XMLFOREST = 351,
        XMLITERATE = 352,
        XMLNAMESPACES = 353,
        XMLPARSE = 354,
        XMLPI = 355,
        XML = 356, // must be 356 
        XMLQUERY = 357,
        XMLSERIALIZE = 358,
        XMLTABLE = 359,
        XMLTEXT = 360,
        XMLVALIDATE = 361,
        YEAR = 362,	// last reserved word
        //====================TOKEN TYPES=====================
        ASSIGNMENT = 363, // := 
        BLOBLITERAL = 364, // 
        BOOLEANLITERAL = 365,
        CHARLITERAL = 366, //
        COLON = 367, // :
        COMMA = 368,  // ,        
        CONCATENATE = 369, // ||        
        DIVIDE = 370, // /        
        DOCUMENTLITERAL = 371, // v5.1
        DOT = 372, // . 5.2 was STOP
        DOUBLECOLON = 373, // ::        
        EMPTY = 374, // []        
        EQL = 375,  // =        
        GEQ = 376, // >=    
        GTR = 377, // >    
        ID = 378, // identifier
        INTEGERLITERAL = 379, // Pyrrho
        LBRACE = 380, // {
        LBRACK = 381, // [
        LEQ = 382, // <=
        LPAREN = 383, // (
        LSS = 384, // <
        MEXCEPT = 385, // 
        MINTERSECT = 386, //
        MINUS = 387, // -
        MUNION = 388, // 
        NEQ = 389, // <>
        NUMERICLITERAL = 390, // 
        PLUS = 391, // + 
        QMARK = 392, // ?
        RBRACE = 393, // } 
        RBRACK = 394, // ] 
        RDFDATETIME = 395, //
        RDFLITERAL = 396, // 
        RDFTYPE = 397, // Pyrrho 7.0
        REALLITERAL = 398, //
        RPAREN = 399, // ) 
        SEMICOLON = 400, // ; 
        TIMES = 401, // *
        VBAR = 402, // | 
        //=========================NON-RESERVED WORDS================
        A = 403, // first non-reserved word
        ABSOLUTE = 404,
        ACTION = 405,
        ADA = 406,
        ADD = 407,
        ADMIN = 408,
        AFTER = 409,
        ALWAYS = 410,
        APPLICATION = 411, // Pyrrho 4.6
        ASC = 412,
        ASSERTION = 413,
        ATTRIBUTE = 414,
        ATTRIBUTES = 415,
        BEFORE = 416,
        BERNOULLI = 417,
        BREADTH = 418,
        BREAK = 419, // Pyrrho
        C = 420,
        CAPTION = 421, // Pyrrho 4.5
        CASCADE = 422,
        CATALOG = 423,
        CATALOG_NAME = 424,
        CHAIN = 425,
        CHARACTER_SET_CATALOG = 426,
        CHARACTER_SET_NAME = 427,
        CHARACTER_SET_SCHEMA = 428,
        CHARACTERISTICS = 429,
        CHARACTERS = 430,
        CLASS_ORIGIN = 431,
        COBOL = 432,
        COLLATION = 433,
        COLLATION_CATALOG = 434,
        COLLATION_NAME = 435,
        COLLATION_SCHEMA = 436,
        COLUMN_NAME = 437,
        COMMAND_FUNCTION = 438,
        COMMAND_FUNCTION_CODE = 439,
        COMMITTED = 440,
        CONDITION_NUMBER = 441,
        CONNECTION = 442,
        CONNECTION_NAME = 443,
        CONSTRAINT_CATALOG = 444,
        CONSTRAINT_NAME = 445,
        CONSTRAINT_SCHEMA = 446,
        CONSTRAINTS = 447,
        CONSTRUCTOR = 448,
        CONTENT = 449,
        CONTINUE = 450,
        CSV = 451, // Pyrrho 5.5
        CURATED = 452, // Pyrrho
        CURSOR_NAME = 453,
        DATA = 454,
        DATABASE = 455, // Pyrrho
        DATETIME_INTERVAL_CODE = 456,
        DATETIME_INTERVAL_PRECISION = 457,
        DEFAULTS = 458,
        DEFERRABLE = 459,
        DEFERRED = 460,
        DEFINED = 461,
        DEFINER = 462,
        DEGREE = 463,
        DEPTH = 464,
        DERIVED = 465,
        DESC = 466,
        DESCRIPTOR = 467,
        DIAGNOSTICS = 468,
        DISPATCH = 469,
        DOMAIN = 470,
        DYNAMIC_FUNCTION = 471,
        DYNAMIC_FUNCTION_CODE = 472,
        ENFORCED = 473,
        ENTITY = 474, // Pyrrho 4.5
        EXCLUDE = 475,
        EXCLUDING = 476,
        FINAL = 477,
        FIRST = 478,
        FLAG = 479, // unused (SIMILAR)
        FOLLOWING = 480,
        FORTRAN = 481,
        FOUND = 482,
        G = 483,
        GENERAL = 484,
        GENERATED = 485,
        GO = 486,
        GOTO = 487,
        GRANTED = 488,
        HIERARCHY = 489,
        HISTOGRAM = 490, // Pyrrho 4.5
        IGNORE = 491,
        IMMEDIATE = 492,
        IMMEDIATELY = 493,
        IMPLEMENTATION = 494,
        INCLUDING = 495,
        INCREMENT = 496,
        INITIALLY = 497,
        INPUT = 498,
        INSTANCE = 499,
        INSTANTIABLE = 500,
        INSTEAD = 501,
        INVERTS = 502, // Pyrrho Metadata 5.7
        INVOKER = 503,
        ISOLATION = 504,
        JSON = 505, // Pyrrho 5.5
        K = 506,
        KEY = 507,
        KEY_MEMBER = 508,
        KEY_TYPE = 509,
        LAST = 510,
        LEGEND = 511, // Pyrrho Metadata 4.8
        LENGTH = 512,
        LEVEL = 513,
        LINE = 514, // Pyrrho 4.5
        LOCATOR = 515,
        M = 516,
        MAP = 517,
        MATCHED = 518,
        MAXVALUE = 519,
        MESSAGE_LENGTH = 520,
        MESSAGE_OCTET_LENGTH = 521,
        MESSAGE_TEXT = 522,
        MINVALUE = 523,
        MONOTONIC = 524, // Pyrrho 5.7
        MORE = 525,
        MUMPS = 526,
        NAME = 527,
        NAMES = 528,
        NESTING = 529,
        NEXT = 530,
        NFC = 531,
        NFD = 532,
        NFKC = 533,
        NFKD = 534,
        NORMALIZED = 535,
        NULLABLE = 536,
        NULLS = 537,
        NUMBER = 538,
        OCCURRENCE = 539,
        OCTETS = 540,
        OPTION = 541,
        OPTIONS = 542,
        ORDERING = 543,
        ORDINALITY = 544,
        OTHERS = 545,
        OUTPUT = 546,
        OVERRIDING = 547,
        OWNER = 548, // Pyrrho
        P = 549,
        PAD = 550,
        PARAMETER_MODE = 551,
        PARAMETER_NAME = 552,
        PARAMETER_ORDINAL_POSITION = 553,
        PARAMETER_SPECIFIC_CATALOG = 554,
        PARAMETER_SPECIFIC_NAME = 555,
        PARAMETER_SPECIFIC_SCHEMA = 556,
        PARTIAL = 557,
        PASCAL = 558,
        PATH = 559,
        PIE = 560, // Pyrrho 4.5
        PLACING = 561,
        PL1 = 562,
        POINTS = 563, // Pyrrho 4.5
        PRECEDING = 564,
        PRESERVE = 565,
        PRIOR = 566,
        PRIVILEGES = 567,
        PROFILING = 568, // Pyrrho
        PROVENANCE = 569, // Pyrrho
        PUBLIC = 570,
        READ = 571,
        REFERRED = 572, // 5.2
        REFERS = 573, // 5.2
#if MONGO || OLAP || SIMILAR
        REGULAR_EXPRESSION = 574, // Pyrrho 5.1
#endif
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
        SIMPLE = 604,
        SIZE = 605,
        SOURCE = 606,
        SPACE = 607,
        SPECIFIC_NAME = 608,
        STANDALONE = 609, // vol 14
        STATE = 610,
        STATEMENT = 611,
        STRUCTURE = 612,
        STYLE = 613,
        SUBCLASS_ORIGIN = 614,
        T = 615,
        TABLE_NAME = 616,
        TEMPORARY = 617,
        TIES = 618,
        TIMEOUT = 619, // Pyrrho
        TOP_LEVEL_COUNT = 620,
        TRANSACTION = 621,
        TRANSACTION_ACTIVE = 622,
        TRANSACTIONS_COMMITTED = 623,
        TRANSACTIONS_ROLLED_BACK = 624,
        TRANSFORM = 625,
        TRANSFORMS = 626,
        TRIGGER_CATALOG = 627,
        TRIGGER_NAME = 628,
        TRIGGER_SCHEMA = 629, // TYPE  is 267 but is not a reserved word
        TYPE_URI = 630, // Pyrrho
        UNBOUNDED = 631,
        UNCOMMITTED = 632,
        UNDER = 633,
        UNDO = 634,
        UNNAMED = 635,
        USAGE = 636,
        USER_DEFINED_TYPE_CATALOG = 637,
        USER_DEFINED_TYPE_CODE = 638,
        USER_DEFINED_TYPE_NAME = 639,
        USER_DEFINED_TYPE_SCHEMA = 640,
        VIEW = 641,
        WORK = 642,
        WRITE = 643,
        X = 644, // Pyrrho 4.5
        Y = 645, // Pyrrho 4.5
        ZONE = 646
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
		DomainRef,  // typedefpos, Integer els, els x data 
		Blob,		// Integer length, length x byte: Opaque binary type (Clob is String)
		Row,		// spec, Integer cols, cols x data
		Multiset,	// Integer els, els x data
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
            ATree<Sqlx, TypedValue>.Add(ref info, k, v??TNull.Value);
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
            return new Rvv(rc.tabledefpos, rc.defpos,rc.defpos);
        }
        internal static Rvv For(Transaction tr,long tp,long rp)
        {
            var rc = (tr.objects[tp] as Table)?.tableRows[rp] as TableRow;
            return (rc != null) ? new Rvv(tp, rp, rc.defpos) : null;
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
            var tb = tr.objects[Unq(ss[0])] as Table;
            var rc = tb?.tableRows[Unq(ss[1])] as TableRow;
            return (rc?.defpos == Unq(ss[2])) == true;
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
