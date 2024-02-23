using System.Text;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

/// <summary>
/// Everything in the Common namespace is Immutable and Shareabl
/// </summary>
namespace Pyrrho.Common
{
    /// <summary>
    /// Sqlx enumerates the tokens of SQL2011, mostly defined in the standard
    /// The keys is only roughly alphabetic
    /// </summary>
    public enum Sqlx
    {
        Null = 0,
        // reserved words (not case sensitive) SQL2011 vol2, vol4, vol14 + // entries
        // 3 alphabetical sequences: reserved words, token types, and non-reserved words
        ///===================RESERVED WORDS=====================
        // last reserved word must be YEAR (mentioned in code below); XML is a special case
        ABS = 1,
        ABSENT = 2, // GQL
        ACOS = 3,
        ALL = 4,
        ALL_DIFFERENT = 5, // GQL
        ALLOICATE = 6,
        ALTER = 7,
        AND = 8,
        ANY = 9,
        ANY_VALUE = 10,
        ARE = 12, 
        ARRAY = 11, // must be 11
        ARRAY_AG = 13,
        ARRAY_MAX_CARDINALITY = 14,
        AS = 15,
        ASC = 16,  // GQL
        ASCENDING = 17,  // GQL
        ASENSITIVE = 18,
        ASIN = 19,
        ASYMMETRIC = 20,
        AT = 21,
        ATAN = 22,
        ATOMIC = 23,
        AUTHORIZATION = 24,
        AVG = 25, //
        BEGIN = 26,
        BOOLEAN = 27, // must be 27
        BEGIN_FRAME = 28,
        BEGIN_PARTITION = 29,
        BETWEEN = 30,
        BIG = 31,  // GQL
        BIGINT = 32,
        BINARY = 33,
        BLOB = 34,
        BOOL = 35, // BOOLEAN see 27
        BOTH = 36,
        CHAR = 37, // must be 37: see also CHARLITERAL for literal 
        BTRIM = 38,
        BY = 39,
        CLOB = 40, // must be 40
        BYTE_LENGTH = 41,
        CALL = 42,
        CALLED = 43,
        CARDINALITY = 44,
        CASCADED = 45,
        CASE = 46,
        CAST = 47,
        CEIL = 48,
        CEILING = 49, // CHAR see 37
        CHAR_LENGTH = 50,
        CHARACTER = 51,
        CHARACTER_LENGTH = 52,
        CHARACTERISTICS = 53, // GQL
        CHECK = 54, // CLOB see 40
        CLOSE = 55,
        COALESCE = 56,
        COLLATE = 57,
        COLLECT = 58,
        COLLECT_LIST = 59, // GQL
        COLUMN = 60,
        COMMIT = 61,
        CONDITION = 62,
        CONNECT = 63,
        CONSTRAINT = 64,
        CURSOR = 65, // must be 65
        CONTAINS = 66,
        DATE = 67, // must be 67	
        CONVERT = 68,
        COPY = 69, // GQL
        CORR = 70,
        CORRESPONDING = 71,
        COS = 72,
        COSH = 73,
        COT = 74, // GQL
        COUNT = 75, //
        COVAR_POP = 76,
        COVAR_SAMP = 77,
        CREATE = 78,
        CROSS = 79,
        CUBE = 80,
        CUME_DIST = 81,
        CURRENT = 82,
        CURRENT_CATALOG = 83,
        CURRENT_DATE = 84,
        CURRENT_DEFAULT_TRANSFORM_GROUP = 85,
        CURRENT_GRAPH = 86, // GQL
        CURRENT_PATH = 87,
        CURRENT_PROPERTY_GRAPH = 88, // GQL
        CURRENT_ROLE = 89,
        CURRENT_ROW = 90,
        CURRENT_SCHEMA = 91,
        CURRENT_TIME = 92,
        CURRENT_TIMESTAMP = 93,
        CURRENT_TRANSFORM_GROUP_FOR_TYPE = 94,
        CURRENT_USER = 95, // CURSOR see 65
        CYCLE = 96, // DATE see 67
        DATETIME = 97, // GQL
        DAY = 98,
        DEALLOCATE = 99,
        DEC = 100,
        DECFLOAT = 101,
        DECIMAL = 102,
        DECLARE = 103,
        DEFAULT = 104,
        DEFINE = 105,
        DEGREES = 106, // GQL
        DELETE = 107,
        DENSE_RANK = 108,
        DEREF = 109,
        DESC = 110, // GQL
        DESCENDING = 111, // GQL
        DESCRIBE = 112,
        DETACH = 113, // GQL
        DETERMINISTIC = 114,
        DISCONNECT = 115,
        DISTINCT = 116,
        DO = 117, // from vol 4
        DOCARRAY = 118, // Pyrrho 5.1
        DOCUMENT = 119, // Pyrrho 5.1
        DOUBLE = 120,
        DROP = 121,
        DURATION = 122, // GQL
        DURATION_BETWEEN = 123, // GQL
        EACH = 124,
        ELEMENT = 125,
        ELEMENT_ID = 126, // GQL
        ELSE = 127,
        ELSEIF = 128, // from vol 4
        EMPTY = 129,
        END = 130,
        END_EXEC = 131, // misprinted in SQL2023 as END-EXEC
        END_FRAME = 132,
        END_PARTITION = 133,
        EOF = 134,	// Pyrrho 0.1
        INTEGER = 135, // must be 135
        INT = 136,  // must be 136 deprecated: see also INTEGERLITERAL
        INTERVAL0 = 137,  // must be 137 (old version of INTERVAL)
        EQUALS = 138,
        ESCAPE = 139,
        EVERY = 140,
        EXCEPT = 141,
        EXEC = 142,
        EXECUTE = 143,
        EXISTS = 144,
        EXIT = 145, // from vol 4
        EXP = 146,
        EXTERNAL = 147,
        EXTRACT = 148,
        FALSE = 149,
        FETCH = 150,
        FILTER = 151,
        INTERVAL = 152, // must be 152 see also INTERVAL0
        FINISH = 153, // GQL
        FIRST_VALUE = 154,
        FLOAT = 155,
        FLOAT16 = 156, // GQL
        FLOAT32 = 157, // GQL
        FLOAT64 = 158, // GQL
        FLOAT128 = 159, // GQL
        FLOAT256 = 160, // GQL
        FLOOR = 161,
        FOR = 162,
        FOREIGN = 163,
        FRAME_ROW = 164,
        FREE = 165,
        FROM = 166,
        FULL = 167,
        MULTISET = 168, // must be 168
        FUNCTION = 169,
        FUSION = 170,
        NCHAR = 171, // must be 171	
        NCLOB = 172, // must be 172
        GET = 173,
        GLOBAL = 174,
        GRANT = 175,
        GREATEST = 176,
        NULL = 177, // must be 177
        GROUP = 178,
        NUMERIC = 179, // must be 179
        GROUPING = 180,
        GROUPS = 181,
        HANDLER = 182, // vol 4 
        HAVING = 183,
        HOME_GRAPH = 184, // GQL
        HOME_PROPERTY_GRAPH = 185, // GQL
        HOME_SCHEMA = 186, // GQL
        HOLD = 187,
        HOUR = 188,
        IDENTITY = 189,
        IF = 190,  // vol 4
        IMPLIES = 191, // GQL
        IN = 192,
        INDICATOR = 193,
        INITIAL = 194,
        INNER = 195,
        INOUT = 196,
        INSENSITIVE = 197,
        INSERT = 198, // INT is 136, INTEGER is 135
        REAL0 = 199, // must be 199, previous version of REAL
        INT8 = 200, // GQL
        INT16 = 201, // GQL
        INT32 = 202, // GQL
        REAL = 203, // must be 203, see also REAL0 and REALLITERAL
        INT64 = 204, // GQL
        INT128 = 205, // GQL
        INT256 = 206, // GQL
        INTEGER8 = 207, // GQL
        INTEGER16 = 208, // GQL
        INTEGER32 = 209, // GQL
        INTEGER64 = 210, // GQL
        INTEGER128 = 211, // GQL
        INTEGER256 = 212, // GQL
        INTERSECT = 213,
        INTERSECTION = 214, // INTERVAL is 152
        INTO = 215,
        IS = 216,
        ITERATE = 217, // vol 4
        PASSWORD = 218, // must be 218, Pyrrho v5
        JOIN = 219,
        JSON = 220,
        JSON_ARRAY = 221,
        JSON_ARRAYAGG = 222,
        JSON_EXISTS = 223,
        JSON_OBJECT = 224,
        JSON_OBJECTAGG = 225,
        JSON_QUERY = 226,
        JSON_SCALAR = 227,
        JSON_SERIALIZE = 228,
        JSON_TABLE = 229,
        JSON_TABLE_PRIMITIVE = 230,
        JSON_VALUE = 231,
        LAG = 232,
        LANGUAGE = 233,
        LARGE = 234,
        LAST_DATA = 235, // Pyrrho v7
        LAST_VALUE = 236,
        LATERAL = 237,
        LEAD = 238,
        LEADING = 239,
        LEAST = 240,
        LEAVE = 241, // vol 4
        LEFT = 242,
        LET = 243, // GQL
        LIKE = 244,
        LIKE_REGEX = 245,
        LIMIT = 246, // GQL
        LIST = 247, // GQL
        LISTAGG = 248,
        LN = 249,
        LOCAL = 250,
        LOCAL_DATETIME = 251, // GQL
        LOCAL_TIME = 252, // GQL
        LOCAL_TIMESTAMP = 253, // GQL
        LOCALTIME = 254,
        SET = 255,  // must be 255
        LOCALTIMESTAMP = 256,
        TIME = 257, // must be 257
        TIMESTAMP = 258, // must be 258
        LOG = 259,
        LOG10 = 260,
        LOOP = 261,  // vol 4
        LOWER = 262,
        LPAD = 263,
        LTRIM = 264,
        MATCH = 265,
        MATCH_NUMBER = 266,
        TYPE = 267, // must be 267 but is not a reserved word
        MATCH_RECOGNIZE = 268,
        MATCHES = 269,
        MAX = 270,
        MEMBER = 271,
        MERGE = 272,
        METHOD = 273,
        MIN = 274,
        MINUTE = 275,
        MOD = 276,
        MODIFIES = 277,
        MODULE = 278,
        MONTH = 279,	 // MULTISET is 168
        NATIONAL = 280,
        NATURAL = 281, // NCHAR 171 NCLOB 172
        NEW = 282,
        NO = 283,
        NODETACH = 284, // GQL
        NONE = 285,
        NORMALIZE = 286,
        NOT = 287,
        NTH_VALUE = 288,
        NTILE = 289, // NULL is 177
        NULLIF = 290, 
        NULLS = 291, // NUMERIC 179, see also NUMERICLITERAL
        OBJECT = 292,
        OCCURRENCES_REGEX = 293, // alphabetical sequence is a misprint in SQL2008
        OCTET_LENGTH = 294,
        OF = 295,
        OFFSET = 296,
        TABLE = 297, // must be 297
        OLD = 298,
        OMIT = 299,
        ON = 300,
        ONE = 301,
        ONLY = 302,
        OPEN = 303,
        OPTIONAL = 304, // GQL
        OR = 305,
        ORDER = 306,
        OTHERWISE = 307, // GQL
        OUT = 308,
        OUTER = 309,
        OVER = 310,
        OVERLAPS = 311,
        OVERLAY = 312,
        PARAMETER = 313,
        PARAMETERS = 314,
        PARTITION = 315,
        PATH = 316, // GQL
        PATH_LENGTH = 317, // GQL
        PATHS = 318, // GQL
        PATTERN = 319,
        PER = 320,
        PERCENT = 321,
        PERCENT_RANK = 322,
        PERCENTILE_CONT = 323,
        PERCENTILE_DISC = 324,
        PERIOD = 325,
        PORTION = 326,
        POSITION = 327,
        POSITION_REGEX = 328,
        POWER = 329,
        PRECEDES = 330,
        PRECISION = 331,
        PREPARE = 332,
        PRIMARY = 333,
        PROCEDURE = 334,
        PROPERTY_EXISTS = 335,
        PTF = 336,
        RADIANS = 337, // GQL
        RANGE = 338,
        RANK = 339,
        READS = 340,    // REAL 203 (previously 199 see REAL0)
        RECORD = 341, // GQL
        RECURSIVE = 342,
        REF = 343,
        REFERENCES = 344,
        REFERENCING = 345,
        RELEASE = 346,
        REMOVE = 347, // GQL 
        REPEAT = 348, // vol 4
        REPLACE = 349, // GQL
        RESET = 350, // GQL
        RESIGNAL = 351, // vol 4
        RESULT = 352,
        RETURN = 353,
        RETURNS = 354,
        REVOKE = 355,
        RIGHT = 356,
        ROLLBACK = 357,
        ROW = 358,
        ROW_NUMBER = 359,
        ROWS = 360,
        RPAD = 361,
        RTRIM = 362,
        RUNNING = 363,
        SAME = 364, // GQL
        SAVEPOINT = 365,
        SCHEMA = 366, // GQL
        SCOPE = 367,
        SCROLL = 368,
        SEARCH = 369,
        SECOND = 370,
        SELECT = 371,
        SENSITIVE = 372,  
        SESSION = 373, // GQL
        SESSION_USER = 374, // SET is 255
        SHOW = 375,
        SIGNAL = 376, //vol 4
        SIGNED = 377, // GQL
        SIMILAR = 378, 
        SIN = 379, 
        SINH = 380, 
        SIZE = 381, 
        SKIP = 382, 
        SMALL = 383,
        SMALLINT = 384,
        SOME = 385,
        SPECIFIC = 386,
        SPECIFICTYPE = 387,
        SQL = 388,
        SQLEXCEPTION = 389,
        SQLSTATE = 390,
        SQLWARNING = 391,
        SQRT = 392,
        START = 393,
        STATIC = 394,
        STDDEV_POP = 395,
        STDDEV_SAMP = 396,
        STRING = 397, // GQL
        SUBMULTISET = 398,
        SUBSET = 399,
        SUBSTRING = 400, //
        SUBSTRING_REGEX = 401,
        SUCCEEDS = 402,
        SUM = 403, //
        SYMMETRIC = 404,
        SYSTEM = 405,
        SYSTEM_TIME = 406,
        SYSTEM_USER = 407,  // TABLE is 297
        TABLESAMPLE = 408,
        TAN = 409,
        TANH = 410,
        THEN = 411,  // TIME is 257, TIMESTAMP is 258	
        TIMEZONE_HOUR = 412,
        TIMEZONE_MINUTE = 413,
        TO = 414,
        TRAILING = 415,
        TRANSLATE = 416,
        TRANSLATE_REGEX = 417,
        TRANSLATION = 418,
        TREAT = 419,
        TRIGGER = 420,
        TRIM = 421,
        TRIM_ARRAY = 422,
        TRUE = 423,
        TRUNCATE = 424,
        TYPED = 425, // GQL
        UBIGINT = 426, // GQL
        UESCAPE = 427,
        UINT = 428, // GQL
        UINT8 = 429, // GQL
        UINT16 = 430, // GQL
        UINT32 = 431, //GQL
        UINT64 = 432, // GQL
        UINT128 = 433, // GQL
        UINT256 = 434, // GQL
        UNION = 435,
        UNIQUE = 436,
        UNKNOWN = 437,
        UNNEST = 438,
        UNTIL = 439, // vol 4
        UPDATE = 440,
        UPPER = 441, //
        USE = 442, // GQL
        USER = 443,
        USING = 444,
        USMALLINT = 445,
        VALUE = 446,
        VALUE_OF = 447,
        VALUES = 448,
        VAR_POP = 449,
        VAR_SAMP = 450,
        VARBINARY = 451,
        VARCHAR = 452,
        VARIABLE = 453,
        VARYING = 454,
        VERSIONING = 455,
        WHEN = 456,
        WHENEVER = 457,
        WHERE = 458,
        WHILE = 459, // vol 4
        WIDTH_BUCKET = 460,
        EDGETYPE = 461, // Metadata 7.03 must be 461
        WINDOW = 462,
        WITH = 463,
        WITHIN = 464,
        WITHOUT = 465,
        XOR = 466, // GQL
        YEAR = 467,	
        YIELD = 468, // GQL
        ZONED = 469, // GQL
        ZONED_DATETIME = 470, //GQL 
        ZONED_TIME = 471, // GQL last reserved word
        //====================TOKEN TYPES=====================
        AMPERSAND = 472, // for GQL label expression
        ARROW = 473, // ]-> GQL bracket right arrow 
        ARROWL = 474, // <- GQL left arrow
        ARROWLTILDE = 475, // <~ GQL left arrow tilde
        ARROWR = 476, // -> GQL right arrow
        ARROWRTILDE = 477, // ~> GQL tilde right arrow
        ARROWTILDE = 478, // ]~> GQL bracket tilde right arrow
        ARROWBASE = 479, // -[ GQL minus left bracket
        ARROWBASETILDE = 480, // ~[ GQL tilde left bracket
        BLOBLITERAL = 481, // 
        BOOLEANLITERAL = 482,
        CHARLITERAL = 483, //
        COLON = 484, // :
        COMMA = 485,  // ,        
        CONCATENATE = 486, // ||        
        DIVIDE = 487, // /        
        DOT = 488, // . 5.2 was STOP
        DOUBLEARROW = 489, // => GQL right double arrow
        DOUBLECOLON = 490, // ::
        DOUBLEPERIOD = 491, // ..
        EQL = 492,  // =
        EXCLAMATION = 493, // !  GQL
        GEQ = 494, // >=    
        GTR = 495, // >    
        Id = 496, // identifier
        INTEGERLITERAL = 497, // Pyrrho
        LBRACE = 498, // {
        LBRACK = 499, // [
        LEQ = 500, // <=
        LPAREN = 501, // (
        LSS = 502, // <
        MINUS = 503, // -
        NEQ = 504, // <>
        NUMERICLITERAL = 505, // 
        PLUS = 506, // + 
        QMARK = 507, // ?
        RARROW = 508, // <-[ GQL left arrow bracket
        RARROWTILDE = 509, // <~[ GQL left arrow tilde bracket
        RARROWBASE = 510, // ]- GQL right bracket minus
        RBRACE = 511, // } 
        RBRACK = 512, // ] 
        RBRACKTILDE = 513, // ]~
        RDFDATETIME = 514, //
        RDFLITERAL = 515, // 
        RDFTYPE = 516, // Pyrrho 7.0
        REALLITERAL = 517, //
        RPAREN = 518, // ) 
        SEMICOLON = 519, // ; 
        TIMES = 520, // *
        VBAR = 521, // | 
        VPLUS = 522, // |+| GQL multiset alternation operator
        //=========================NON-RESERVED WORDS================
        A = 523, // first non-reserved word
        ABSOLUTE = 524,
        ACTION = 525,
        ACYCLIC = 526,
        ADA = 527,
        ADD = 528,
        ADMIN = 529,
        AFTER = 530,
        ALWAYS = 531,
        APPLICATION = 532, // Pyrrho 4.6
        ARRIVING = 533, // Pyrrho Metadata 7.07
        NODETYPE = 534, // Metadata 7.03 must be 534
        ASSERTION = 535,
        ATTRIBUTE = 536,
        ATTRIBUTES = 537,
        BEFORE = 538,
        BERNOULLI = 539,
        BREADTH = 540,
        BREAK = 541, // Pyrrho
        C = 542,
        CAPTION = 543, // Pyrrho 4.5
        CASCADE = 544,
        CATALOG = 545,
        CATALOG_NAME = 546,
        CHAIN = 547,
        CHARACTER_SET_CATALOG = 548,
        CHARACTER_SET_NAME = 549,
        CHARACTER_SET_SCHEMA = 550,
        CHARACTERS = 551,
        CLASS_ORIGIN = 552,
        COBOL = 553,
        COLLATION = 554,
        COLLATION_CATALOG = 555,
        COLLATION_NAME = 556,
        COLLATION_SCHEMA = 557,
        COLUMN_NAME = 558,
        COMMAND_FUNCTION = 559,
        COMMAND_FUNCTION_CODE = 560,
        COMMITTED = 561,
        CONDITION_NUMBER = 562,
        CONNECTION = 563,
        CONNECTION_NAME = 564,
        CONSTRAINT_CATALOG = 565,
        CONSTRAINT_NAME = 566,
        CONSTRAINT_SCHEMA = 567,
        CONSTRAINTS = 568,
        CONSTRUCTOR = 569,
        CONTENT = 570,
        CONTINUE = 571,
        CSV = 572, // Pyrrho 5.5
        CURATED = 573, // Pyrrho
        CURSOR_NAME = 574,
        DATA = 575,
        DATABASE = 576, // Pyrrho
        DATETIME_INTERVAL_CODE = 577,
        DATETIME_INTERVAL_PRECISION = 578,
        DEFAULTS = 579,
        DEFERRABLE = 580,
        DEFERRED = 581,
        DEFINED = 582,
        DEFINER = 583,
        DEGREE = 584,
        DEPTH = 585,
        DERIVED = 586,
        DESCRIPTOR = 587,
        DIAGNOSTICS = 588,
        DISPATCH = 589,
        DOMAIN = 590,
        DYNAMIC_FUNCTION = 591,
        DYNAMIC_FUNCTION_CODE = 592,
        EDGE = 593, // 7.03 EDGETYPE is 461
        ELEMENTID = 594, // Pyrrho 7.05
        ENFORCED = 595,
        ENTITY = 596, // Pyrrho 4.5
        ETAG = 597, // Pyrrho Metadata 7.0
        EXCLUDE = 598,
        EXCLUDING = 599,
        FINAL = 600,
        FIRST = 601,
        FLAG = 602,
        FOLLOWING = 603,
        FORTRAN = 604,
        FOUND = 605,
        G = 606,
        GENERAL = 607,
        GENERATED = 608,
        GO = 609,
        GOTO = 610,
        GRANTED = 611,
        GRAPH = 612, //7.03
        HIERARCHY = 613,
        HISTOGRAM = 614, // Pyrrho 4.5
        HTTPDATE = 615, // Pyrrho 7 RFC 7231
        ID = 616, // Distinguished from the token type Id from Pyrrho 7.05
        IGNORE = 617,
        IMMEDIATE = 618,
        IMMEDIATELY = 619,
        IMPLEMENTATION = 620,
        INCLUDING = 621,
        INCREMENT = 622,
        INITIALLY = 623,
        INPUT = 624,
        INSTANCE = 625,
        INSTANTIABLE = 626,
        INSTEAD = 627,
        INVERTS = 628, // Pyrrho Metadata 5.7
        INVOKER = 629,
        IRI = 630, // Pyrrho 7
        ISOLATION = 631,
        K = 632,
        KEY = 633,
        KEY_MEMBER = 634,
        KEY_TYPE = 635,
        LABELS = 636, // Pyrrho 7.05
        LAST = 637,
        LEAVING = 638, // Pyrrho Metadata 7.07
        LEGEND = 639, // Pyrrho Metadata 4.8
        LENGTH = 640,
        LEVEL = 641,
        LINE = 642, // Pyrrho 4.5
        LOCATOR = 643,
        METADATA = 644, // Pyrrho 7 must be 514
        M = 645,
        MAP = 646,
        MATCHED = 647,
        MAXVALUE = 648,
        MESSAGE_LENGTH = 649,
        MESSAGE_OCTET_LENGTH = 650,
        MESSAGE_TEXT = 651,
        MILLI = 652, // Pyrrho 7
        MIME = 653, // Pyrrho 7
        MINVALUE = 654,
        MONOTONIC = 655, // Pyrrho 5.7
        MORE = 656,
        MULTIPLICITY = 657, // Pyrrho 7.03
        MUMPS = 658,
        NAME = 659,
        NAMES = 660,
        NESTING = 661,
        NEXT = 662,
        NFC = 663,
        NFD = 665,
        NFKC = 666,
        NFKD = 667,
        NODE = 668, // 7.03 NODETYPE is 534
        NORMALIZED = 669,
        NULLABLE = 670,
        NUMBER = 671,
        OCCURRENCE = 672,
        OCTETS = 673,
        OPTION = 674,
        OPTIONS = 675,
        ORDERING = 676,
        ORDINALITY = 677,
        OTHERS = 678,
        OUTPUT = 679,
        OVERRIDING = 680,
        OWNER = 681, // Pyrrho
        P = 682,
        PAD = 683,
        PARAMETER_MODE = 684,
        PARAMETER_NAME = 685,
        PARAMETER_ORDINAL_POSITION = 686,
        PARAMETER_SPECIFIC_CATALOG = 687,
        PARAMETER_SPECIFIC_NAME = 688,
        PARAMETER_SPECIFIC_SCHEMA = 689,
        PARTIAL = 690,
        PASCAL = 691,
        PIE = 692, // Pyrrho 4.5
        PLACING = 693,
        PL1 = 694,
        POINTS = 695, // Pyrrho 4.5
        PRECEDING = 696,
        PREFIX = 697, // Pyrrho 7.01
        PRESERVE = 698,
        PRIOR = 699,
        PRIVILEGES = 700,
        PROFILING = 701, // Pyrrho
        PUBLIC = 702,
        READ = 703,
        REFERRED = 704, // 5.2
        REFERS = 705, // 5.2
        RELATIVE = 706,
        REPEATABLE = 707,
        RESPECT = 708,
        RESTART = 709,
        RESTRICT = 710,
        RETURNED_CARDINALITY = 711,
        RETURNED_LENGTH = 712,
        RETURNED_OCTET_LENGTH = 713,
        RETURNED_SQLSTATE = 714,
        ROLE = 715,
        ROUTINE = 716,
        ROUTINE_CATALOG = 717,
        ROUTINE_NAME = 718,
        ROUTINE_SCHEMA = 719,
        ROW_COUNT = 720,
        SCALE = 721,
        SCHEMA_NAME = 722,
        SCOPE_CATALOG = 723,
        SCOPE_NAME = 724,
        SCOPE_SCHEMA = 725,
        SECTION = 726,
        SECURITY = 727,
        SELF = 728,
        SEQUENCE = 729,
        SERIALIZABLE = 730,
        SERVER_NAME = 731,
        SETS = 732,
        SHORTEST = 733,
        SHORTESTPATH = 734,
        SIMPLE = 735,
        SOURCE = 736,
        SPACE = 737,
        SPECIFIC_NAME = 738,
        SQLAGENT = 739, // Pyrrho 7
        STANDALONE = 740, // vol 14
        STATE = 741,
        STATEMENT = 742,
        STRUCTURE = 743,
        STYLE = 744,
        SUBCLASS_ORIGIN = 745,
        SUFFIX = 746, // Pyrrho 7.01
        T = 747,
        TABLE_NAME = 748,
        TEMPORARY = 749,
        TIES = 750,
        TIMEOUT = 751, // Pyrrho
        TOP_LEVEL_COUNT = 752,
        TRAIL = 753,
        TRANSACTION = 754,
        TRANSACTION_ACTIVE = 755,
        TRANSACTIONS_COMMITTED = 756,
        TRANSACTIONS_ROLLED_BACK = 757,
        TRANSFORM = 758,
        TRANSFORMS = 759,
        TRIGGER_CATALOG = 760,
        TRIGGER_NAME = 761,
        TRIGGER_SCHEMA = 762, // TYPE  is 267 but is not a reserved word
        TRUNCATING = 763, // Pyrrho 7.07
        TYPE_URI = 764, // Pyrrho
        UNBOUNDED = 765,
        UNCOMMITTED = 766,
        UNDER = 767,
        UNDO = 768,
        UNNAMED = 769,
        URL = 770,  // Pyrrho 7
        USAGE = 771,
        USER_DEFINED_TYPE_CATALOG = 772,
        USER_DEFINED_TYPE_CODE = 773,
        USER_DEFINED_TYPE_NAME = 774,
        USER_DEFINED_TYPE_SCHEMA = 775,
        VIEW = 776,
        WORK = 777,
        WRITE = 778,
        X = 779, // Pyrrho 4.5
        Y = 780, // Pyrrho 4.5
        ZONE = 781
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
