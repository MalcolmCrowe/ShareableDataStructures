using System.Text;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2026
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
    /// Qlx enumerates the tokens of SQL2011, mostly defined in the standard
    /// The keys is only roughly alphabetic
    /// </summary>
    public enum Qlx
    {
        Null = 0,
        // reserved words from GQL ISO 39075 (SQL only words no longer reserved in Pyrrho)
        // 3 alphabetical sequences: reserved words, token types, and non-reserved words
        ///===================GQL RESERVED WORDS=====================
        // last reserved word must be ZONED_TIME
        ABS = 1,
        ACOS = 2,
        ALL = 3,
        ALL_DIFFERENT = 4,
        AND = 5,
        ANY = 6, // ARRAY is 11
        AS = 7,
        ASC = 8,
        ASCENDING = 9,
        ASIN = 10,
        ARRAY = 11, // must be 11
        AT = 12,
        ATAN = 13,
        AVG = 14,
        BIG = 15,
        BIGINT = 16,
        BINARY = 17,
        BOOL = 18,// BOOLEAN see 27
        BOTH = 19,
        BTRIM = 20,
        BY = 21,
        BYTE_LENGTH = 22,
        BYTES = 23,
        CALL = 24,
        CARDINALITY = 25,
        CASE = 26,
        BOOLEAN = 27, // must be 27
        CAST = 28,
        CEIL = 29,
        CEILING = 30, // CHAR see 37 
        CHAR_LENGTH = 31,
        CHARACTER_LENGTH = 32,
        CHARACTERISTICS = 33,
        CLOSE = 34,
        COALESCE = 35,
        COLLECT_LIST = 36,
        CHAR = 37, // must be 37: see also CHARLITERAL for literal 
        COMMIT = 38,
        CONTAINING = 39,
        COPY = 41,
        CLOB = 40, // must be 40 (not in GQL)
        COS = 42,
        COSH = 43,
        COT = 44,
        COUNT = 45,
        CREATE = 46,
        CURRENT_DATE = 47,
        CURRENT_GRAPH = 48,
        CURRENT_PROPERTY_GRAPH = 49,
        CURRENT_SCHEMA = 50,
        CURRENT_TIME = 51,
        CURRENT_TIMESTAMP = 52, // DATE see 67
        DATETIME = 53,
        DAY = 54,
        DEC = 55,
        DECIMAL = 56,
        DEGREES = 57,
        DELETE = 58,
        DESC = 59,
        DESCENDING = 60,
        DETACH = 61,
        DISTINCT = 62,
        DOUBLE = 63,
        DROP = 64,
        CURSOR = 65, // must be 65 (not in GQL)
        DURATION = 66,
        DATE = 67, // must be 67	
        DURATION_BETWEEN = 68,
        ELEMENT_ID = 69,
        ELSE = 70,
        END = 71,
        EXCEPT = 72,
        EXISTS = 73,
        EXP = 74,
        FALSE = 75,
        FILTER = 76,
        FINISH = 77,
        FLOAT = 78,
        FLOAT16 = 79,
        FLOAT32 = 80,
        FLOAT64 = 81,
        FLOAT128 = 82,
        FLOAT256 = 83,
        FLOOR = 84,
        FOR = 85,
        FROM = 86,
        GROUP = 87,
        HAVING = 88,
        HOME_GRAPH = 89,
        HOME_PROPERTY_GRAPH = 90,
        HOME_SCHEMA = 91,
        HOUR = 92,
        IF = 93,
        IMPLIES = 94,
        IN = 95,
        INSERT = 96, // INT is 136, INTEGER is 135
        INT8 = 97,
        INT16 = 98,
        INT32 = 99,
        INT64 = 100,
        INT128 = 101,
        INT256 = 102,
        INTEGER8 = 103,
        INTEGER16 = 104,
        INTEGER32 = 105,
        INTEGER64 = 106,
        INTEGER128 = 107,
        INTEGER256 = 108,
        INTERSECT = 109, // INTERVAL is 152, (INTERVAL0 is 137)
        IS = 110,
        KEY = 111,
        LEADING = 112,
        LEFT = 113,
        LET = 114,
        LIKE = 115,
        LIMIT = 116,
        LIST = 117,
        LN = 118,
        LOCAL = 119,
        LOCAL_DATETIME = 120,
        LOCAL_TIME = 121,
        LOCAL_TIMESTAMP = 122,
        LOG = 123,
        LOG10 = 124,
        LOWER = 125,
        LTRIM = 126,
        MATCH = 127,
        MAX = 128,
        MIN = 129,
        MINUTE = 130,
        MOD = 131,
        MONTH = 132,	 // MULTISET is 168 (not in GQL)
        NEXT = 133,
        NODETACH = 134,
        INTEGER = 135, // must be 135
        INT = 136,  // must be 136 deprecated: see also INTEGERLITERAL
        INTERVAL0 = 137,  // must be 137 (old version of INTERVAL)
        NORMALIZE = 138,
        NOT = 139,
        NOTHING = 140, // NULL is 177
        NULLIF = 141,
        NULLS = 142, // NUMERIC 179, see also NUMERICLITERAL
        OCTET_LENGTH = 143,
        OF = 144,
        OFFSET = 145,
        ON = 146,
        OPTIONAL = 147,
        OR = 148,
        ORDER = 149,
        OTHERWISE = 150,
        PARAMETER = 151,
        INTERVAL = 152, // must be 152 see also INTERVAL0 at 137
        PARAMETERS = 153,
        PATH = 154,
        PATH_LENGTH = 155,
        PATHS = 156,
        PERCENTILE_CONT = 157,
        PERCENTILE_DISC = 158,
        POWER = 159,
        PRECISION = 160,
        PRODUCT = 161,
        PROPERTY_EXISTS = 162,
        RADIANS = 163, // REAL see 203
        RECORD = 164,
        REMOVE = 165,
        REPLACE = 166,
        REQUIRE = 167,
        MULTISET = 168, // must be 168 (not in GQL)
        RESET = 169,
        RETURN = 170,
        NCHAR = 171, // must be 171	(not in GQL)
        NCLOB = 172, // must be 172 (not in GQL)
        RIGHT = 173,
        ROLLBACK = 174,
        RTRIM = 175,
        SAME = 176,
        NULL = 177, // must be 177
        SATISFYING = 178,
        NUMERIC = 179, // must be 179 (not reserved in GQL) see DECIMAL
        SCHEMA = 180,
        SECOND = 181,
        SELECT = 182,
        SESSION = 183,
        SESSION_USER = 184, // SET is 255
        SIGNED = 185,
        SIN = 186,
        SINH = 187,
        SIZE = 188,
        SKIP = 189,
        SMALL = 190,
        SMALLINT = 191,
        SQRT = 192,
        START = 193,
        STDDEV_POP = 194,
        STDDEV_SAMP = 195,
        STRING = 196,
        SUM = 197,
        TAN = 198,
        REAL0 = 199, // must be 199, previous version of REAL
        TANH = 200,
        THEN = 201,  // TIME is 257, TIMESTAMP is 258	
        TRAILING = 202,
        REAL = 203, // must be 203, see also REAL0 and REALLITERAL
        TRIM = 204,
        TRUE = 205,
        TYPED = 206,
        UBIGINT = 207,
        UINT = 208,
        UINT8 = 209,
        UINT16 = 210,
        UINT32 = 211,
        UINT64 = 212,
        UINT128 = 213,
        UINT256 = 214,
        UNION = 215,
        UNIQUE = 216,
        UNKNOWN = 217,
        PASSWORD = 218, // must be 218, Pyrrho v5 (not reserved, deprecated)
        UNSIGNED = 219,
        UPPER = 220,
        USE = 221,
        USMALLINT = 222,
        VALUE = 223,
        VARBINARY = 224,
        VARCHAR = 225,
        VARIABLE = 226,
        VECTOR = 227,
        VECTOR_DIMENSION_COUNT = 228,
        VECTOR_DISTANCE = 229,
        VECTOR_NORM = 230,
        VECTOR_SERIALIZE = 231,
        WHEN = 232,
        WHERE = 233,
        WITH = 234,
        XOR = 235,
        YEAR = 236,
        YIELD = 237,
        ZONED = 238,
        ZONED_DATETIME = 239,
        ZONED_TIME = 240, // last reserved word
        //====================TOKEN TYPES=====================
        AMPERSAND = 241, // for GQL label expression
        ARROW = 242, // ]-> GQL bracket right arrow 
        ARROWL = 243, // <- GQL left arrow
        ARROWLTILDE = 244, // <~ GQL left arrow tilde
        ARROWR = 245, // -> GQL right arrow
        ARROWRTILDE = 246, // ~> GQL tilde right arrow
        ARROWTILDE = 247, // ]~> GQL bracket tilde right arrow
        ARROWBASE = 248, // -[ GQL minus left bracket
        ARROWBASETILDE = 249, // ~[ GQL tilde left bracket
        BLOBLITERAL = 250, // 
        BOOLEANLITERAL = 251,
        CHARLITERAL = 252, //
        COLON = 253, // :
        COMMA = 254,  // ,        
        SET = 255,  // must be 255 (is reserved word)
        CONCATENATE = 256, // ||        
        TIME = 257, // must be 257 (is reserved word)
        TIMESTAMP = 258, // must be 258
        DIVIDE = 259, // /        
        DOTTOKEN = 260, // . 5.2 was STOP
        DOUBLEARROW = 261, // => GQL right double arrow
        DOUBLECOLON = 262, // ::
        DOUBLEPERIOD = 263, // ..
        EQL = 264,  // =
        EXCLAMATION = 265, // !  GQL
        GEQ = 266, // >=    
        TYPE = 267, // must be 267
        GTR = 268, // >    
        Id = 269, // identifier
        INTEGERLITERAL = 270, // Pyrrho
        LBRACE = 271, // {
        LBRACK = 272, // [
        LEQ = 273, // <=
        LPAREN = 274, // (
        LSS = 275, // <
        MINUS = 276, // -
        NEQ = 277, // <>
        NUMERICLITERAL = 278, // 
        PLUS = 279, // + 
        QMARK = 280, // ?
        RARROW = 281, // <-[ GQL left arrow bracket
        RARROWTILDE = 282, // <~[ GQL left arrow tilde bracket
        RARROWBASE = 283, // ]- GQL right bracket minus
        RBRACE = 284, // } 
        RBRACK = 285, // ] 
        RBRACKTILDE = 286, // ]~ GQL right brancket tilde
        RDFDATETIME = 287, //
                           //        RDFLITERAL, // 
        RDFTYPE = 288, // Pyrrho 7.0
        REALLITERAL = 289, //
        RPAREN = 290, // ) 
        SEMICOLON = 291, // ; 
        TILDE = 292, // ~ GQL
        TIMES = 293, // *
        VBAR = 294, // | 
        VPLUS = 295, // |+| GQL multiset alternation operator
        //=========================NON-RESERVED WORDS================
        A = 296, 
        TABLE = 297, // must be 297 
        ABSENT = 298,
        ABSOLUTE = 299,
        ACTION = 300,
        ACYCLIC = 301,
        ADA = 302,
        ADD = 303,
        ADMIN = 304,
        AFTER = 305,
        ALLOCATE = 306,
        ALTER = 307,
        ALWAYS = 308,
        ANY_VALUE = 309,
        APPLICATION = 310, // Pyrrho 4.6
        ARE = 311,
        ARRAY_AG = 312,
        ARRAY_MAX_CARDINALITY = 313,
        ARRIVING = 314, // Pyrrho Metadata 7.07 deprecated
        ASENSITIVE = 315,
        ASSERTION = 316,
        ASYMMETRIC = 317,
        ATOMIC = 318,
        ATTRIBUTE = 319,
        ATTRIBUTES = 320,
        AUTHORIZATION = 321,
        BECAUSE = 322, // Graph metadata 8.1
        BEFORE = 323,
        BEGIN = 324,
        BEGIN_FRAME = 325,
        BEGIN_PARTITION = 326,
        REF = 327, // was POSITION must be 327
        BERNOULLI = 328,
        BETWEEN = 329,
        BINDING = 330, // GQL
        BINDINGS = 331, // GQL
        BLOB = 332,
        BREADTH = 333,
        BREAK = 334, // Pyrrho
        C = 335,
        CALL_PROCEDURE_STATEMENT = 336, // GQL
        CALLED = 337,
        CAPTION = 338, // Pyrrho 4.5
        CASCADE = 339,
        CASCADED = 340,
        CATALOG = 341,
        CATALOG_NAME = 342,
        CHAIN = 343,
        CHARACTER = 344,
        CHARACTER_SET_CATALOG = 345,
        CHARACTER_SET_NAME = 346,
        CHARACTER_SET_SCHEMA = 347,
        CHARACTERS = 348,
        CHECK = 349,
        CLASS_ORIGIN = 350, // CLOB see 40
        COBOL = 351,
        COLLATE = 352,
        COLLATION = 353,
        COLLATION_CATALOG = 354,
        COLLATION_NAME = 355,
        COLLATION_SCHEMA = 356,
        COLLECT = 357,
        COLUMN = 358,
        COLUMN_NAME = 359,
        COMMAND_FUNCTION = 360,
        COMMAND_FUNCTION_CODE = 361,
        COMMIT_COMMAND = 362, // GQL
        COMMITTED = 363,
        CONDITION = 364,
        CONDITION_NUMBER = 365,
        CONNECT = 366,
        CONNECTING = 367, // GQL
        CONNECTION = 368,
        CONNECTION_NAME = 369,
        CONSTRAINT = 370,
        CONSTRAINT_CATALOG = 371,
        CONSTRAINT_NAME = 372,
        CONSTRAINT_SCHEMA = 373,
        CONSTRAINTS = 374,
        CONSTRUCTOR = 375,
        CONTAINS = 376,
        CONVERT = 377,
        CONTENT = 378,
        CONTINUE = 379,
        CORR = 380,
        CORRESPONDING = 381,
        COSINE = 382,
        COVAR_POP = 383,
        COVAR_SAMP = 384,
        CREATE_GRAPH_STATEMENT = 385, // GQL
        CREATE_GRAPH_TYPE_STATEMENT = 386, // GQL
        CREATE_SCHEMA_STATEMENT = 387, // GQL
        CROSS = 388,
        CSV = 389, // Pyrrho 5.5
        CUBE = 390,
        CUME_DIST = 391,
        CURATED = 392, // Pyrrho
        CURRENT = 393,
        CURRENT_CATALOG = 394,
        CURRENT_DEFAULT_TRANSFORM_GROUP = 395,
        CURRENT_PATH = 396,
        CURRENT_ROLE = 397,
        CURRENT_ROW = 398,
        CURRENT_TRANSFORM_GROUP_FOR_TYPE = 399,
        CURRENT_USER = 400, // CURSOR see 65
        CURSOR_NAME = 401,
        CYCLE = 402, // DATE see 67
        DATA = 403,
        DATABASE = 404, // Pyrrho
        DATETIME_INTERVAL_CODE = 405,
        DATETIME_INTERVAL_PRECISION = 406,
        DEALLOCATE = 407,
        DECFLOAT = 408,
        DECLARE = 409,
        DEFAULT = 410,
        DEFAULTS = 411,
        DEFERRABLE = 412,
        DEFERRED = 413,
        DEFINE = 414,
        DEFINED = 415,
        DEFINER = 416,
        DEGREE = 417,
        DENSE_RANK = 418,
        DELETE_STATEMENT = 419, // GQL
        DEPTH = 420,
        DEREF = 421,
        DERIVED = 422,
        DESCRIBE = 423,
        DESCRIPTOR = 424,
        DESTINATION = 425, // GQL pre
        DETERMINISTIC = 426,
        DIAGNOSTICS = 427,
        DIRECTED = 428, // GQL
        DISPATCH = 429,
        DISCONNECT = 430,
        DO = 431, // from vol 4
        DOCARRAY = 432, // Pyrrho 5.1
        DOCUMENT = 433, // Pyrrho 5.1
        DOMAIN = 434,
        DOT = 435,
        DROP_GRAPH_STATEMENT = 436, // GQL
        DROP_GRAPH_TYPE_STATEMENT = 437, // GQL
        DROP_SCHEMA_STATEMENT = 438, // GQL
        DYNAMIC_FUNCTION = 439,
        DYNAMIC_FUNCTION_CODE = 440,
        EACH = 441,
        EDGE = 442, // 7.03 EDGETYPE is 461
        EDGES = 443, // GQL
        ELEMENT = 444,
        ELEMENTID = 445, // Pyrrho 7.05
        ELEMENTS = 446, // GQL pre
        ELSEIF = 447, // from vol 4
        EMPTY = 448,
        ENFORCED = 449,
        ENTITY = 450, // Pyrrho 4.5
        END_EXEC = 451, // misprinted in SQL2023 as END-EXEC
        END_FRAME = 452,
        END_PARTITION = 453,
        EOF = 454,	// Pyrrho 0.1
        EQUALS = 455,
        ESCAPE = 456,
        ETAG = 457, // Pyrrho Metadata 7.0
        EUCLIDEAN = 458,
        EUCLIDEAN_SQUARED = 459,
        EVERY = 460,
        EDGETYPE = 461, // Metadata 7.03 must be 461
        EXCLUDE = 462,
        EXCLUDING = 463,
        EXEC = 464,
        EXECUTE = 465,
        EXIT = 466, // from vol 4
        EXPECT = 467, // 8.1
        EXTERNAL = 468,
        EXTRACT = 469,
        FETCH = 470,
        FILTER_STATEMENT = 471, // GQL
        FINAL = 472,
        FIRST = 473, // GQL
        FIRST_VALUE = 474,
        FLAG = 475,
        FOLLOWING = 476,
        FOR_STATEMENT = 477, // GQL
        FOREIGN = 478,
        FRAME_ROW = 479,
        FREE = 480,
        FORTRAN = 481,
        FOUND = 482,
        FULL = 483,
        FUNCTION = 484,
        FUSION = 485,
        G = 486,
        GENERAL = 487,
        GENERATED = 488,
        GET = 489,
        GLOBAL = 490,
        GO = 491,
        GOTO = 492,
        GRANT = 493,
        GRANTED = 494,
        GRAPH = 495, //7.03
        GREATEST = 496,
        GROUPING = 497,
        GROUPS = 498,
        HAMMING = 499,
        HANDLER = 500, // vol 4 
        HIERARCHY = 501,
        HISTOGRAM = 502, // Pyrrho 4.5
        HOLD = 503,
        HTTP = 504,
        HTTPDATE = 505, // Pyrrho 7 RFC 7231
        IDENTITY = 506,
        IGNORE = 507,
        IMMEDIATE = 508,
        IMMEDIATELY = 509,
        IMPLEMENTATION = 510,
        INCLUDING = 511,
        INDICATOR = 512,
        INITIAL = 513,
        METADATA = 514, // must be 514
        INNER = 515,
        INOUT = 516,
        INSENSITIVE = 517,
        INCREMENT = 518,
        INITIALLY = 519,
        INPUT = 520,
        INSERT_STATEMENT = 521, // GQL
        INSTANCE = 522,
        INSTANTIABLE = 523,
        INSTEAD = 524,
        INTERSECTION = 525, // INTERVAL is 152
        INTO = 526,
        INVERTS = 527, // Pyrrho Metadata 5.7
        INVOKER = 528,
        IRI = 529, // Pyrrho 7
        ISOLATION = 530,
        ITERATE = 531, // vol 4
        JOIN = 532,
        JSON = 533,
        JSON_ARRAY = 534,
        NODETYPE = 535, // Metadata 7.03 must be 534
        JSON_ARRAYAGG = 536,
        JSON_EXISTS = 537,
        JSON_OBJECT = 538,
        JSON_OBJECTAGG = 539,
        JSON_QUERY = 540,
        JSON_SCALAR = 541,
        JSON_SERIALIZE = 542,
        JSON_TABLE = 543,
        JSON_TABLE_PRIMITIVE = 544,
        JSON_VALUE = 545,
        K = 546,
        KEEP = 547, // GQL
        KEY_MEMBER = 548,
        KEY_TYPE = 549,
        LABEL = 550,  // GQL
        LABELLED = 551, // GQL
        LABELS = 552, // Pyrrho 7.05
        LAG = 553,
        LANGUAGE = 554,
        LARGE = 555,
        LAST = 556,
        LAST_DATA = 557, // Pyrrho v7
        LAST_VALUE = 558,
        LATERAL = 559,
        LEAD = 560,
        LEAST = 561,
        LEAVE = 562, // vol 4
        LEAVING = 563, // Pyrrho Metadata 7.07
        LEGEND = 564, // Pyrrho Metadata 4.8
        LENGTH = 565,
        LET_STATEMENT = 566, //GQL
        LEVEL = 567,
        LIKE_REGEX = 568,
        LINE = 569, // Pyrrho 4.5
        LISTAGG = 570,
        LOCALTIME = 571,
        LOCALTIMESTAMP = 572,
        LOCATOR = 573,
        LONGEST = 574, // Pyrrho 7.09 added to GQL
        LOOP = 575,  // vol 4
        LPAD = 576,
        M = 577,
        MANHATTAN = 578,
        MAP = 579,
        MATCH_STATEMENT = 580, // GQL
        MATCH_RECOGNIZE = 581,
        MATCHED = 582,
        MATCHES = 583,
        MEMBER = 584,
        MERGE = 585,
        METHOD = 586,
        MAXVALUE = 587,
        MESSAGE_LENGTH = 588,
        MESSAGE_OCTET_LENGTH = 589,
        MESSAGE_TEXT = 590, // METADATA is 514
        MILLI = 591, // Pyrrho 7
        MIME = 592, // Pyrrho 7
        MINVALUE = 593,
        MONOTONIC = 594, // Pyrrho 5.7
        MORE = 595,
        MULTIPLICITY = 596, // Pyrrho 7.03
        MUMPS = 597,
        NAME = 598,
        NAMES = 599,
        NESTING = 600,
        MATCH_NUMBER = 601,
        MODIFIES = 602,
        MODULE = 603,
        NATIONAL = 604,
        NATURAL = 605, // NCHAR 171 NCLOB 172
        NEW = 606,
        NFC = 607,  // GQL Normalization forms
        NFD = 608,  // GQL
        NFKC = 609,  // GQL
        NFKD = 610,  // GQL
        NO = 611,
        NODE = 612, // 7.03 NODETYPE is 534
        NONE = 613,
        NORMALIZED = 614,
        NULLABLE = 615,
        NUMBER = 616,
        NTH_VALUE = 617,
        NTILE = 618, // NULL is 177
        OBJECT = 619,
        OCCURRENCE = 620,
        OCCURRENCES_REGEX = 621,
        OCTETS = 622,
        OLD = 623,
        OMIT = 624,
        ONE = 625,
        ONLY = 626,
        OPEN = 627,
        OPTION = 628,
        OPTIONS = 629,
        ORDER_BY_AND_PAGE_STATEMENT = 630, // GQL
        ORDERING = 631,
        ORDINALITY = 632, // GQL
        OTHERS = 633,
        OUT = 634,
        OUTER = 635,
        OUTPUT = 636,
        OVER = 637,
        OVERLAPS = 638,
        OVERLAY = 639,
        OVERRIDING = 640,
        OWNER = 641, // Pyrrho
        P = 642,
        PAD = 643,
        PARAMETER_MODE = 644,
        PARAMETER_NAME = 645,
        PARAMETER_ORDINAL_POSITION = 646,
        PARAMETER_SPECIFIC_CATALOG = 647,
        PARAMETER_SPECIFIC_NAME = 648,
        PARAMETER_SPECIFIC_SCHEMA = 649,
        PARTIAL = 650,
        PARTITION = 651,
        PASCAL = 652,
        PATTERN = 653,
        PER = 654,
        PERCENT = 655,
        PERCENT_RANK = 656,
        PERIOD = 657,
        PIE = 658, // Pyrrho 4.5
        PLACING = 659,
        PL1 = 660,
        POINTS = 661, // Pyrrho 4.5
        PORTION = 662,
        POSITION_REGEX = 663,
        PRECEDES = 664,
        PRECEDING = 665,
        PREFIX = 666, // Pyrrho 7.01
        PREPARE = 667,
        PRESERVE = 668,
        PRIMARY = 669,
        PRIOR = 670,
        PRIVILEGES = 671,
        PROCEDURE = 672,
        PROPERTY = 673, // GQL
        PTF = 674,
        PUBLIC = 675,
        READ = 676,
        RANGE = 677,
        RANK = 678,
        READS = 679,    // REAL 203 (previously 199 see REAL0)
        RECURSIVE = 680, // REF is 327 must be 327
        RELATIONSHIP = 681, // GQL
        RELATIONSHIPS = 682, // GQL
        RELATIVE = 683,
        REMOVE_STATEMENT = 684, // GQL
        REPEATABLE = 685,
        RESPECT = 686,
        RESTART = 687,
        RESTRICT = 688,
        REFERENCES = 689,
        REFERENCING = 690,
        RELEASE = 691,
        REPEAT = 692, // vol 4
        RESIGNAL = 693, // vol 4
        RESULT = 694,
        RETURN_STATEMENT = 695, // GQL
        RETURNED_CARDINALITY = 696,
        RETURNED_LENGTH = 697,
        RETURNED_OCTET_LENGTH = 698,
        RETURNED_SQLSTATE = 699,
        RETURNING = 700,
        RETURNS = 701,
        REVOKE = 702,
        ROLE = 703,
        ROLLBACK_COMMAND = 704, // GQL
        ROUTINE = 705,
        ROUTINE_CATALOG = 706,
        ROUTINE_NAME = 707,
        ROUTINE_SCHEMA = 708,
        ROW_COUNT = 709,
        ROW = 710,
        ROW_NUMBER = 711,
        ROWS = 712,
        RPAD = 713,
        RUNNING = 714,
        SAVEPOINT = 715,
        SCALE = 716,
        SCHEMA_NAME = 717,
        SCOPE = 718,
        SCOPE_CATALOG = 719,
        SCOPE_NAME = 720,
        SCOPE_SCHEMA = 721,
        SCROLL = 722,
        SEARCH = 723,
        SECTION = 724,
        SECURITY = 725,
        SELECT_STATEMENT = 726, // GQL
        SELF = 727,
        SENSITIVE = 728,
        SEQUENCE = 729,
        SERIALIZABLE = 730,
        SERVER_NAME = 731,
        SESSION_CLOSE_COMMAND = 732, // GQL
        SESSION_RESET_COMMAND = 733, // GQL
        SESSION_SET_BINDING_TABLE_PARAMETER_COMMAND = 734, // GQL
        SESSION_SET_PROPERTY_GRAPH_COMMAND = 735, // GQL
        SESSION_SET_PROPERTY_GRAPH_PARAMETER_COMMAND = 736, //GQL
        SESSION_SET_SCHEMA_COMMAND = 737, // GQL
        SESSION_SET_TIME_ZONE_COMMAND = 738, // GQL
        SESSION_SET_VALUE_PARAMETER_COMMAND = 739, // GQL
        SET_STATEMENT = 740, // GQL
        SETS = 741,
        SHORTEST = 742,
        SHOW = 743,
        SIGNAL = 744, //vol 4
        SIMILAR = 745,
        SIMPLE = 746,
        SOME = 747,
        SOURCE = 748,
        SPACE = 749,
        SPECIFIC = 750,
        SPECIFIC_NAME = 751,
        SPECIFICTYPE = 752,
        SQL = 753,
        SQLAGENT = 754, // Pyrrho 7
        SQLEXCEPTION = 755,
        SQLSTATE = 756,
        SQLWARNING = 757,
        STANDALONE = 758, // vol 14
        START_TRANSACTION_COMMAND = 759, // GQL
        STATE = 760,
        STATEMENT = 761,
        STATIC = 762,
        STRUCTURE = 763,
        STYLE = 764,
        SUBCLASS_ORIGIN = 765,
        SUBMULTISET = 766,
        SUBSET = 767,
        SUBSTRING = 768, //
        SUBSTRING_REGEX = 769,
        SUCCEEDS = 770,
        SUFFIX = 771, // Pyrrho 7.01
        SYMMETRIC = 772,
        SYSTEM = 773,
        SYSTEM_TIME = 774,
        SYSTEM_USER = 775,
        T = 776,
        TABLESAMPLE = 777,// TABLE is 297
        TABLE_NAME = 778,
        TEMP = 779, // GQL
        TEMPORARY = 780,
        TIES = 781,
        TIMEOUT = 782, // Pyrrho
        TIMEZONE_HOUR = 783,
        TIMEZONE_MINUTE = 784,
        TO = 785,
        TRAIL = 786,
        TRANSACTION = 787,
        TRANSACTION_ACTIVE = 788,
        TRANSACTIONS_COMMITTED = 789,
        TRANSACTIONS_ROLLED_BACK = 790,
        TRANSFORM = 791,
        TRANSFORMS = 792,
        TRANSLATE = 793,
        TRANSLATE_REGEX = 794,
        TRANSLATION = 795,
        TREAT = 796,
        TRIGGER = 797,
        TRIGGER_CATALOG = 798,
        TRIGGER_NAME = 799,
        TRIGGER_SCHEMA = 800, // TYPE  is 267 but is not a reserved word
        TRIM_ARRAY = 801,
        TRUNCATE = 802,
        TRUNCATING = 803, // Pyrrho 7.07
        TYPE_URI = 804, // Pyrrho
        UESCAPE = 805,
        UNBOUNDED = 806,
        UNCOMMITTED = 807,
        UNDER = 808,
        UNDIRECTED = 809, //GQL
        UNNEST = 810,
        UNTIL = 811, // vol 4
        UPDATE = 812,
        USER = 813,
        USING = 814,
        UNDO = 815,
        UNNAMED = 816,
        URL = 817,  // Pyrrho 7
        USAGE = 818,
        USER_DEFINED_TYPE_CATALOG = 819,
        USER_DEFINED_TYPE_CODE = 820,
        USER_DEFINED_TYPE_NAME = 821,
        USER_DEFINED_TYPE_SCHEMA = 822,
        VALUE_OF = 823,
        VALUES = 824,
        VAR_POP = 825,
        VAR_SAMP = 826,
        VARYING = 827,
        VERSION = 828, // row-versioning: new for Pyrrho 7.09 April 2026
        VERSIONING = 829,
        VERTEX = 830, // GQL
        VIEW = 831,
        WHENEVER = 832,
        WHILE = 833, // vol 4
        WIDTH_BUCKET = 834,
        WITHIN = 835,
        WITHOUT = 836,
        WORK = 837,
        WRITE = 838,
        X = 839, // Pyrrho 4.5
        Y = 840, // Pyrrho 4.5
        ZONE = 841
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
        Multiset,   // Integer els, els x (ob,count) 
        Array,		// Integer els, els x (long, ob)
        Vector,     // Integer els, els x (long, ob)
        List        // List or Set: Integer els, els x obs

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
        internal string signal; // Compatible with GQL
        internal object[] objects; // additional obs for insertion in (possibly localised) message format
        // diagnostic info (there is an active transaction unless we have just done a rollback)
        internal ATree<Qlx, TypedValue> info = new BTree<Qlx, TypedValue>(Qlx.TRANSACTION_ACTIVE, new TInt(1));
        readonly TChar iso = new ("ISO 39075");
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
        /// <param name="k">diagnostic key as in SQL2023 or GQL</param>
        /// <param name="v">value of this diagnostic</param>
        /// <returns>this (so we can chain diagnostics)</returns>
        internal DBException Add(Qlx k, TypedValue? v = null)
        {
            ATree<Qlx, TypedValue>.Add(ref info, k, v ?? TNull.Value);
            return this;
        }
        internal DBException AddType(ObInfo t)
        {
            Add(Qlx.TYPE, new TChar(t.ToString()));
            return this;
        }
        internal DBException AddType(Domain t)
        {
            Add(Qlx.TYPE, new TChar(t.ToString()));
            return this;
        }
        internal DBException AddValue(TypedValue v)
        {
            Add(Qlx.VALUE, v);
            return this;
        }
        internal DBException AddValue(Domain t)
        {
            Add(Qlx.VALUE, new TChar(t.ToString()));
            return this;
        }
        /// <summary>
        /// Helper for GQL-defined exceptions
        /// </summary>
        /// <returns>this (so we can chain diagnostics)</returns>
        internal DBException ISO()
        {
            Add(Qlx.CLASS_ORIGIN, iso);
            Add(Qlx.SUBCLASS_ORIGIN, pyrrho);
            return this;
        }
        /// <summary>
        /// Helper for Pyrrho-defined exceptions
        /// </summary>
        /// <returns>this (so we can chain diagnostics)</returns>
        internal DBException Pyrrho()
        {
            Add(Qlx.CLASS_ORIGIN, pyrrho);
            Add(Qlx.SUBCLASS_ORIGIN, pyrrho);
            return this;
        }
        /// <summary>
        /// Helper for Pyrrho-defined exceptions in SQL-2011 class
        /// </summary>
        /// <returns>this (so we can chain diagnostics)</returns>
        internal DBException Mix()
        {
            Add(Qlx.CLASS_ORIGIN, iso);
            Add(Qlx.SUBCLASS_ORIGIN, pyrrho);
            return this;
        }
    }

    /// <summary>
    /// Supports the SQL2011 Interval object type. 
    /// Note that Intervals cannot have both year-month and day-_inner fields.
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
            else throw new DBException("22G04");
        }
    }
    /// <summary>
    /// Row Version cookie (Qlx.VERSIONING). See Laiho/Laux 2010.
    /// CheckFields allows transactions to find out if another transaction has overritten the row.
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
        internal new static Rvv Empty = new ();
        internal long version => First()?.value()?.Last()?.value() ?? 0L;

        internal static readonly string[] separator = ["\",\""];

        Rvv() : base()
        { }
        protected Rvv(CTree<long, CTree<long, long>> t) : base(t.root ?? throw new PEException("PE925")) { }
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
            var s = ((d <= 0) ? null : r[t]) ?? CTree<long, long>.Empty;
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
                if (bb.value() is CTree<long, long> bt)
                {
                    var k = bb.key();
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
                            if (dp == 0L || dp == -1L)
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
            var ss = s.Trim('"').Split(separator, StringSplitOptions.None);
            foreach (var t in ss)
            {
                var tt = t.Split('.');
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
            var sb = new StringBuilder("\"");
            var sc = "";
            for (var b = First(); b != null; b = b.Next())
            {
                sb.Append(sc); sc = "\",\"";
                sb.Append(DBObject.Uid(b.key())); 
                for (var c = b.value()?.First(); c != null; c = c.Next())
                if (c.value() is long p){
                    sb.Append('.');
                    sb.Append(DBObject.Uid(c.key())); sb.Append('.');
                    sb.Append(DBObject.Uid(p));
                }
            }
            sb.Append('"');
            return sb.ToString();
        }
    }
    /// <summary>
    /// Supports the SQL2003 Date object type
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
