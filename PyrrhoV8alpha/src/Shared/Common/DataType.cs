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
        A = 296, // first non-reserved word
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
        BEFORE = 322,
        BEGIN = 323,
        BEGIN_FRAME = 324,
        BEGIN_PARTITION = 325,
        BERNOULLI = 326,
        REF = 327, // was POSITION must be 327
        BETWEEN = 328,
        BINDING = 329, // GQL
        BINDINGS = 330, // GQL
        BLOB = 331,
        BREADTH = 332,
        BREAK = 333, // Pyrrho
        C = 334,
        CALL_PROCEDURE_STATEMENT = 335, // GQL
        CALLED = 336,
        CAPTION = 337, // Pyrrho 4.5
        CASCADE = 338,
        CASCADED = 339,
        CATALOG = 340,
        CATALOG_NAME = 341,
        CHAIN = 342,
        CHARACTER = 343,
        CHARACTER_SET_CATALOG = 344,
        CHARACTER_SET_NAME = 345,
        CHARACTER_SET_SCHEMA = 346,
        CHARACTERS = 347,
        CHECK = 348,
        CLASS_ORIGIN = 349, // CLOB see 40
        COBOL = 350,
        COLLATE = 351,
        COLLATION = 352,
        COLLATION_CATALOG = 353,
        COLLATION_NAME = 354,
        COLLATION_SCHEMA = 355,
        COLLECT = 356,
        COLUMN = 357,
        COLUMN_NAME = 358,
        COMMAND_FUNCTION = 359,
        COMMAND_FUNCTION_CODE = 360,
        COMMIT_COMMAND = 361, // GQL
        COMMITTED = 362,
        CONDITION = 363,
        CONDITION_NUMBER = 364,
        CONNECT = 365,
        CONNECTING = 366, // GQL
        CONNECTION = 367,
        CONNECTION_NAME = 368,
        CONSTRAINT = 369,
        CONSTRAINT_CATALOG = 370,
        CONSTRAINT_NAME = 371,
        CONSTRAINT_SCHEMA = 372,
        CONSTRAINTS = 373,
        CONSTRUCTOR = 374,
        CONTAINS = 375,
        CONVERT = 376,
        CONTENT = 377,
        CONTINUE = 378,
        CORR = 379,
        CORRESPONDING = 380,
        COSINE = 381,
        COVAR_POP = 382,
        COVAR_SAMP = 383,
        CREATE_GRAPH_STATEMENT = 384, // GQL
        CREATE_GRAPH_TYPE_STATEMENT = 385, // GQL
        CREATE_SCHEMA_STATEMENT = 386, // GQL
        CROSS = 387,
        CSV = 388, // Pyrrho 5.5
        CUBE = 389,
        CUME_DIST = 390,
        CURATED = 391, // Pyrrho
        CURRENT = 392,
        CURRENT_CATALOG = 393,
        CURRENT_DEFAULT_TRANSFORM_GROUP = 394,
        CURRENT_PATH = 395,
        CURRENT_ROLE = 396,
        CURRENT_ROW = 397,
        CURRENT_TRANSFORM_GROUP_FOR_TYPE = 398,
        CURRENT_USER = 399, // CURSOR see 65
        CURSOR_NAME = 400,
        CYCLE = 401, // DATE see 67
        DATA = 402,
        DATABASE = 403, // Pyrrho
        DATETIME_INTERVAL_CODE = 404,
        DATETIME_INTERVAL_PRECISION = 405,
        DEALLOCATE = 406,
        DECFLOAT = 407,
        DECLARE = 408,
        DEFAULT = 409,
        DEFAULTS = 410,
        DEFERRABLE = 411,
        DEFERRED = 412,
        DEFINE = 413,
        DEFINED = 414,
        DEFINER = 415,
        DEGREE = 416,
        DENSE_RANK = 417,
        DELETE_STATEMENT = 418, // GQL
        DEPTH = 419,
        DEREF = 420,
        DERIVED = 421,
        DESCRIBE = 422,
        DESCRIPTOR = 423,
        DESTINATION = 424, // GQL pre
        DETERMINISTIC = 425,
        DIAGNOSTICS = 426,
        DIRECTED = 427, // GQL
        DISPATCH = 428,
        DISCONNECT = 429,
        DO = 430, // from vol 4
        DOCARRAY = 431, // Pyrrho 5.1
        DOCUMENT = 432, // Pyrrho 5.1
        DOMAIN = 433,
        DOT = 434,
        DROP_GRAPH_STATEMENT = 435, // GQL
        DROP_GRAPH_TYPE_STATEMENT = 436, // GQL
        DROP_SCHEMA_STATEMENT = 437, // GQL
        DYNAMIC_FUNCTION = 438,
        DYNAMIC_FUNCTION_CODE = 439,
        EACH = 440,
        EDGE = 441, // 7.03 EDGETYPE is 461
        EDGES = 442, // GQL
        ELEMENT = 443,
        ELEMENTID = 444, // Pyrrho 7.05
        ELEMENTS = 445, // GQL pre
        ELSEIF = 446, // from vol 4
        EMPTY = 447,
        ENFORCED = 448,
        ENTITY = 449, // Pyrrho 4.5
        END_EXEC = 450, // misprinted in SQL2023 as END-EXEC
        END_FRAME = 451,
        END_PARTITION = 452,
        EOF = 453,	// Pyrrho 0.1
        EQUALS = 454,
        ESCAPE = 455,
        ETAG = 456, // Pyrrho Metadata 7.0
        EUCLIDEAN = 457,
        EUCLIDEAN_SQUARED = 458,
        EVERY = 459,
        EXCLUDE = 460,
        EDGETYPE = 461, // Metadata 7.03 must be 461
        EXCLUDING = 462,
        EXEC = 463,
        EXECUTE = 464,
        EXIT = 465, // from vol 4
        EXTERNAL = 466,
        EXTRACT = 467,
        FETCH = 468,
        FILTER_STATEMENT = 469, // GQL
        FINAL = 470,
        FIRST = 471, // GQL
        FIRST_VALUE = 472,
        FLAG = 473,
        FOLLOWING = 474,
        FOR_STATEMENT = 475, // GQL
        FOREIGN = 476,
        FRAME_ROW = 477,
        FREE = 478,
        FORTRAN = 479,
        FOUND = 480,
        FULL = 481,
        FUNCTION = 482,
        FUSION = 483,
        G = 484,
        GENERAL = 485,
        GENERATED = 486,
        GET = 487,
        GLOBAL = 488,
        GO = 489,
        GOTO = 490,
        GRANT = 491,
        GRANTED = 492,
        GRAPH = 493, //7.03
        GREATEST = 494,
        GROUPING = 495,
        GROUPS = 496,
        HAMMING = 497,
        HANDLER = 498, // vol 4 
        HIERARCHY = 499,
        HISTOGRAM = 500, // Pyrrho 4.5
        HOLD = 501,
        HTTP = 502,
        HTTPDATE = 503, // Pyrrho 7 RFC 7231
        ID = 504, // Distinguished from the token type Id from Pyrrho 7.05
        IDENTITY = 505,
        IGNORE = 506,
        IMMEDIATE = 507,
        IMMEDIATELY = 508,
        IMPLEMENTATION = 509,
        INCLUDING = 510,
        INDICATOR = 511,
        INITIAL = 512,
        INNER = 513,
        METADATA = 514, // must be 514
        INOUT = 515,
        INSENSITIVE = 516,
        INCREMENT = 517,
        INITIALLY = 518,
        INPUT = 519,
        INSERT_STATEMENT = 520, // GQL
        INSTANCE = 521,
        INSTANTIABLE = 522,
        INSTEAD = 523,
        INTERSECTION = 524, // INTERVAL is 152
        INTO = 525,
        INVERTS = 526, // Pyrrho Metadata 5.7
        INVOKER = 527,
        IRI = 528, // Pyrrho 7
        ISOLATION = 529,
        ITERATE = 530, // vol 4
        JOIN = 531,
        JSON = 532,
        JSON_ARRAY = 533,
        NODETYPE = 534, // Metadata 7.03 must be 534
        JSON_ARRAYAGG = 535,
        JSON_EXISTS = 536,
        JSON_OBJECT = 537,
        JSON_OBJECTAGG = 538,
        JSON_QUERY = 539,
        JSON_SCALAR = 540,
        JSON_SERIALIZE = 541,
        JSON_TABLE = 542,
        JSON_TABLE_PRIMITIVE = 543,
        JSON_VALUE = 544,
        K = 545,
        KEEP = 546, // GQL
        KEY_MEMBER = 547,
        KEY_TYPE = 548,
        LABEL = 549,  // GQL
        LABELLED = 550, // GQL
        LABELS = 551, // Pyrrho 7.05
        LAG = 552,
        LANGUAGE = 553,
        LARGE = 554,
        LAST = 555,
        LAST_DATA = 556, // Pyrrho v7
        LAST_VALUE = 557,
        LATERAL = 558,
        LEAD = 559,
        LEAST = 560,
        LEAVE = 561, // vol 4
        LEAVING = 562, // Pyrrho Metadata 7.07
        LEGEND = 563, // Pyrrho Metadata 4.8
        LENGTH = 564,
        LET_STATEMENT = 565, //GQL
        LEVEL = 566,
        LIKE_REGEX = 567,
        LINE = 568, // Pyrrho 4.5
        LISTAGG = 569,
        LOCALTIME = 570,
        LOCALTIMESTAMP = 571,
        LOCATOR = 572,
        LONGEST = 573, // Pyrrho 7.09 added to GQL
        LOOP = 574,  // vol 4
        LPAD = 575,
        M = 576,
        MANHATTAN = 577,
        MAP = 578,
        MATCH_STATEMENT = 579, // GQL
        MATCH_RECOGNIZE = 580,
        MATCHED = 581,
        MATCHES = 582,
        MEMBER = 583,
        MERGE = 584,
        METHOD = 585,
        MAXVALUE = 586,
        MESSAGE_LENGTH = 587,
        MESSAGE_OCTET_LENGTH = 588,
        MESSAGE_TEXT = 589, // METADATA is 514
        MILLI = 590, // Pyrrho 7
        MIME = 591, // Pyrrho 7
        MINVALUE = 592,
        MONOTONIC = 593, // Pyrrho 5.7
        MORE = 594,
        MULTIPLICITY = 595, // Pyrrho 7.03
        MUMPS = 596,
        NAME = 597,
        NAMES = 598,
        NESTING = 599,
        MATCH_NUMBER = 600,
        MODIFIES = 601,
        MODULE = 602,
        NATIONAL = 603,
        NATURAL = 604, // NCHAR 171 NCLOB 172
        NEW = 605,
        NFC = 606,  // GQL Normalization forms
        NFD = 607,  // GQL
        NFKC = 608,  // GQL
        NFKD = 609,  // GQL
        NO = 610,
        NODE = 611, // 7.03 NODETYPE is 534
        NONE = 612,
        NORMALIZED = 613,
        NULLABLE = 614,
        NUMBER = 615,
        NTH_VALUE = 616,
        NTILE = 617, // NULL is 177
        OBJECT = 618,
        OCCURRENCE = 619,
        OCCURRENCES_REGEX = 620,
        OCTETS = 621,
        OLD = 622,
        OMIT = 623,
        ONE = 624,
        ONLY = 625,
        OPEN = 626,
        OPTION = 627,
        OPTIONS = 628,
        ORDER_BY_AND_PAGE_STATEMENT = 629, // GQL
        ORDERING = 630,
        ORDINALITY = 631, // GQL
        OTHERS = 632,
        OUT = 633,
        OUTER = 634,
        OUTPUT = 635,
        OVER = 636,
        OVERLAPS = 637,
        OVERLAY = 638,
        OVERRIDING = 639,
        OWNER = 640, // Pyrrho
        P = 641,
        PAD = 642,
        PARAMETER_MODE = 643,
        PARAMETER_NAME = 644,
        PARAMETER_ORDINAL_POSITION = 645,
        PARAMETER_SPECIFIC_CATALOG = 646,
        PARAMETER_SPECIFIC_NAME = 647,
        PARAMETER_SPECIFIC_SCHEMA = 648,
        PARTIAL = 649,
        PARTITION = 650,
        PASCAL = 651,
        PATTERN = 652,
        PER = 653,
        PERCENT = 654,
        PERCENT_RANK = 655,
        PERIOD = 656,
        PIE = 657, // Pyrrho 4.5
        PLACING = 658,
        PL1 = 659,
        POINTS = 660, // Pyrrho 4.5
        PORTION = 661,
        POSITION_REGEX = 662,
        PRECEDES = 663,
        PRECEDING = 664,
        PREFIX = 665, // Pyrrho 7.01
        PREPARE = 666,
        PRESERVE = 667,
        PRIMARY = 668,
        PRIOR = 669,
        PRIVILEGES = 670,
        PROCEDURE = 671,
        PROPERTY = 672, // GQL
        PTF = 673,
        PUBLIC = 674,
        READ = 675,
        RANGE = 676,
        RANK = 677,
        READS = 678,    // REAL 203 (previously 199 see REAL0)
        RECURSIVE = 679, // REF is 327 must be 327
        RELATIONSHIP = 680, // GQL
        RELATIONSHIPS = 681, // GQL
        RELATIVE = 682,
        REMOVE_STATEMENT = 683, // GQL
        REPEATABLE = 684,
        RESPECT = 685,
        RESTART = 686,
        RESTRICT = 687,
        REFERENCES = 688,
        REFERENCING = 689,
        RELEASE = 690,
        REPEAT = 691, // vol 4
        RESIGNAL = 692, // vol 4
        RESULT = 693,
        RETURN_STATEMENT = 694, // GQL
        RETURNED_CARDINALITY = 695,
        RETURNED_LENGTH = 696,
        RETURNED_OCTET_LENGTH = 697,
        RETURNED_SQLSTATE = 698,
        RETURNING = 699,
        RETURNS = 700,
        REVOKE = 701,
        ROLE = 702,
        ROLLBACK_COMMAND = 703, // GQL
        ROUTINE = 704,
        ROUTINE_CATALOG = 705,
        ROUTINE_NAME = 706,
        ROUTINE_SCHEMA = 707,
        ROW_COUNT = 708,
        ROW = 709,
        ROW_NUMBER = 710,
        ROWS = 711,
        RPAD = 712,
        RUNNING = 713,
        SAVEPOINT = 714,
        SCALE = 715,
        SCHEMA_NAME = 716,
        SCOPE = 717,
        SCOPE_CATALOG = 718,
        SCOPE_NAME = 719,
        SCOPE_SCHEMA = 720,
        SCROLL = 721,
        SEARCH = 722,
        SECTION = 723,
        SECURITY = 724,
        SELECT_STATEMENT = 725, // GQL
        SELF = 726,
        SENSITIVE = 727,
        SEQUENCE = 728,
        SERIALIZABLE = 729,
        SERVER_NAME = 730,
        SESSION_CLOSE_COMMAND = 731, // GQL
        SESSION_RESET_COMMAND = 732, // GQL
        SESSION_SET_BINDING_TABLE_PARAMETER_COMMAND = 733, // GQL
        SESSION_SET_PROPERTY_GRAPH_COMMAND = 734, // GQL
        SESSION_SET_PROPERTY_GRAPH_PARAMETER_COMMAND = 735, //GQL
        SESSION_SET_SCHEMA_COMMAND = 736, // GQL
        SESSION_SET_TIME_ZONE_COMMAND = 737, // GQL
        SESSION_SET_VALUE_PARAMETER_COMMAND = 738, // GQL
        SET_STATEMENT = 739, // GQL
        SETS = 740,
        SHORTEST = 741,
        SHOW = 742,
        SIGNAL = 743, //vol 4
        SIMILAR = 744,
        SIMPLE = 745,
        SOME = 746,
        SOURCE = 747,
        SPACE = 748,
        SPECIFIC = 749,
        SPECIFIC_NAME = 750,
        SPECIFICTYPE = 751,
        SQL = 752,
        SQLAGENT = 753, // Pyrrho 7
        SQLEXCEPTION = 754,
        SQLSTATE = 755,
        SQLWARNING = 756,
        STANDALONE = 757, // vol 14
        START_TRANSACTION_COMMAND = 758, // GQL
        STATE = 759,
        STATEMENT = 760,
        STATIC = 761,
        STRUCTURE = 762,
        STYLE = 763,
        SUBCLASS_ORIGIN = 764,
        SUBMULTISET = 765,
        SUBSET = 766,
        SUBSTRING = 767, //
        SUBSTRING_REGEX = 768,
        SUCCEEDS = 769,
        SUFFIX = 770, // Pyrrho 7.01
        SYMMETRIC = 771,
        SYSTEM = 772,
        SYSTEM_TIME = 773,
        SYSTEM_USER = 774,
        T = 775,
        TABLESAMPLE = 776,// TABLE is 297
        TABLE_NAME = 777,
        TEMP = 778, // GQL
        TEMPORARY = 779,
        TIES = 780,
        TIMEOUT = 781, // Pyrrho
        TIMEZONE_HOUR = 782,
        TIMEZONE_MINUTE = 783,
        TO = 784,
        TOP_LEVEL_COUNT = 785,
        TRAIL = 786,
        TRANSACTION = 787,
        TRANSACTION_ACTIVE = 788,
        TRANSACTIONS_COMMITTED = 789,
        TRANSACTIONS_ROLLED_BACK = 790,
        TRANSFORM = 781,
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
