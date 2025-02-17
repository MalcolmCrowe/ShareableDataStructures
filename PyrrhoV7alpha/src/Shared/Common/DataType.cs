using System.Text;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
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
        COPY = 39,
        CLOB = 40, // must be 40 (not in GQL)
        COS = 41,
        COSH = 42,
        COT = 43,
        COUNT = 44,
        CREATE = 45,
        CURRENT_DATE = 46,
        CURRENT_GRAPH = 47, 
        CURRENT_PROPERTY_GRAPH = 48, 
        CURRENT_SCHEMA = 49,
        CURRENT_TIME = 50,
        CURRENT_TIMESTAMP = 51, // DATE see 67
        DATETIME = 52, 
        DAY = 53,
        DEC = 54,
        DECIMAL = 55,
        DEGREES = 56, 
        DELETE = 57,
        DESC = 58, 
        DESCENDING = 59, 
        DETACH = 60, 
        DISTINCT = 61,
        DROP = 62,
        DOUBLE = 63,
        DURATION = 64, 
        CURSOR = 65, // must be 65 (not in GQL)
        DURATION_BETWEEN = 66, 
        DATE = 67, // must be 67	
        ELEMENT_ID = 68, 
        ELSE = 69,
        END = 70,
        EXCEPT = 71,
        EXISTS = 72,
        EXP = 73,
        FALSE = 74,
        FILTER = 75,
        FINISH = 76, 
        FLOAT = 77,
        FLOAT16 = 78,
        FLOAT32 = 79,
        FLOAT64 = 80,
        FLOAT128 = 81,
        FLOAT256 = 82, 
        FLOOR = 83,
        FOR = 84,
        FROM = 85,
        GROUP = 86,
        HAVING = 87,
        HOME_GRAPH = 88,
        HOME_PROPERTY_GRAPH = 89, 
        HOME_SCHEMA = 90, 
        HOUR = 91,
        IF = 92,  
        IMPLIES = 93, 
        IN = 94,
        INSERT = 95, // INT is 136, INTEGER is 135
        INT8 = 96, 
        INT16 = 97, 
        INT32 = 98, 
        INT64 = 99, 
        INT128 = 100, 
        INT256 = 101, 
        INTEGER8 = 102,
        INTEGER16 = 103,
        INTEGER32 = 104,
        INTEGER64 = 105,
        INTEGER128 = 106,
        INTEGER256 = 107,
        INTERSECT = 108, // INTERVAL is 152, (INTERVAL0 is 137)
        IS = 109,
        LEADING = 110,
        LEFT = 111,
        LET = 112, 
        LIKE = 113,
        LIMIT = 114,
        LIST = 115, 
        LN = 116,
        LOCAL = 117,
        LOCAL_DATETIME = 118,
        LOCAL_TIME = 119, 
        LOCAL_TIMESTAMP = 120,
        LOG = 121,
        LOG10 = 122,
        LOWER = 123,
        LTRIM = 124,
        MATCH = 125,
        MAX = 126,
        MIN = 127,
        MINUTE = 128,
        MOD = 129,
        MONTH = 130,	 // MULTISET is 168 (not in GQL)
        NEXT = 131,
        NODETACH = 132, 
        NORMALIZE = 133,
        NOT = 134,
        INTEGER = 135, // must be 135
        INT = 136,  // must be 136 deprecated: see also INTEGERLITERAL
        INTERVAL0 = 137,  // must be 137 (old version of INTERVAL)
        NOTHING = 138, // NULL is 177
        NULLIF = 139, 
        NULLS = 140, // NUMERIC 179, see also NUMERICLITERAL
        OCTET_LENGTH = 141,
        OF = 142,
        OFFSET = 143,
        OPTIONAL = 144, 
        OR = 145,
        ORDER = 146,
        OTHERWISE = 147,
        PARAMETER = 148,
        PARAMETERS = 149,
        PATH = 150,
        PATH_LENGTH = 151,
        INTERVAL = 152, // must be 152 see also INTERVAL0 at 137
        PATHS = 153,
        PERCENTILE_CONT = 154,
        PERCENTILE_DISC = 155,
        POWER = 156,
        PRECISION = 157,
        PROPERTY_EXISTS = 158,
        RADIANS = 159, // REAL see 203
        RECORD = 160, 
        REMOVE = 161, 
        REPLACE = 162,
        RESET = 163,
        RETURN = 164,
        RIGHT = 165,
        ROLLBACK = 166,
        RTRIM = 167,
        MULTISET = 168, // must be 168 (not in GQL)
        SAME = 169, 
        SCHEMA = 170, 
        NCHAR = 171, // must be 171	(not in GQL)
        NCLOB = 172, // must be 172 (not in GQL)
        SECOND = 173,
        SELECT = 174,
        SESSION = 175, 
        SESSION_USER = 176, // SET is 255
        NULL = 177, // must be 177
        SIGNED = 178, 
        NUMERIC = 179, // must be 179 (not reserved in GQL) see DECIMAL
        SIN = 180, 
        SINH = 181, 
        SIZE = 182, 
        SKIP = 183, 
        SMALL = 184,
        SMALLINT = 185,
        SQRT = 186,
        START = 187,
        STDDEV_POP = 188,
        STDDEV_SAMP = 189,
        STRING = 190, 
        SUM = 191, 
        TAN = 192,
        TANH = 193,
        THEN = 194,  // TIME is 257, TIMESTAMP is 258	
        TRAILING = 195,
        TRIM = 196,
        TRUE = 197,
        TYPED = 198,
        REAL0 = 199, // must be 199, previous version of REAL
        UBIGINT = 200,
        UINT = 201, 
        UINT8 = 202,
        REAL = 203, // must be 203, see also REAL0 and REALLITERAL
        UINT16 = 204,
        UINT32 = 205, 
        UINT64 = 206, 
        UINT128 = 207,
        UINT256 = 208,
        UNION = 209,
        UNKNOWN = 210,
        UNSIGNED = 211,
        UPPER = 212,
        USE = 213,
        USMALLINT = 214,
        VALUE = 215,
        VARBINARY = 216,
        VARCHAR = 217,
        PASSWORD = 218, // must be 218, Pyrrho v5 (not reserved)
        VARIABLE = 219,
        WHEN = 220,
        WHERE = 221,
        WITH = 222,
        XOR = 223,
        YEAR = 224,
        YIELD = 225, 
        ZONED = 226, 
        ZONED_DATETIME = 227, 
        ZONED_TIME = 228, // last reserved word
        //====================TOKEN TYPES=====================
        AMPERSAND = 229, // for GQL label expression
        ARROW = 230, // ]-> GQL bracket right arrow 
        ARROWL = 231, // <- GQL left arrow
        ARROWLTILDE = 232, // <~ GQL left arrow tilde
        ARROWR = 233, // -> GQL right arrow
        ARROWRTILDE = 234, // ~> GQL tilde right arrow
        ARROWTILDE = 235, // ]~> GQL bracket tilde right arrow
        ARROWBASE = 236, // -[ GQL minus left bracket
        ARROWBASETILDE = 237, // ~[ GQL tilde left bracket
        BLOBLITERAL = 238, // 
        BOOLEANLITERAL = 239,
        CHARLITERAL = 240, //
        COLON = 241, // :
        COMMA = 242,  // ,        
        CONCATENATE = 243, // ||        
        DIVIDE = 244, // /        
        DOT = 245, // . 5.2 was STOP
        DOUBLEARROW = 246, // => GQL right double arrow
        DOUBLECOLON = 247, // ::
        DOUBLEPERIOD = 248, // ..
        EQL = 249,  // =
        EXCLAMATION = 250, // !  GQL
        GEQ = 251, // >=    
        GTR = 252, // >    
        Id = 253, // identifier
        INTEGERLITERAL = 254, // Pyrrho
        SET = 255,  // must be 255 (is reserved word)
        LBRACE = 256, // {
        TIME = 257, // must be 257
        TIMESTAMP = 258, // must be 258
        LBRACK = 259, // [
        LEQ = 260, // <=
        LPAREN = 261, // (
        LSS = 262, // <
        MINUS = 263, // -
        NEQ = 264, // <>
        NUMERICLITERAL = 265, // 
        PLUS = 266, // + 
        TYPE = 267, // must be 267
        QMARK = 268, // ?
        RARROW = 269, // <-[ GQL left arrow bracket
        RARROWTILDE = 270, // <~[ GQL left arrow tilde bracket
        RARROWBASE = 271, // ]- GQL right bracket minus
        RBRACE = 272, // } 
        RBRACK = 273, // ] 
        RBRACKTILDE = 274, // ]~ GQL right brancket tilde
        RDFDATETIME = 275, //
        RDFLITERAL = 276, // 
        RDFTYPE = 277, // Pyrrho 7.0
        REALLITERAL = 278, //
        RPAREN = 279, // ) 
        SEMICOLON = 280, // ; 
        TILDE = 281, // ~ GQL
        TIMES = 282, // *
        VBAR = 283, // | 
        VPLUS = 284, // |+| GQL multiset alternation operator
        //=========================NON-RESERVED WORDS================
        A = 285, // first non-reserved word
        ABSENT = 286,
        ABSOLUTE = 287,
        ACTION = 288,
        ACYCLIC = 289,
        ADA = 290,
        ADD = 291,
        ADMIN = 292,
        AFTER = 293,
        ALLOCATE = 294,
        ALTER = 295, 
        ALWAYS = 296,
        TABLE = 297, // must be 297 
        ANY_VALUE = 298,
        APPLICATION = 299, // Pyrrho 4.6
        ARE = 300,
        ARRAY_AG = 301,
        ARRAY_MAX_CARDINALITY = 302,
        ARRIVING = 303, // Pyrrho Metadata 7.07
        ASENSITIVE = 304,
        ASSERTION = 305,
        ASYMMETRIC = 306,
        ATOMIC = 307,
        ATTRIBUTE = 308,
        ATTRIBUTES = 309,
        AUTHORIZATION = 310,
        BEFORE = 311,
        BEGIN = 312,
        BEGIN_FRAME = 313,
        BEGIN_PARTITION = 314,
        BERNOULLI = 315,
        BETWEEN = 316,
        BINDING = 317, // GQL
        BINDINGS = 318, // GQL
        BLOB = 319,
        BREADTH = 320,
        BREAK = 321, // Pyrrho
        C = 322,
        CALL_PROCEDURE_STATEMENT = 323, // GQL
        CALLED = 324,
        CAPTION = 325, // Pyrrho 4.5
        CASCADE = 326,
        POSITION = 327, // Pyrrho 7.08 must be 327
        CASCADED = 328,
        CATALOG = 329,
        CATALOG_NAME = 330,
        CHAIN = 331,
        CHARACTER = 332,
        CHARACTER_SET_CATALOG = 333,
        CHARACTER_SET_NAME = 334,
        CHARACTER_SET_SCHEMA = 335,
        CHARACTERS = 336,
        CHECK = 337,
        CLASS_ORIGIN = 338, // CLOB see 40
        COBOL = 339, 
        COLLATE = 340,
        COLLATION = 341,
        COLLATION_CATALOG = 342,
        COLLATION_NAME = 343,
        COLLATION_SCHEMA = 344,
        COLLECT = 345,
        COLUMN = 346,
        COLUMN_NAME = 347,
        COMMAND_FUNCTION = 348,
        COMMAND_FUNCTION_CODE = 349,
        COMMIT_COMMAND = 350, // GQL
        COMMITTED = 351,
        CONDITION = 352,
        CONDITION_NUMBER = 353,
        CONNECT = 354,
        CONNECTING = 355, // GQL
        CONNECTION = 356,
        CONNECTION_NAME = 357,
        CONSTRAINT = 358,
        CONSTRAINT_CATALOG = 359,
        CONSTRAINT_NAME = 360,
        CONSTRAINT_SCHEMA = 361,
        CONSTRAINTS = 362,
        CONSTRUCTOR = 363,
        CONTAINS = 364,
        CONVERT = 365,
        CONTENT = 366,
        CONTINUE = 367,
        CORR = 368,
        CORRESPONDING = 369,
        COVAR_POP = 370,
        COVAR_SAMP = 371,
        CREATE_GRAPH_STATEMENT = 372, // GQL
        CREATE_GRAPH_TYPE_STATEMENT = 373, // GQL
        CREATE_SCHEMA_STATEMENT = 374, // GQL
        CROSS = 375,
        CSV = 376, // Pyrrho 5.5
        CUBE = 377,
        CUME_DIST = 378,
        CURATED = 379, // Pyrrho
        CURRENT = 380,
        CURRENT_CATALOG = 381,
        CURRENT_DEFAULT_TRANSFORM_GROUP = 382,
        CURRENT_PATH = 383,
        CURRENT_ROLE = 384,
        CURRENT_ROW = 385,
        CURRENT_TRANSFORM_GROUP_FOR_TYPE = 386,
        CURRENT_USER = 387, // CURSOR see 65
        CURSOR_NAME = 388,
        CYCLE = 389, // DATE see 67
        DATA = 390,
        DATABASE = 391, // Pyrrho
        DATETIME_INTERVAL_CODE = 392,
        DATETIME_INTERVAL_PRECISION = 393,
        DEALLOCATE = 394,
        DECFLOAT = 395,
        DECLARE = 396,
        DEFAULT = 397,
        DEFAULTS = 398,
        DEFERRABLE = 399,
        DEFERRED = 400,
        DEFINE = 401,
        DEFINED = 402,
        DEFINER = 403,
        DEGREE = 404,
        DENSE_RANK = 405,
        DELETE_STATEMENT = 406, // GQL
        DEPTH = 407,
        DEREF = 408,
        DERIVED = 409,
        DESCRIBE = 410,
        DESCRIPTOR = 411,
        DESTINATION = 412, // GQL pre
        DETERMINISTIC = 413,
        DIAGNOSTICS = 414,
        DIRECTED = 415, // GQL
        DISPATCH = 416,
        DISCONNECT = 417,
        DO = 418, // from vol 4
        DOCARRAY = 419, // Pyrrho 5.1
        DOCUMENT = 420, // Pyrrho 5.1
        DOMAIN = 421,
        DROP_GRAPH_STATEMENT = 422, // GQL
        DROP_GRAPH_TYPE_STATEMENT = 423, // GQL
        DROP_SCHEMA_STATEMENT = 424, // GQL
        DYNAMIC_FUNCTION = 425,
        DYNAMIC_FUNCTION_CODE = 426,
        EACH = 427,
        EDGE = 428, // 7.03 EDGETYPE is 461
        EDGES = 429, // GQL
        ELEMENT = 430,
        ELEMENTID = 431, // Pyrrho 7.05
        ELEMENTS = 432, // GQL pre
        ELSEIF = 433, // from vol 4
        EMPTY = 434,
        ENFORCED = 435,
        ENTITY = 436, // Pyrrho 4.5
        END_EXEC = 437, // misprinted in SQL2023 as END-EXEC
        END_FRAME = 438,
        END_PARTITION = 439,
        EOF = 440,	// Pyrrho 0.1
        EQUALS = 441,
        ESCAPE = 442,
        ETAG = 443, // Pyrrho Metadata 7.0
        EVERY = 444,
        EXCLUDE = 445,
        EXCLUDING = 446,
        EXEC = 447,
        EXECUTE = 448,
        EXIT = 449, // from vol 4
        EXTERNAL = 450,
        EXTRACT = 451,
        FETCH = 452,
        FILTER_STATEMENT = 453, // GQL
        FINAL = 454,
        FIRST = 455, // GQL
        FIRST_VALUE = 456,
        FLAG = 457,
        FOLLOWING = 458,
        FOR_STATEMENT = 459, // GQL
        FOREIGN = 460,
        EDGETYPE = 461, // Metadata 7.03 must be 461
        FRAME_ROW = 462,
        FREE = 463,
        FORTRAN = 464,
        FOUND = 465,
        FULL = 466,
        FUNCTION = 467,
        FUSION = 468,
        G = 469,
        GENERAL = 470,
        GENERATED = 471,
        GET = 472,
        GLOBAL = 473,
        GO = 474,
        GOTO = 475,
        GRANT = 476,
        GRANTED = 477,
        GRAPH = 478, //7.03
        GREATEST = 479,
        GROUPING = 480,
        GROUPS = 481,
        HANDLER = 482, // vol 4 
        HIERARCHY = 483,
        HISTOGRAM = 484, // Pyrrho 4.5
        HOLD = 485,
        HTTP = 486,
        HTTPDATE = 487, // Pyrrho 7 RFC 7231
        ID = 488, // Distinguished from the token type Id from Pyrrho 7.05
        IDENTITY = 489,
        IGNORE = 490,
        IMMEDIATE = 491,
        IMMEDIATELY = 492,
        IMPLEMENTATION = 493,
        INCLUDING = 494,
        INDICATOR = 495,
        INITIAL = 496,
        INNER = 497,
        INOUT = 498,
        INSENSITIVE = 499,
        INCREMENT = 500,
        INITIALLY = 501,
        INPUT = 502,
        INSERT_STATEMENT = 503, // GQL
        INSTANCE = 504,
        INSTANTIABLE = 505,
        INSTEAD = 506,
        INTERSECTION = 507, // INTERVAL is 152
        INTO = 508,
        INVERTS = 509, // Pyrrho Metadata 5.7
        INVOKER = 510,
        IRI = 511, // Pyrrho 7
        ISOLATION = 512,
        ITERATE = 513, // vol 4
        METADATA = 514, // must be 514
        JOIN = 515,
        JSON = 516,
        JSON_ARRAY = 517,
        JSON_ARRAYAGG = 518,
        JSON_EXISTS = 519,
        JSON_OBJECT = 520,
        JSON_OBJECTAGG = 521,
        JSON_QUERY = 522,
        JSON_SCALAR = 523,
        JSON_SERIALIZE = 524,
        JSON_TABLE = 525,
        JSON_TABLE_PRIMITIVE = 526,
        JSON_VALUE = 527,
        K = 528,
        KEEP = 529, // GQL
        KEY = 530,
        KEY_MEMBER = 531,
        KEY_TYPE = 532,
        LABEL = 533,  // GQL
        NODETYPE = 534, // Metadata 7.03 must be 534
        LABELLED = 535, // GQL
        LABELS = 536, // Pyrrho 7.05
        LAG = 537,
        LANGUAGE = 538,
        LARGE = 539,
        LAST = 540,
        LAST_DATA = 541, // Pyrrho v7
        LAST_VALUE = 542,
        LATERAL = 543,
        LEAD = 544,
        LEAST = 545,
        LEAVE = 546, // vol 4
        LEAVING = 547, // Pyrrho Metadata 7.07
        LEGEND = 548, // Pyrrho Metadata 4.8
        LENGTH = 549,
        LET_STATEMENT = 550, //GQL
        LEVEL = 551,
        LIKE_REGEX = 552,
        LINE = 553, // Pyrrho 4.5
        LISTAGG = 554,
        LOCALTIME = 555,
        LOCALTIMESTAMP = 556,
        LOCATOR = 557,
        LONGEST = 558, // Pyrrho 7.09 added to GQL
        LOOP = 559,  // vol 4
        LPAD = 560,
        M = 561,
        MAP = 562,
        MATCH_STATEMENT = 563, // GQL
        MATCH_RECOGNIZE = 564,
        MATCHED = 565,
        MATCHES = 566,
        MEMBER = 567,
        MERGE = 568,
        METHOD = 569,
        MAXVALUE = 570,
        MESSAGE_LENGTH = 571,
        MESSAGE_OCTET_LENGTH = 572,
        MESSAGE_TEXT = 573, // METADATA is 514
        MILLI = 574, // Pyrrho 7
        MIME = 575, // Pyrrho 7
        MINVALUE = 576,
        MONOTONIC = 577, // Pyrrho 5.7
        MORE = 578,
        MULTIPLICITY = 579, // Pyrrho 7.03
        MUMPS = 580,
        NAME = 581,
        NAMES = 582,
        NESTING = 583,
        MATCH_NUMBER = 584,
        MODIFIES = 585,
        MODULE = 586,
        NATIONAL = 587,
        NATURAL = 588, // NCHAR 171 NCLOB 172
        NEW = 589,
        NFC = 590,  // GQL Normalization forms
        NFD = 591,  // GQL
        NFKC = 592,  // GQL
        NFKD = 593,  // GQL
        NO = 594,
        NODE = 595, // 7.03 NODETYPE is 534
        NONE = 596,
        NORMALIZED = 597,
        NULLABLE = 598,
        NUMBER = 599,
        NTH_VALUE = 600,
        NTILE = 601, // NULL is 177
        OBJECT = 602,
        OCCURRENCE = 603,
        OCCURRENCES_REGEX = 604,
        OCTETS = 605,
        OLD = 606,
        OMIT = 607,
        ON = 608,
        ONE = 609,
        ONLY = 610,
        OPEN = 611,
        OPTION = 612,
        OPTIONS = 613,
        ORDER_BY_AND_PAGE_STATEMENT = 614, // GQL
        ORDERING = 615,
        ORDINALITY = 616, // GQL
        OTHERS = 617,
        OUT = 618,
        OUTER = 619,
        OUTPUT = 620,
        OVER = 621,
        OVERLAPS = 622,
        OVERLAY = 623,
        OVERRIDING = 624,
        OWNER = 625, // Pyrrho
        P = 626,
        PAD = 627,
        PARAMETER_MODE = 628,
        PARAMETER_NAME = 629,
        PARAMETER_ORDINAL_POSITION = 630,
        PARAMETER_SPECIFIC_CATALOG = 631,
        PARAMETER_SPECIFIC_NAME = 632,
        PARAMETER_SPECIFIC_SCHEMA = 633,
        PARTIAL = 634,
        PARTITION = 635,
        PASCAL = 636,
        PATTERN = 637,
        PER = 638,
        PERCENT = 639,
        PERCENT_RANK = 640,
        PERIOD = 641,
        PIE = 642, // Pyrrho 4.5
        PLACING = 643,
        PL1 = 644,
        POINTS = 645, // Pyrrho 4.5
        PORTION = 646,// POSITION is 327 but is not a reserved word
        POSITION_REGEX = 647,
        PRECEDES = 648,
        PRECEDING = 649,
        PREFIX = 650, // Pyrrho 7.01
        PREPARE = 651,
        PRESERVE = 652,
        PRIMARY = 653,
        PRIOR = 654,
        PRIVILEGES = 655,
        PROCEDURE = 656,
        PROPERTY = 657, // GQL
        PTF = 658,
        PUBLIC = 659,
        READ = 660,
        RANGE = 661,
        RANK = 662,
        READS = 663,    // REAL 203 (previously 199 see REAL0)
        RECURSIVE = 664,
        REF = 665,
        REFERRED = 666, // 5.2
        REFERS = 667, // 5.2
        RELATIONSHIP = 668, // GQL
        RELATIONSHIPS = 669, // GQL
        RELATIVE = 670,
        REMOVE_STATEMENT = 671, // GQL
        REPEATABLE = 672,
        RESPECT = 673,
        RESTART = 674,
        RESTRICT = 675,
        REFERENCES = 676,
        REFERENCING = 677,
        RELEASE = 678,
        REPEAT = 679, // vol 4
        RESIGNAL = 680, // vol 4
        RESULT = 681,
        RETURN_STATEMENT = 682, // GQL
        RETURNED_CARDINALITY = 683,
        RETURNED_LENGTH = 684,
        RETURNED_OCTET_LENGTH = 685,
        RETURNED_SQLSTATE = 686,
        RETURNS = 687,
        REVOKE = 688,
        ROLE = 689,
        ROLLBACK_COMMAND = 690, // GQL
        ROUTINE = 691,
        ROUTINE_CATALOG = 692,
        ROUTINE_NAME = 693,
        ROUTINE_SCHEMA = 694,
        ROW_COUNT = 695,
        ROW = 696,
        ROW_NUMBER = 697,
        ROWS = 698,
        RPAD = 699,
        RUNNING = 700,
        SAVEPOINT = 701,
        SCALE = 702,
        SCHEMA_NAME = 703,
        SCOPE = 704,
        SCOPE_CATALOG = 705,
        SCOPE_NAME = 706,
        SCOPE_SCHEMA = 707,
        SCROLL = 708,
        SEARCH = 709,
        SECTION = 710,
        SECURITY = 711,
        SELECT_STATEMENT = 712, // GQL
        SELF = 713,
        SENSITIVE = 714,
        SEQUENCE = 715,
        SERIALIZABLE = 716,
        SERVER_NAME = 717,
        SESSION_CLOSE_COMMAND = 718, // GQL
        SESSION_RESET_COMMAND = 719, // GQL
        SESSION_SET_BINDING_TABLE_PARAMETER_COMMAND = 720, // GQL
        SESSION_SET_PROPERTY_GRAPH_COMMAND = 721, // GQL
        SESSION_SET_PROPERTY_GRAPH_PARAMETER_COMMAND = 722, //GQL
        SESSION_SET_SCHEMA_COMMAND = 723, // GQL
        SESSION_SET_TIME_ZONE_COMMAND = 724, // GQL
        SESSION_SET_VALUE_PARAMETER_COMMAND = 725, // GQL
        SET_STATEMENT = 726, // GQL
        SETS = 727,
        SHORTEST = 728,
        SHOW = 729,
        SIGNAL = 730, //vol 4
        SIMILAR = 731,
        SIMPLE = 732,
        SOME = 733,
        SOURCE = 734,
        SPACE = 735,
        SPECIFIC = 736,
        SPECIFIC_NAME = 737,
        SPECIFICTYPE = 738,
        SQL = 739,
        SQLAGENT = 740, // Pyrrho 7
        SQLEXCEPTION = 741,
        SQLSTATE = 742,
        SQLWARNING = 743,
        STANDALONE = 744, // vol 14
        START_TRANSACTION_COMMAND = 745, // GQL
        STATE = 746,
        STATEMENT = 747,
        STATIC = 748,
        STRUCTURE = 749,
        STYLE = 750,
        SUBCLASS_ORIGIN = 751,
        SUBMULTISET = 752,
        SUBSET = 753,
        SUBSTRING = 754, //
        SUBSTRING_REGEX = 755,
        SUCCEEDS = 756,
        SUFFIX = 757, // Pyrrho 7.01
        SYMMETRIC = 758,
        SYSTEM = 759,
        SYSTEM_TIME = 760,
        SYSTEM_USER = 761,  
        T = 762, 
        TABLESAMPLE = 763,// TABLE is 297
        TABLE_NAME = 764,
        TEMP = 765, // GQL
        TEMPORARY = 766,
        TIES = 767,
        TIMEOUT = 768, // Pyrrho
        TIMEZONE_HOUR = 769,
        TIMEZONE_MINUTE = 770,
        TO = 771,
        TOP_LEVEL_COUNT = 772,
        TRAIL = 773,
        TRANSACTION = 774,
        TRANSACTION_ACTIVE = 775,
        TRANSACTIONS_COMMITTED = 776,
        TRANSACTIONS_ROLLED_BACK = 777,
        TRANSFORM = 778,
        TRANSFORMS = 779,
        TRANSLATE = 780,
        TRANSLATE_REGEX = 781,
        TRANSLATION = 782,
        TREAT = 783,
        TRIGGER = 784,
        TRIGGER_CATALOG = 785,
        TRIGGER_NAME = 786,
        TRIGGER_SCHEMA = 787, // TYPE  is 267 but is not a reserved word
        TRIM_ARRAY = 788,
        TRUNCATE = 789,
        TRUNCATING = 790, // Pyrrho 7.07
        TYPE_URI = 791, // Pyrrho
        UESCAPE = 792,
        UNBOUNDED = 793,
        UNCOMMITTED = 794,
        UNDER = 795,
        UNDIRECTED = 796, //GQL
        UNIQUE = 797,
        UNNEST = 798,
        UNTIL = 799, // vol 4
        UPDATE = 800,
        USER = 801,
        USING = 802,
        UNDO = 803,
        UNNAMED = 804,
        URL = 805,  // Pyrrho 7
        USAGE = 806,
        USER_DEFINED_TYPE_CATALOG = 807,
        USER_DEFINED_TYPE_CODE = 808,
        USER_DEFINED_TYPE_NAME = 809,
        USER_DEFINED_TYPE_SCHEMA = 810,
        VALUE_OF = 811,
        VALUES = 812,
        VAR_POP = 813,
        VAR_SAMP = 814,
        VARYING = 815,
        VERSIONING = 816,
        VERTEX = 817, // GQL
        VIEW = 818,
        WHENEVER = 819,
        WHILE = 820, // vol 4
        WIDTH_BUCKET = 821,
        WITHIN = 822,
        WITHOUT = 823,
        WORK = 824,
        WRITE = 825,
        X = 826, // Pyrrho 4.5
        Y = 827, // Pyrrho 4.5
        ZONE = 828
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
            else throw new DBException("22G04");
        }
    }
    /// <summary>
    /// Row Version cookie (Qlx.VERSIONING). See Laiho/Laux 2010.
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

        internal static readonly string[] separator = ["\",\""];

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
            var sb = new StringBuilder();
            var sc = "\"";
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
                sb.Append('"');
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
