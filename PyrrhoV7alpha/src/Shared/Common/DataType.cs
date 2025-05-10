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
        VECTOR = 220,
        VECTOR_DIMENSION_COUNT = 221,
        VECTOR_DISTANCE = 222,
        VECTOR_NORM = 223,
        VECTOR_SERIALIZE = 224,
        WHEN = 225,
        WHERE = 226,
        WITH = 227,
        XOR = 228,
        YEAR = 229,
        YIELD = 230, 
        ZONED = 231, 
        ZONED_DATETIME = 232, 
        ZONED_TIME = 233, // last reserved word
        //====================TOKEN TYPES=====================
        AMPERSAND = 234, // for GQL label expression
        ARROW = 235, // ]-> GQL bracket right arrow 
        ARROWL = 236, // <- GQL left arrow
        ARROWLTILDE = 237, // <~ GQL left arrow tilde
        ARROWR = 238, // -> GQL right arrow
        ARROWRTILDE = 239, // ~> GQL tilde right arrow
        ARROWTILDE = 240, // ]~> GQL bracket tilde right arrow
        ARROWBASE = 241, // -[ GQL minus left bracket
        ARROWBASETILDE = 242, // ~[ GQL tilde left bracket
        BLOBLITERAL = 243, // 
        BOOLEANLITERAL = 244,
        CHARLITERAL = 245, //
        COLON = 246, // :
        COMMA = 247,  // ,        
        CONCATENATE = 248, // ||        
        DIVIDE = 249, // /        
        DOT = 250, // . 5.2 was STOP
        DOUBLEARROW = 251, // => GQL right double arrow
        DOUBLECOLON = 252, // ::
        DOUBLEPERIOD = 253, // ..
        EQL = 254,  // =
        SET = 255,  // must be 255 (is reserved word)
        EXCLAMATION = 256, // !  GQL
        TIME = 257, // must be 257 (is reserved word)
        TIMESTAMP = 258, // must be 258
        GEQ = 259, // >=    
        GTR = 260, // >    
        Id = 261, // identifier
        INTEGERLITERAL = 262, // Pyrrho
        LBRACE = 263, // {
        LBRACK = 264, // [
        LEQ = 265, // <=
        LPAREN = 266, // (
        TYPE = 267, // must be 267
        LSS = 268, // <
        MINUS = 269, // -
        NEQ = 270, // <>
        NUMERICLITERAL = 271, // 
        PLUS = 272, // + 
        QMARK = 273, // ?
        RARROW = 274, // <-[ GQL left arrow bracket
        RARROWTILDE = 275, // <~[ GQL left arrow tilde bracket
        RARROWBASE = 276, // ]- GQL right bracket minus
        RBRACE = 277, // } 
        RBRACK = 278, // ] 
        RBRACKTILDE = 279, // ]~ GQL right brancket tilde
        RDFDATETIME = 280, //
        RDFLITERAL = 281, // 
        RDFTYPE = 282, // Pyrrho 7.0
        REALLITERAL = 283, //
        RPAREN = 284, // ) 
        SEMICOLON = 285, // ; 
        TILDE = 286, // ~ GQL
        TIMES = 287, // *
        VBAR = 288, // | 
        VPLUS = 289, // |+| GQL multiset alternation operator
        //=========================NON-RESERVED WORDS================
        A = 290, // first non-reserved word
        ABSENT = 291,
        ABSOLUTE = 292,
        ACTION = 293,
        ACYCLIC = 294,
        ADA = 295,
        ADD = 296,
        TABLE = 297, // must be 297 
        ADMIN = 298,
        AFTER = 299,
        ALLOCATE = 300,
        ALTER = 301, 
        ALWAYS = 302,
        ANY_VALUE = 303,
        APPLICATION = 304, // Pyrrho 4.6
        ARE = 305,
        ARRAY_AG = 306,
        ARRAY_MAX_CARDINALITY = 307,
        ARRIVING = 308, // Pyrrho Metadata 7.07 deprecated
        ASENSITIVE = 309,
        ASSERTION = 310,
        ASYMMETRIC = 311,
        ATOMIC = 312,
        ATTRIBUTE = 313,
        ATTRIBUTES = 314,
        AUTHORIZATION = 315,
        BEFORE = 316,
        BEGIN = 317,
        BEGIN_FRAME = 318,
        BEGIN_PARTITION = 319,
        BERNOULLI = 320,
        BETWEEN = 321,
        BINDING = 322, // GQL
        BINDINGS = 323, // GQL
        BLOB = 324,
        BREADTH = 325,
        BREAK = 326, // Pyrrho
        POSITION = 327, // Pyrrho 7.08 must be 327
        C = 328,
        CALL_PROCEDURE_STATEMENT = 329, // GQL
        CALLED = 330,
        CAPTION = 331, // Pyrrho 4.5
        CASCADE = 332,
        CASCADED = 333,
        CATALOG = 334,
        CATALOG_NAME = 335,
        CHAIN = 336,
        CHARACTER = 337,
        CHARACTER_SET_CATALOG = 338,
        CHARACTER_SET_NAME = 339,
        CHARACTER_SET_SCHEMA = 340,
        CHARACTERS = 341,
        CHECK = 342,
        CLASS_ORIGIN = 343, // CLOB see 40
        COBOL = 344, 
        COLLATE = 345,
        COLLATION = 346,
        COLLATION_CATALOG = 347,
        COLLATION_NAME = 348,
        COLLATION_SCHEMA = 349,
        COLLECT = 350,
        COLUMN = 351,
        COLUMN_NAME = 352,
        COMMAND_FUNCTION = 353,
        COMMAND_FUNCTION_CODE = 354,
        COMMIT_COMMAND = 355, // GQL
        COMMITTED = 356,
        CONDITION = 357,
        CONDITION_NUMBER = 358,
        CONNECT = 359,
        CONNECTING = 360, // GQL
        CONNECTION = 361,
        CONNECTION_NAME = 362,
        CONSTRAINT = 363,
        CONSTRAINT_CATALOG = 364,
        CONSTRAINT_NAME = 365,
        CONSTRAINT_SCHEMA = 366,
        CONSTRAINTS = 367,
        CONSTRUCTOR = 368,
        CONTAINS = 369,
        CONVERT = 370,
        CONTENT = 371,
        CONTINUE = 372,
        CORR = 373,
        CORRESPONDING = 374,
        COVAR_POP = 375,
        COVAR_SAMP = 376,
        CREATE_GRAPH_STATEMENT = 377, // GQL
        CREATE_GRAPH_TYPE_STATEMENT = 378, // GQL
        CREATE_SCHEMA_STATEMENT = 379, // GQL
        CROSS = 380,
        CSV = 381, // Pyrrho 5.5
        CUBE = 382,
        CUME_DIST = 383,
        CURATED = 384, // Pyrrho
        CURRENT = 385,
        CURRENT_CATALOG = 386,
        CURRENT_DEFAULT_TRANSFORM_GROUP = 387,
        CURRENT_PATH = 388,
        CURRENT_ROLE = 389,
        CURRENT_ROW = 390,
        CURRENT_TRANSFORM_GROUP_FOR_TYPE = 391,
        CURRENT_USER = 392, // CURSOR see 65
        CURSOR_NAME = 393,
        CYCLE = 394, // DATE see 67
        DATA = 395,
        DATABASE = 396, // Pyrrho
        DATETIME_INTERVAL_CODE = 397,
        DATETIME_INTERVAL_PRECISION = 398,
        DEALLOCATE = 399,
        DECFLOAT = 400,
        DECLARE = 401,
        DEFAULT = 402,
        DEFAULTS = 403,
        DEFERRABLE = 404,
        DEFERRED = 405,
        DEFINE = 406,
        DEFINED = 407,
        DEFINER = 408,
        DEGREE = 409,
        DENSE_RANK = 410,
        DELETE_STATEMENT = 411, // GQL
        DEPTH = 412,
        DEREF = 413,
        DERIVED = 414,
        DESCRIBE = 415,
        DESCRIPTOR = 416,
        DESTINATION = 417, // GQL pre
        DETERMINISTIC = 418,
        DIAGNOSTICS = 419,
        DIRECTED = 420, // GQL
        DISPATCH = 421,
        DISCONNECT = 422,
        DO = 423, // from vol 4
        DOCARRAY = 424, // Pyrrho 5.1
        DOCUMENT = 425, // Pyrrho 5.1
        DOMAIN = 426,
        DROP_GRAPH_STATEMENT = 427, // GQL
        DROP_GRAPH_TYPE_STATEMENT = 428, // GQL
        DROP_SCHEMA_STATEMENT = 429, // GQL
        DYNAMIC_FUNCTION = 430,
        DYNAMIC_FUNCTION_CODE = 431,
        EACH = 432,
        EDGE = 433, // 7.03 EDGETYPE is 461
        EDGES = 434, // GQL
        ELEMENT = 435,
        ELEMENTID = 436, // Pyrrho 7.05
        ELEMENTS = 437, // GQL pre
        ELSEIF = 438, // from vol 4
        EMPTY = 439,
        ENFORCED = 440,
        ENTITY = 441, // Pyrrho 4.5
        END_EXEC = 442, // misprinted in SQL2023 as END-EXEC
        END_FRAME = 443,
        END_PARTITION = 444,
        EOF = 445,	// Pyrrho 0.1
        EQUALS = 446,
        ESCAPE = 447,
        ETAG = 448, // Pyrrho Metadata 7.0
        EVERY = 449,
        EXCLUDE = 450,
        EXCLUDING = 451,
        EXEC = 452,
        EXECUTE = 453,
        EXIT = 454, // from vol 4
        EXTERNAL = 455,
        EXTRACT = 456,
        FETCH = 457,
        FILTER_STATEMENT = 458, // GQL
        FINAL = 459,
        FIRST = 460, // GQL
        EDGETYPE = 461, // Metadata 7.03 must be 461
        FIRST_VALUE = 462,
        FLAG = 463,
        FOLLOWING = 464,
        FOR_STATEMENT = 465, // GQL
        FOREIGN = 466,
        FRAME_ROW = 467,
        FREE = 468,
        FORTRAN = 469,
        FOUND = 470,
        FULL = 471,
        FUNCTION = 472,
        FUSION = 473,
        G = 474,
        GENERAL = 475,
        GENERATED = 476,
        GET = 477,
        GLOBAL = 478,
        GO = 479,
        GOTO = 480,
        GRANT = 481,
        GRANTED = 482,
        GRAPH = 483, //7.03
        GREATEST = 484,
        GROUPING = 485,
        GROUPS = 486,
        HANDLER = 487, // vol 4 
        HIERARCHY = 488,
        HISTOGRAM = 489, // Pyrrho 4.5
        HOLD = 490,
        HTTP = 491,
        HTTPDATE = 492, // Pyrrho 7 RFC 7231
        ID = 493, // Distinguished from the token type Id from Pyrrho 7.05
        IDENTITY = 494,
        IGNORE = 495,
        IMMEDIATE = 496,
        IMMEDIATELY = 497,
        IMPLEMENTATION = 498,
        INCLUDING = 499,
        INDICATOR = 500,
        INITIAL = 501,
        INNER = 502,
        INOUT = 503,
        INSENSITIVE = 504,
        INCREMENT = 505,
        INITIALLY = 506,
        INPUT = 507,
        INSERT_STATEMENT = 508, // GQL
        INSTANCE = 509,
        INSTANTIABLE = 510,
        INSTEAD = 511,
        INTERSECTION = 512, // INTERVAL is 152
        INTO = 513,
        METADATA = 514, // must be 514
        INVERTS = 515, // Pyrrho Metadata 5.7
        INVOKER = 516,
        IRI = 517, // Pyrrho 7
        ISOLATION = 518,
        ITERATE = 519, // vol 4
        JOIN = 520,
        JSON = 521,
        JSON_ARRAY = 522,
        JSON_ARRAYAGG = 523,
        JSON_EXISTS = 524,
        JSON_OBJECT = 525,
        JSON_OBJECTAGG = 526,
        JSON_QUERY = 527,
        JSON_SCALAR = 528,
        JSON_SERIALIZE = 529,
        JSON_TABLE = 530,
        JSON_TABLE_PRIMITIVE = 531,
        JSON_VALUE = 532,
        K = 533,
        NODETYPE = 534, // Metadata 7.03 must be 534
        KEEP = 535, // GQL
        KEY = 536,
        KEY_MEMBER = 537,
        KEY_TYPE = 538,
        LABEL = 539,  // GQL
        LABELLED = 540, // GQL
        LABELS = 541, // Pyrrho 7.05
        LAG = 542,
        LANGUAGE = 543,
        LARGE = 544,
        LAST = 545,
        LAST_DATA = 546, // Pyrrho v7
        LAST_VALUE = 547,
        LATERAL = 548,
        LEAD = 549,
        LEAST = 550,
        LEAVE = 551, // vol 4
        LEAVING = 552, // Pyrrho Metadata 7.07
        LEGEND = 553, // Pyrrho Metadata 4.8
        LENGTH = 554,
        LET_STATEMENT = 555, //GQL
        LEVEL = 556,
        LIKE_REGEX = 557,
        LINE = 558, // Pyrrho 4.5
        LISTAGG = 559,
        LOCALTIME = 560,
        LOCALTIMESTAMP = 561,
        LOCATOR = 562,
        LONGEST = 563, // Pyrrho 7.09 added to GQL
        LOOP = 564,  // vol 4
        LPAD = 565,
        M = 566,
        MAP = 567,
        MATCH_STATEMENT = 568, // GQL
        MATCH_RECOGNIZE = 569,
        MATCHED = 570,
        MATCHES = 571,
        MEMBER = 572,
        MERGE = 573,
        METHOD = 574,
        MAXVALUE = 575,
        MESSAGE_LENGTH = 576,
        MESSAGE_OCTET_LENGTH = 577,
        MESSAGE_TEXT = 578, // METADATA is 514
        MILLI = 579, // Pyrrho 7
        MIME = 580, // Pyrrho 7
        MINVALUE = 581,
        MONOTONIC = 582, // Pyrrho 5.7
        MORE = 583,
        MULTIPLICITY = 584, // Pyrrho 7.03
        MUMPS = 585,
        NAME = 586,
        NAMES = 587,
        NESTING = 588,
        MATCH_NUMBER = 589,
        MODIFIES = 590,
        MODULE = 591,
        NATIONAL = 592,
        NATURAL = 593, // NCHAR 171 NCLOB 172
        NEW = 594,
        NFC = 595,  // GQL Normalization forms
        NFD = 596,  // GQL
        NFKC = 597,  // GQL
        NFKD = 598,  // GQL
        NO = 599,
        NODE = 600, // 7.03 NODETYPE is 534
        NONE = 601,
        NORMALIZED = 602,
        NULLABLE = 603,
        NUMBER = 604,
        NTH_VALUE = 605,
        NTILE = 606, // NULL is 177
        OBJECT = 607,
        OCCURRENCE = 608,
        OCCURRENCES_REGEX = 609,
        OCTETS = 610,
        OLD = 611,
        OMIT = 612,
        ON = 613,
        ONE = 614,
        ONLY = 615,
        OPEN = 616,
        OPTION = 617,
        OPTIONS = 618,
        ORDER_BY_AND_PAGE_STATEMENT = 619, // GQL
        ORDERING = 620,
        ORDINALITY = 621, // GQL
        OTHERS = 622,
        OUT = 623,
        OUTER = 624,
        OUTPUT = 625,
        OVER = 626,
        OVERLAPS = 627,
        OVERLAY = 628,
        OVERRIDING = 629,
        OWNER = 630, // Pyrrho
        P = 631,
        PAD = 632,
        PARAMETER_MODE = 633,
        PARAMETER_NAME = 634,
        PARAMETER_ORDINAL_POSITION = 635,
        PARAMETER_SPECIFIC_CATALOG = 636,
        PARAMETER_SPECIFIC_NAME = 637,
        PARAMETER_SPECIFIC_SCHEMA = 638,
        PARTIAL = 639,
        PARTITION = 640,
        PASCAL = 641,
        PATTERN = 642,
        PER = 643,
        PERCENT = 644,
        PERCENT_RANK = 645,
        PERIOD = 646,
        PIE = 647, // Pyrrho 4.5
        PLACING = 647,
        PL1 = 648,
        POINTS = 650, // Pyrrho 4.5
        PORTION = 651,// POSITION is 327 but is not a reserved word
        POSITION_REGEX = 652,
        PRECEDES = 653,
        PRECEDING = 654,
        PREFIX = 655, // Pyrrho 7.01
        PREPARE = 656,
        PRESERVE = 657,
        PRIMARY = 658,
        PRIOR = 659,
        PRIVILEGES = 660,
        PROCEDURE = 661,
        PROPERTY = 662, // GQL
        PTF = 663,
        PUBLIC = 664,
        READ = 665,
        RANGE = 666,
        RANK = 667,
        READS = 668,    // REAL 203 (previously 199 see REAL0)
        RECURSIVE = 669,
        REF = 670,
        REFERRED = 671, // 5.2
        REFERS = 672, // 5.2
        RELATIONSHIP = 673, // GQL
        RELATIONSHIPS = 674, // GQL
        RELATIVE = 675,
        REMOVE_STATEMENT = 676, // GQL
        REPEATABLE = 677,
        RESPECT = 678,
        RESTART = 679,
        RESTRICT = 680,
        REFERENCES = 681,
        REFERENCING = 682,
        RELEASE = 683,
        REPEAT = 684, // vol 4
        RESIGNAL = 685, // vol 4
        RESULT = 686,
        RETURN_STATEMENT = 687, // GQL
        RETURNED_CARDINALITY = 688,
        RETURNED_LENGTH = 689,
        RETURNED_OCTET_LENGTH = 690,
        RETURNED_SQLSTATE = 691,
        RETURNS = 692,
        REVOKE = 693,
        ROLE = 694,
        ROLLBACK_COMMAND = 695, // GQL
        ROUTINE = 696,
        ROUTINE_CATALOG = 697,
        ROUTINE_NAME = 698,
        ROUTINE_SCHEMA = 699,
        ROW_COUNT = 700,
        ROW = 701,
        ROW_NUMBER = 702,
        ROWS = 703,
        RPAD = 704,
        RUNNING = 705,
        SAVEPOINT = 706,
        SCALE = 707,
        SCHEMA_NAME = 708,
        SCOPE = 709,
        SCOPE_CATALOG = 710,
        SCOPE_NAME = 711,
        SCOPE_SCHEMA = 712,
        SCROLL = 713,
        SEARCH = 714,
        SECTION = 715,
        SECURITY = 716,
        SELECT_STATEMENT = 717, // GQL
        SELF = 718,
        SENSITIVE = 719,
        SEQUENCE = 720,
        SERIALIZABLE = 721,
        SERVER_NAME = 722,
        SESSION_CLOSE_COMMAND = 723, // GQL
        SESSION_RESET_COMMAND = 724, // GQL
        SESSION_SET_BINDING_TABLE_PARAMETER_COMMAND = 725, // GQL
        SESSION_SET_PROPERTY_GRAPH_COMMAND = 726, // GQL
        SESSION_SET_PROPERTY_GRAPH_PARAMETER_COMMAND = 727, //GQL
        SESSION_SET_SCHEMA_COMMAND = 728, // GQL
        SESSION_SET_TIME_ZONE_COMMAND = 729, // GQL
        SESSION_SET_VALUE_PARAMETER_COMMAND = 730, // GQL
        SET_STATEMENT = 731, // GQL
        SETS = 732,
        SHORTEST = 733,
        SHOW = 734,
        SIGNAL = 735, //vol 4
        SIMILAR = 736,
        SIMPLE = 737,
        SOME = 738,
        SOURCE = 739,
        SPACE = 740,
        SPECIFIC = 741,
        SPECIFIC_NAME = 742,
        SPECIFICTYPE = 743,
        SQL = 744,
        SQLAGENT = 745, // Pyrrho 7
        SQLEXCEPTION = 746,
        SQLSTATE = 747,
        SQLWARNING = 748,
        STANDALONE = 749, // vol 14
        START_TRANSACTION_COMMAND = 750, // GQL
        STATE = 751,
        STATEMENT = 752,
        STATIC = 753,
        STRUCTURE = 754,
        STYLE = 755,
        SUBCLASS_ORIGIN = 756,
        SUBMULTISET = 757,
        SUBSET = 758,
        SUBSTRING = 759, //
        SUBSTRING_REGEX = 760,
        SUCCEEDS = 761,
        SUFFIX = 762, // Pyrrho 7.01
        SYMMETRIC = 763,
        SYSTEM = 764,
        SYSTEM_TIME = 765,
        SYSTEM_USER = 766,  
        T = 767, 
        TABLESAMPLE = 768,// TABLE is 297
        TABLE_NAME = 769,
        TEMP = 770, // GQL
        TEMPORARY = 771,
        TIES = 772,
        TIMEOUT = 773, // Pyrrho
        TIMEZONE_HOUR = 774,
        TIMEZONE_MINUTE = 775,
        TO = 776,
        TOP_LEVEL_COUNT = 777,
        TRAIL = 778,
        TRANSACTION = 779,
        TRANSACTION_ACTIVE = 780,
        TRANSACTIONS_COMMITTED = 781,
        TRANSACTIONS_ROLLED_BACK = 782,
        TRANSFORM = 783,
        TRANSFORMS = 784,
        TRANSLATE = 785,
        TRANSLATE_REGEX = 786,
        TRANSLATION = 787,
        TREAT = 788,
        TRIGGER = 789,
        TRIGGER_CATALOG = 790,
        TRIGGER_NAME = 791,
        TRIGGER_SCHEMA = 792, // TYPE  is 267 but is not a reserved word
        TRIM_ARRAY = 793,
        TRUNCATE = 794,
        TRUNCATING = 795, // Pyrrho 7.07
        TYPE_URI = 796, // Pyrrho
        UESCAPE = 797,
        UNBOUNDED = 798,
        UNCOMMITTED = 799,
        UNDER = 800,
        UNDIRECTED = 801, //GQL
        UNIQUE = 802,
        UNNEST = 803,
        UNTIL = 804, // vol 4
        UPDATE = 805,
        USER = 806,
        USING = 807,
        UNDO = 808,
        UNNAMED = 809,
        URL = 810,  // Pyrrho 7
        USAGE = 811,
        USER_DEFINED_TYPE_CATALOG = 812,
        USER_DEFINED_TYPE_CODE = 813,
        USER_DEFINED_TYPE_NAME = 814,
        USER_DEFINED_TYPE_SCHEMA = 815,
        VALUE_OF = 816,
        VALUES = 817,
        VAR_POP = 818,
        VAR_SAMP = 819,
        VARYING = 820,
        VERSION = 821, // row-versioning: new for Pyrrho 7.09 April 2025
        VERSIONING = 822,
        VERTEX = 823, // GQL
        VIEW = 824,
        WHENEVER = 825,
        WHILE = 826, // vol 4
        WIDTH_BUCKET = 827,
        WITHIN = 828,
        WITHOUT = 829,
        WORK = 830,
        WRITE = 831,
        X = 832, // Pyrrho 4.5
        Y = 833, // Pyrrho 4.5
        ZONE = 834
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
