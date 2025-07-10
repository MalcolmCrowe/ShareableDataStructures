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
        PASSWORD = 218, // must be 218, Pyrrho v5 (not reserved, deprecated)
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
        DOTTOKEN = 250, // . 5.2 was STOP
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
//        RDFLITERAL = 281, // 
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
        COSINE = 375,
        COVAR_POP = 376,
        COVAR_SAMP = 377,
        CREATE_GRAPH_STATEMENT = 378, // GQL
        CREATE_GRAPH_TYPE_STATEMENT = 379, // GQL
        CREATE_SCHEMA_STATEMENT = 380, // GQL
        CROSS = 381,
        CSV = 382, // Pyrrho 5.5
        CUBE = 383,
        CUME_DIST = 384,
        CURATED = 385, // Pyrrho
        CURRENT = 386,
        CURRENT_CATALOG = 387,
        CURRENT_DEFAULT_TRANSFORM_GROUP = 388,
        CURRENT_PATH = 389,
        CURRENT_ROLE = 390,
        CURRENT_ROW = 391,
        CURRENT_TRANSFORM_GROUP_FOR_TYPE = 392,
        CURRENT_USER = 393, // CURSOR see 65
        CURSOR_NAME = 394,
        CYCLE = 395, // DATE see 67
        DATA = 396,
        DATABASE = 397, // Pyrrho
        DATETIME_INTERVAL_CODE = 398,
        DATETIME_INTERVAL_PRECISION = 399,
        DEALLOCATE = 400,
        DECFLOAT = 401,
        DECLARE = 402,
        DEFAULT = 403,
        DEFAULTS = 404,
        DEFERRABLE = 405,
        DEFERRED = 406,
        DEFINE = 407,
        DEFINED = 408,
        DEFINER = 409,
        DEGREE = 410,
        DENSE_RANK = 411,
        DELETE_STATEMENT = 412, // GQL
        DEPTH = 413,
        DEREF = 414,
        DERIVED = 415,
        DESCRIBE = 416,
        DESCRIPTOR = 417,
        DESTINATION = 418, // GQL pre
        DETERMINISTIC = 419,
        DIAGNOSTICS = 420,
        DIRECTED = 421, // GQL
        DISPATCH = 422,
        DISCONNECT = 423,
        DO = 424, // from vol 4
        DOCARRAY = 425, // Pyrrho 5.1
        DOCUMENT = 426, // Pyrrho 5.1
        DOMAIN = 427,
        DOT = 428,
        DROP_GRAPH_STATEMENT = 429, // GQL
        DROP_GRAPH_TYPE_STATEMENT = 430, // GQL
        DROP_SCHEMA_STATEMENT = 431, // GQL
        DYNAMIC_FUNCTION = 432,
        DYNAMIC_FUNCTION_CODE = 433,
        EACH = 434,
        EDGE = 435, // 7.03 EDGETYPE is 461
        EDGES = 436, // GQL
        ELEMENT = 437,
        ELEMENTID = 438, // Pyrrho 7.05
        ELEMENTS = 439, // GQL pre
        ELSEIF = 440, // from vol 4
        EMPTY = 441,
        ENFORCED = 442,
        ENTITY = 443, // Pyrrho 4.5
        END_EXEC = 444, // misprinted in SQL2023 as END-EXEC
        END_FRAME = 445,
        END_PARTITION = 446,
        EOF = 447,	// Pyrrho 0.1
        EQUALS = 448,
        ESCAPE = 449,
        ETAG = 450, // Pyrrho Metadata 7.0
        EUCLIDEAN = 451,
        EUCLIDEAN_SQUARED = 452,
        EVERY = 453,
        EXCLUDE = 454,
        EXCLUDING = 455,
        EXEC = 456,
        EXECUTE = 457,
        EXIT = 458, // from vol 4
        EXTERNAL = 459,
        EXTRACT = 460,
        EDGETYPE = 461, // Metadata 7.03 must be 461
        FETCH = 462,
        FILTER_STATEMENT = 463, // GQL
        FINAL = 464,
        FIRST = 465, // GQL
        FIRST_VALUE = 466,
        FLAG = 467,
        FOLLOWING = 468,
        FOR_STATEMENT = 469, // GQL
        FOREIGN = 470,
        FRAME_ROW = 471,
        FREE = 472,
        FORTRAN = 473,
        FOUND = 474,
        FULL = 475,
        FUNCTION = 476,
        FUSION = 477,
        G = 478,
        GENERAL = 479,
        GENERATED = 480,
        GET = 481,
        GLOBAL = 482,
        GO = 483,
        GOTO = 484,
        GRANT = 485,
        GRANTED = 486,
        GRAPH = 487, //7.03
        GREATEST = 488,
        GROUPING = 489,
        GROUPS = 490,
        HAMMING = 491,
        HANDLER = 492, // vol 4 
        HIERARCHY = 493,
        HISTOGRAM = 494, // Pyrrho 4.5
        HOLD = 495,
        HTTP = 496,
        HTTPDATE = 497, // Pyrrho 7 RFC 7231
        ID = 498, // Distinguished from the token type Id from Pyrrho 7.05
        IDENTITY = 499,
        IGNORE = 500,
        IMMEDIATE = 501,
        IMMEDIATELY = 502,
        IMPLEMENTATION = 503,
        INCLUDING = 504,
        INDICATOR = 505,
        INITIAL = 506,
        INNER = 507,
        INOUT = 508,
        INSENSITIVE = 509,
        INCREMENT = 510,
        INITIALLY = 511,
        INPUT = 512,
        INSERT_STATEMENT = 513, // GQL
        METADATA = 514, // must be 514
        INSTANCE = 515,
        INSTANTIABLE = 516,
        INSTEAD = 517,
        INTERSECTION = 518, // INTERVAL is 152
        INTO = 519,
        INVERTS = 520, // Pyrrho Metadata 5.7
        INVOKER = 521,
        IRI = 522, // Pyrrho 7
        ISOLATION = 523,
        ITERATE = 524, // vol 4
        JOIN = 525,
        JSON = 526,
        JSON_ARRAY = 527,
        JSON_ARRAYAGG = 528,
        JSON_EXISTS = 529,
        JSON_OBJECT = 530,
        JSON_OBJECTAGG = 531,
        JSON_QUERY = 532,
        JSON_SCALAR = 533,
        NODETYPE = 534, // Metadata 7.03 must be 534
        JSON_SERIALIZE = 535,
        JSON_TABLE = 536,
        JSON_TABLE_PRIMITIVE = 537,
        JSON_VALUE = 538,
        K = 539,
        KEEP = 540, // GQL
        KEY = 541,
        KEY_MEMBER = 542,
        KEY_TYPE = 543,
        LABEL = 544,  // GQL
        LABELLED = 545, // GQL
        LABELS = 546, // Pyrrho 7.05
        LAG = 547,
        LANGUAGE = 548,
        LARGE = 549,
        LAST = 550,
        LAST_DATA = 551, // Pyrrho v7
        LAST_VALUE = 552,
        LATERAL = 553,
        LEAD = 554,
        LEAST = 555,
        LEAVE = 556, // vol 4
        LEAVING = 557, // Pyrrho Metadata 7.07
        LEGEND = 558, // Pyrrho Metadata 4.8
        LENGTH = 559,
        LET_STATEMENT = 560, //GQL
        LEVEL = 561,
        LIKE_REGEX = 562,
        LINE = 563, // Pyrrho 4.5
        LISTAGG = 564,
        LOCALTIME = 565,
        LOCALTIMESTAMP = 566,
        LOCATOR = 567,
        LONGEST = 568, // Pyrrho 7.09 added to GQL
        LOOP = 569,  // vol 4
        LPAD = 570,
        M = 571,
        MANHATTAN = 572,
        MAP = 573,
        MATCH_STATEMENT = 574, // GQL
        MATCH_RECOGNIZE = 575,
        MATCHED = 576,
        MATCHES = 577,
        MEMBER = 578,
        MERGE = 579,
        METHOD = 580,
        MAXVALUE = 581,
        MESSAGE_LENGTH = 582,
        MESSAGE_OCTET_LENGTH = 583,
        MESSAGE_TEXT = 584, // METADATA is 514
        MILLI = 585, // Pyrrho 7
        MIME = 586, // Pyrrho 7
        MINVALUE = 587,
        MONOTONIC = 588, // Pyrrho 5.7
        MORE = 589,
        MULTIPLICITY = 590, // Pyrrho 7.03
        MUMPS = 591,
        NAME = 592,
        NAMES = 593,
        NESTING = 594,
        MATCH_NUMBER = 595,
        MODIFIES = 596,
        MODULE = 597,
        NATIONAL = 598,
        NATURAL = 599, // NCHAR 171 NCLOB 172
        NEW = 600,
        NFC = 601,  // GQL Normalization forms
        NFD = 602,  // GQL
        NFKC = 603,  // GQL
        NFKD = 604,  // GQL
        NO = 605,
        NODE = 606, // 7.03 NODETYPE is 534
        NONE = 607,
        NORMALIZED = 608,
        NULLABLE = 609,
        NUMBER = 610,
        NTH_VALUE = 611,
        NTILE = 612, // NULL is 177
        OBJECT = 613,
        OCCURRENCE = 614,
        OCCURRENCES_REGEX = 615,
        OCTETS = 616,
        OLD = 617,
        OMIT = 618,
        ON = 619,
        ONE = 620,
        ONLY = 621,
        OPEN = 622,
        OPTION = 623,
        OPTIONS = 624,
        ORDER_BY_AND_PAGE_STATEMENT = 625, // GQL
        ORDERING = 626,
        ORDINALITY = 627, // GQL
        OTHERS = 628,
        OUT = 629,
        OUTER = 630,
        OUTPUT = 631,
        OVER = 632,
        OVERLAPS = 633,
        OVERLAY = 634,
        OVERRIDING = 635,
        OWNER = 636, // Pyrrho
        P = 637,
        PAD = 638,
        PARAMETER_MODE = 639,
        PARAMETER_NAME = 640,
        PARAMETER_ORDINAL_POSITION = 641,
        PARAMETER_SPECIFIC_CATALOG = 642,
        PARAMETER_SPECIFIC_NAME = 643,
        PARAMETER_SPECIFIC_SCHEMA = 644,
        PARTIAL = 645,
        PARTITION = 646,
        PASCAL = 647,
        PATTERN = 648,
        PER = 649,
        PERCENT = 650,
        PERCENT_RANK = 651,
        PERIOD = 652,
        PIE = 653, // Pyrrho 4.5
        PLACING = 654,
        PL1 = 655,
        POINTS = 656, // Pyrrho 4.5
        PORTION = 657,// POSITION is 327 but is not a reserved word
        POSITION_REGEX = 658,
        PRECEDES = 659,
        PRECEDING = 660,
        PREFIX = 661, // Pyrrho 7.01
        PREPARE = 662,
        PRESERVE = 663,
        PRIMARY = 664,
        PRIOR = 665,
        PRIVILEGES = 666,
        PROCEDURE = 667,
        PROPERTY = 668, // GQL
        PTF = 669,
        PUBLIC = 670,
        READ = 671,
        RANGE = 672,
        RANK = 673,
        READS = 674,    // REAL 203 (previously 199 see REAL0)
        RECURSIVE = 675,
        REF = 676,
        REFERRED = 677, // 5.2
        REFERS = 678, // 5.2
        RELATIONSHIP = 679, // GQL
        RELATIONSHIPS = 680, // GQL
        RELATIVE = 681,
        REMOVE_STATEMENT = 682, // GQL
        REPEATABLE = 683,
        RESPECT = 684,
        RESTART = 685,
        RESTRICT = 686,
        REFERENCES = 687,
        REFERENCING = 688,
        RELEASE = 689,
        REPEAT = 690, // vol 4
        RESIGNAL = 691, // vol 4
        RESULT = 692,
        RETURN_STATEMENT = 693, // GQL
        RETURNED_CARDINALITY = 694,
        RETURNED_LENGTH = 695,
        RETURNED_OCTET_LENGTH = 696,
        RETURNED_SQLSTATE = 697,
        RETURNING = 698,
        RETURNS = 699,
        REVOKE = 700,
        ROLE = 701,
        ROLLBACK_COMMAND = 702, // GQL
        ROUTINE = 703,
        ROUTINE_CATALOG = 704,
        ROUTINE_NAME = 705,
        ROUTINE_SCHEMA = 706,
        ROW_COUNT = 707,
        ROW = 708,
        ROW_NUMBER = 709,
        ROWS = 710,
        RPAD = 711,
        RUNNING = 712,
        SAVEPOINT = 713,
        SCALE = 714,
        SCHEMA_NAME = 715,
        SCOPE = 716,
        SCOPE_CATALOG = 717,
        SCOPE_NAME = 718,
        SCOPE_SCHEMA = 719,
        SCROLL = 720,
        SEARCH = 721,
        SECTION = 722,
        SECURITY = 723,
        SELECT_STATEMENT = 724, // GQL
        SELF = 725,
        SENSITIVE = 726,
        SEQUENCE = 727,
        SERIALIZABLE = 728,
        SERVER_NAME = 729,
        SESSION_CLOSE_COMMAND = 730, // GQL
        SESSION_RESET_COMMAND = 731, // GQL
        SESSION_SET_BINDING_TABLE_PARAMETER_COMMAND = 732, // GQL
        SESSION_SET_PROPERTY_GRAPH_COMMAND = 733, // GQL
        SESSION_SET_PROPERTY_GRAPH_PARAMETER_COMMAND = 734, //GQL
        SESSION_SET_SCHEMA_COMMAND = 735, // GQL
        SESSION_SET_TIME_ZONE_COMMAND = 736, // GQL
        SESSION_SET_VALUE_PARAMETER_COMMAND = 737, // GQL
        SET_STATEMENT = 738, // GQL
        SETS = 739,
        SHORTEST = 740,
        SHOW = 741,
        SIGNAL = 742, //vol 4
        SIMILAR = 743,
        SIMPLE = 744,
        SOME = 745,
        SOURCE = 746,
        SPACE = 747,
        SPECIFIC = 748,
        SPECIFIC_NAME = 749,
        SPECIFICTYPE = 750,
        SQL = 751,
        SQLAGENT = 752, // Pyrrho 7
        SQLEXCEPTION = 753,
        SQLSTATE = 754,
        SQLWARNING = 755,
        STANDALONE = 756, // vol 14
        START_TRANSACTION_COMMAND = 757, // GQL
        STATE = 758,
        STATEMENT = 759,
        STATIC = 760,
        STRUCTURE = 761,
        STYLE = 762,
        SUBCLASS_ORIGIN = 763,
        SUBMULTISET = 764,
        SUBSET = 765,
        SUBSTRING = 766, //
        SUBSTRING_REGEX = 767,
        SUCCEEDS = 768,
        SUFFIX = 769, // Pyrrho 7.01
        SYMMETRIC = 770,
        SYSTEM = 771,
        SYSTEM_TIME = 772,
        SYSTEM_USER = 773,  
        T = 774, 
        TABLESAMPLE = 775,// TABLE is 297
        TABLE_NAME = 776,
        TEMP = 777, // GQL
        TEMPORARY = 778,
        TIES = 779,
        TIMEOUT = 780, // Pyrrho
        TIMEZONE_HOUR = 781,
        TIMEZONE_MINUTE = 782,
        TO = 783,
        TOP_LEVEL_COUNT = 784,
        TRAIL = 785,
        TRANSACTION = 786,
        TRANSACTION_ACTIVE = 787,
        TRANSACTIONS_COMMITTED = 788,
        TRANSACTIONS_ROLLED_BACK = 789,
        TRANSFORM = 790,
        TRANSFORMS = 791,
        TRANSLATE = 792,
        TRANSLATE_REGEX = 793,
        TRANSLATION = 794,
        TREAT = 795,
        TRIGGER = 796,
        TRIGGER_CATALOG = 797,
        TRIGGER_NAME = 798,
        TRIGGER_SCHEMA = 799, // TYPE  is 267 but is not a reserved word
        TRIM_ARRAY = 800,
        TRUNCATE = 801,
        TRUNCATING = 802, // Pyrrho 7.07
        TYPE_URI = 803, // Pyrrho
        UESCAPE = 804,
        UNBOUNDED = 805,
        UNCOMMITTED = 806,
        UNDER = 807,
        UNDIRECTED = 808, //GQL
        UNIQUE = 809,
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
        VERSION = 828, // row-versioning: new for Pyrrho 7.09 April 2025
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
    /// CheckFields allows transactions to find out if another transaction has overritten the row.
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
