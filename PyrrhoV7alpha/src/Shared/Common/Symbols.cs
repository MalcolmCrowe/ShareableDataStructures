using System;
using System.Collections.Generic;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland
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
        ABS =1,
		ALL =2,	
		ALLOCATE =3,	
		ALTER =4,			
		AND =5,    
		ANY =6,	
		ARE =7, // ARRAY see 11
        ARRAY_AGG = 8,
        ARRAY_MAX_CARDINALITY = 9,
		AS =10,
		ARRAY =11 , // must be 11
        ASENSITIVE =12,
        ASYMMETRIC =13,
        AT =14,
		ATOMIC =15,
		AUTHORIZATION =16,
		AVG =17, //
		BEGIN =18,
        BEGIN_FRAME = 19,
        BEGIN_PARTITION = 20,
		BETWEEN =21,
		BIGINT =22,
		BINARY =23,
		BLOB =24, // BOOLEAN see 27
		BOTH =25,
		BY =26,
		BOOLEAN = 27, // must be 27
        CALL =28,
		CALLED =29,
        CARDINALITY =30,
        CASCADED = 31, 
        CASE = 32,
		CAST =33,
        CEIL =34,
        CEILING =35, // CHAR see 37
		CHAR_LENGTH =36,
		CHAR = 37, // must be 37: see also CHARLITERAL for literal 
        CHARACTER =38,
        CHARACTER_LENGTH =39,
		CLOB = 40, // must be 40
        CHECK =41, // CLOB see 40
        CLOSE = 42, 
        COALESCE = 43,
        COLLATE = 44, 
        COLLECT = 45,
		COLUMN =46,
		COMMIT =47,
		CONDITION =48,
		CONNECT =49,
		CONSTRAINT =50,
        CONTAINS = 51,
        CONVERT =52,
#if OLAP
        CORR =53,
#endif
		CORRESPONDING =54,
		COUNT =55, //
#if OLAP
        COVAR_POP =56,
        COVAR_SAMP =57,
#endif
		CREATE =58,
		CROSS =59,
#if OLAP
        CUBE =60,
        CUME_DIST =61,
#endif
		CURRENT =62,
        CURRENT_CATALOG =63,
		CURRENT_DATE =64,
		CURSOR = 65, // must be 65
        CURRENT_DEFAULT_TRANSFORM_GROUP =66,
		DATE =67, // must be 67	
        CURRENT_PATH =68,
		CURRENT_ROLE =69,
        CURRENT_ROW = 70,
        CURRENT_SCHEMA =71,
        CURRENT_TIME = 72, 
        CURRENT_TIMESTAMP = 73, 
        CURRENT_TRANSFORM_GROUP_FOR_TYPE = 74,
		CURRENT_USER = 75, // CURSOR see 65
		CYCLE =76, // DATE see 67
		DAY =77,
		DEALLOCATE =78,
        DEC =79,
		DECIMAL =80, 
		DECLARE =81,
		DEFAULT =82,
		DELETE =83,
#if OLAP
        DENSE_RANK =84,
#endif
		DEREF =85,
		DESCRIBE =86,
		DETERMINISTIC =87,
		DISCONNECT =88,
		DISTINCT =89,
        DO =90, // from vol 4
        DOCARRAY = 91, // Pyrrho 5.1
        DOCUMENT = 92, // Pyrrho 5.1
		DOUBLE =93,
		DROP =94,
		DYNAMIC =95,
		EACH =96,
		ELEMENT =97,
		ELSE =98,
        ELSEIF =99, // from vol 4
		END =100,
        END_EXEC =101, // misprinted in SQL2011 as END-EXEC
        END_FRAME = 102,
        END_PARTITION = 103,
		EOF =104,	// Pyrrho 0.1
        EQUALS = 105,
        ESCAPE =106,
        EVERY =107,
		EXCEPT =108,
		EXEC =109,
		EXECUTE =110,
		EXISTS =111,
        EXIT = 112, // from vol 4
        EXP =113,
		EXTERNAL =114,	
		EXTRACT =115, 
		FALSE =116,
		FETCH =117,	
		FILTER =118,	
		FLOAT =119,
	    FLOOR =120,
		FOR = 121,		
		FOREIGN =122,	
        FREE =123,		
		FROM =124,	
		FULL =125,
		FUNCTION =126,
        FUSION =127,
		GET =128,	
		GLOBAL =129,		
		GRANT =130,		
		GROUP =131,	
		GROUPING =132,
        GROUPS =133,
        HANDLER =134, // vol 4 
		INTEGER =135, // must be 135
        INT = 136,	// deprecated: see also INTEGERLITERAL
		INTERVAL0 = 137,  // must be 137 (old version of INTERVAL)
        HAVING = 138, 
        HOLD = 139,	
        HOUR = 140, 
        HTTP = 141, // Pyrrho 4.5
        IDENTITY =142,
	    IF =143,  // vol 4
		IN =144,	
		INDICATOR = 145,	
		INNER = 146,	
		INOUT = 147,	
		INSENSITIVE =148, 
		INSERT =149, // INT is 136, INTEGER is 135
        INTERSECT = 150,
        INTERSECTION =151, 
        INTERVAL =152, // must be 152 see also INTERVAL0
		INTO =153,	
		IS = 154,		
		ITERATE =155, // vol 4
		JOIN =156,	
        LAG = 157,
		LANGUAGE =158,
		LARGE =159,	
        LAST_VALUE = 160,
		LATERAL =161,
		LEADING =162,	
        LEAVE =163, // vol 4
		LEFT =164,		
		LIKE =165,
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
        LOWER =175,
        MATCH = 176,
        NULL = 177, // must be 177
        MAX = 178, //
        NUMERIC = 179, // must be 179
        MEMBER = 180,
        MERGE =181,	
        METHOD =182,
		MIN =183, //
		MINUTE =184,	
        MOD =185, 
		MODIFIES =186,
        MODULE =187,	
        MONTH = 188,	 // MULTISET is 168
        NATIONAL =189,
		NATURAL =190, // NCHAR 171 NCLOB 172
        NEW = 191, 
        NO = 192,
        NONE =193,
        NORMALIZE = 194,
		NOT = 195,
#if OLAP
        NTH_VALUE = 196,
        NTILE = 197, // NULL 177
#endif
		NULLIF = 198, // NUMERIC 179, see also NUMERICLITERAL
        REAL0 = 199, // must be 199, previous version of REAL
        OBJECT = 200, 
        OCCURRENCES_REGEX =201, // alphabetical sequence is a misprint in SQL2008
        OCTET_LENGTH =202,
        REAL = 203, // must be 203, see also REAL0 and REALLITERAL
        OF = 204,
        OFFSET = 205,
		OLD = 206,	
        ON = 207,			
		ONLY = 208,		
		OPEN =209,	
		OR =210,		
		ORDER =211,		
		OUT = 212,		
		OUTER =213,	
		OVER =214,	
		OVERLAPS =215,
        OVERLAY =216,
		PARAMETER =217, // PASSWORD is 218
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
        POSITION =226,
#if OLAP || MONGO || SIMILAR
        POSITION_REGEX =227,
#endif
        POWER =228,
        PRECEDES = 229,
		PRECISION =230,
		PREPARE =231,
		PRIMARY =232,
		PROCEDURE =233,	
		RANGE =234,
        RANK =235,
		READS =236,	// REAL 203 (previously 199 see REAL0)
		RECURSIVE =237,
		REF = 238,	
		REFERENCES =239,	
		REFERENCING =240,
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
        RELEASE =250,
        REPEAT =251, // vol 4
        RESIGNAL =252, // vol 4
		RESULT =253,	
		RETURN =254,	
		RETURNS =255,
		REVOKE =256,
        TIME = 257, // must be 257
        TIMESTAMP = 258, // must be 258
        RIGHT = 259,
        ROLLBACK = 260,
#if OLAP
        ROLLUP =261,
#endif
        ROW =262,	
        ROW_NUMBER =263,
		ROWS =264,	
		SAVEPOINT =265,	
		SCOPE =266,	
        TYPE = 267, // must be 267 but is not a reserved word
        SCROLL = 268,
        SEARCH = 269,
        SECOND = 270,		
		SELECT =271,	
        SENSITIVE =272,	// has a different usage in Pyrrho
		SESSION_USER =273,
		SET =274,	
	    SIGNAL =275, //vol 4
#if SIMILAR
        SIMILAR =276,
#endif
		SMALLINT =277,	
		SOME =278,	
		SPECIFIC =279,	
		SPECIFICTYPE =280,	
		SQL =281, 	
		SQLEXCEPTION =282,
        SQLSTATE =283,	
		SQLWARNING =284,
        SQRT =285,
		START =286,	
		STATIC =287,
        STDDEV_POP =288,
        STDDEV_SAMP =289,
		SUBMULTISET =290,
        SUBSTRING =291, //
        SUBSTRING_REGEX =292,
        SUCCEEDS = 293,
		SUM =294, //
		SYMMETRIC =295, 
		SYSTEM =296,
	    TABLE = 297, // must be 297
		SYSTEM_TIME =298,
		SYSTEM_USER =299,	// TABLE is 297
		TABLESAMPLE =300,
		THEN =301,	 // TIME is 257, TIMESTAMP is 258	
		TIMEZONE_HOUR = 302, 
		TIMEZONE_MINUTE =303,
		TO =304,	
		TRAILING =305,	
        TRANSLATE =306,
#if SIMILAR
        TRANSLATE_REGEX =307,
#endif
		TRANSLATION =308,
		TREAT =309,	
		TRIGGER =310,
	    TRIM =311,
        TRIM_ARRAY = 312,
		TRUE = 313,
	    TRUNCATE = 314,
		UESCAPE =315,		
		UNION =316,		
		UNIQUE =317,		
		UNKNOWN =318,
		UNNEST =319,	
        UNTIL =320, // vol 4
		UPDATE =321,	
		UPPER =322, //
		USER =323,
		USING =324,	
		VALUE =325,
	    VALUE_OF = 326,
		VALUES =327,
#if OLAP
        VAR_POP =328,
        VAR_SAMP =329,
#endif
        VARBINARY =330,
		VARCHAR =331,
		VARYING =332,
        VERSIONING = 333,
		WHEN =334,	
		WHENEVER =335,
		WHERE =336,		
        WHILE =337, // vol 4
#if OLAP
        WIDTH_BUCKET =338,
#endif
		WINDOW =339,	
		WITH =340,		
		WITHIN =341,		
		WITHOUT =342, // XML is 356 vol 14
        XMLAGG = 343, // Basic XML stuff + XMLAgg vol 14
        XMLATTRIBUTES =344,
        XMLBINARY = 345,
        XMLCAST = 346,
        XMLCOMMENT = 347,
        XMLCONCAT = 348,
        XMLDOCUMENT = 349,
        XMLELEMENT = 350,
        XMLEXISTS = 351,
        XMLFOREST =352, 
        XMLITERATE = 353,
        XMLNAMESPACES =354,
        XMLPARSE =355,
        XML = 356, // must be 356 
        XMLPI = 357,
        XMLQUERY = 358,
        XMLSERIALIZE = 359,
        XMLTABLE =360,
        XMLTEXT =361,
        XMLVALIDATE =362,
        YEAR = 363,	// last reserved word
        //====================TOKEN TYPES=====================
        ASSIGNMENT =364, // := 
        BLOBLITERAL = 365, // 
        BOOLEANLITERAL = 366, 
        CHARLITERAL = 367, //
        COLON =368, // :
		COMMA =369,  // ,        
		CONCATENATE =370, // ||        
        DIVIDE =371, // /        
        DOCUMENTLITERAL = 372, // v5.1
        DOT =373, // . 5.2 was STOP
        DOUBLECOLON =374, // ::        
        EMPTY = 375, // []        
        EQL =376,  // =        
        GEQ =377, // >=    
        GTR =378, // >    
		ID =379, // identifier
        INTEGERLITERAL = 380, // Pyrrho
        LBRACE =381, // {
        LBRACK =382, // [
		LEQ =383, // <=
        LPAREN =384, // (
        LSS =385, // <
        MEXCEPT = 386, // 
        MINTERSECT = 387, //
        MINUS =388, // -
        MUNION = 389, // 
        NEQ =390, // <>
        NUMERICLITERAL = 391, // 
        PLUS =392, // + 
        QMARK =393, // ?
        RBRACE =394, // } 
        RBRACK =395, // ] 
        RDFDATETIME = 396, //
        RDFLITERAL = 397, // 
        REALLITERAL = 398, //
        RPAREN =399, // ) 
        SEMICOLON =400, // ; 
        TIMES =401, // *
        VBAR =402, // | 
        //=========================NON-RESERVED WORDS================
        A = 403, // first non-reserved word
        ABSOLUTE = 404,
        ACTION = 405,
        ADA = 406,
        ADD = 407, 
        ADMIN =408,
        AFTER =409,
        ALWAYS =410,
        APPLICATION =411, // Pyrrho 4.6
		ASC =412,
        ASSERTION =413,
        ATTRIBUTE =414,
        ATTRIBUTES =415,
#if MONGO
        BACKGROUND = 416, // Pyrrho 5.1 for MongoDB
#endif
        BEFORE =417,
        BERNOULLI =418,
        BREADTH = 419,
        BREAK =420, // Pyrrho
        C = 421,
#if MONGO
        CAPPED = 422, // Pyrrho 5.1 for MongoDB
#endif
        CAPTION = 423, // Pyrrho 4.5
        CASCADE = 424, 
        CATALOG = 425,
        CATALOG_NAME =426,
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
        COLUMN_NAME =439,
        COMMAND_FUNCTION =440,
        COMMAND_FUNCTION_CODE =441,
        COMMITTED =442,
        CONDITION_NUMBER =443,
        CONNECTION =444,
        CONNECTION_NAME =445,
        CONSTRAINT_CATALOG =446,
        CONSTRAINT_NAME =447,
        CONSTRAINT_SCHEMA =448,
        CONSTRAINTS =449,
        CONSTRUCTOR =450,
        CONTENT = 451,
        CONTINUE =452,
        CSV = 453, // Pyrrho 5.5
        CURATED = 454, // Pyrrho
        CURSOR_NAME =455,
        DATA =456,
        DATABASE =457, // Pyrrho
        DATETIME_INTERVAL_CODE =458,
        DATETIME_INTERVAL_PRECISION =459,
        DEFAULTS =460,
        DEFERRABLE =461,
        DEFERRED =462,
        DEFINED =463,
        DEFINER =464,
        DEGREE =465,
        DEPTH =466,
        DERIVED = 467,
        DESC =468,
        DESCRIPTOR =469,
        DIAGNOSTICS =470,
        DISPATCH =471,
        DOMAIN =472,
#if MONGO
        DROPDUPS = 473, // Pyrrho 5.1 for MongoDB
#endif
        DYNAMIC_FUNCTION =474,
        DYNAMIC_FUNCTION_CODE =475,
        ENFORCED = 476,
        ENTITY = 477, // Pyrrho 4.5
        EXCLUDE =478,
        EXCLUDING =479,
        FINAL =480,
        FIRST =481, 
        FLAG = 482,
        FOLLOWING = 483,
        FORTRAN = 484,
        FOUND =485,
        G = 486,
        GENERAL = 487,
		GENERATED =488,
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
        INSTANCE =503, 
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
        NAME =531,
        NAMES =532,
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
        OVERRIDING =551,
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
    /// Implement an identifier lexeme class.
    /// This class is used in several different ways:
    /// a) In structured types to name subfields and give the defining position of their types
    /// (note this information is role-specific. PhysBase should track roles when deciding the
    /// appropriate dataType to choose during serialisation and deserialisation)
    /// b) In runtime execution of procedure blocks to help reference variables etc
    /// (again we need to use iterative or explicit dotted access method)
    /// c) In parsing to discover the defining parsing context for aliases
    /// (for this case the dotted structure can be short-circuited to the scope once we discover it)
    /// d) For replacing identifiers by defining positions to support renaming and role-based names
    /// (lexical transformation by DigestSQL/DisplaySQL: see Set methods below)
    /// e) Used in RESTView processing to limit remote requests to columns actually needed
    /// There are five types of Tree indexes defined for Ident, corresponding to indexing on
    /// BTree indexing on the head ident, 
    /// Ident.IdTree indexing on the dotted ident chain, 
    /// Ident.RenTree indexing on lxrstt,
    /// and Tree which is imnplemented as two trees using indexing on both segpos and ident chain.
    /// Changing the value of segpos is not allowed once the Ident has been entered in Tree.
    /// Changing the value of ident is not allowed once the Ident has been entered in idTree or Tree.
    /// Traversal of the fourth type, Tree, will enumerate some Idents twice and is rarely used.
    /// The best sort of Ident has a blockid declaration point and any necessary suffixes
    /// (
    /// </summary>
    internal class Ident : IComparable
    {
        public enum IDType { NoInput, Lexer, Alias, Table, Column, Procedure, Type, Method, Loop, 
            Block, Role, Check, Period, Trigger, Metadata, Variable };
        public IDType type = IDType.NoInput;
        public readonly Ident sub = null;
        internal string blockid = null; // not null for defining context
        static int _iid = 0;
        bool indexed = false;
        internal readonly int iix = ++_iid;
        /// <summary>
        /// The name of the ident: will contain ":" iff it is a declaration point (blockid).
        /// The most explicit Idents begin with a declaration point suffixed by
        /// a table name/alias or variable name, and further suffixes if needed for
        /// for subqueries or columns/field ids.
        /// For nested contexts, use the innermost declaration point.
        /// </summary>
        string _ident;
        internal string ident
        {
            get => _ident;
            set
            {
                if (indexed)
                    throw new PEException("PE725");
                _ident = value;
            }
        }
        long _segpos = 0;
        internal long segpos
        {
            get => _segpos;
            set {
                //      if (segindexed)
                //         throw new PEException("PE726");
                _segpos = value;
            }
        }
        /// <summary>
        /// the start position in the lexer
        /// </summary>
        internal readonly int lxrstt = 0;
        /// <summary>
        /// the current position in the lexer
        /// </summary>
        internal int lxrpos = 0;
        /// <summary>
        /// The lexer responsible
        /// </summary>
        internal Lexer lexer = null;
        /// <summary>
        /// position in list of requested columns
        /// </summary>
        internal int reqpos = -1;
        internal Ident(string s,long pos,IDType tp=IDType.NoInput,Ident sb=null)
        {
            type = tp;
            ident = s;
            sub = sb;
            _segpos = pos;
            if (s.Length > 0 && pos==0)
            {
     //           if (ident[0] == '\'' && long.TryParse(ident.Substring(1), out _segpos)) // this is very unlikely
     //               _segpos += Transaction.TransPos;
     //           else
                    long.TryParse(ident, out _segpos);
            }
        }
        internal Ident(Lexer lx,IDType tp = IDType.Lexer, string s = null)
        {
            ident = s ?? ((lx.tok==Sqlx.ID)? lx.val.ToString() : lx.tok.ToString());
            type = tp;
            lexer = lx;
            lxrstt = lx.start;
            lxrpos = lx.pos;
            if (ident.Length > 0)
            {
       //         if (ident[0] == '\'' && long.TryParse(ident.Substring(1), out _segpos)) // this is very unlikely
       //             _segpos += Transaction.TransPos;
       //         else
                    long.TryParse(ident, out _segpos);
            }
        }
        internal Ident(Lexer lx, int st, int pos, long dp, Ident sb=null)
        {
            ident = new string(lx.input, st, pos - st);
            type = IDType.Lexer;
            lexer = lx;
            lxrstt = st;
            lxrpos = pos;
            segpos = dp;
            sub = sb;
            if (ident.Length > 0 && pos == 0)
            {
      //          if (ident[0] == '\'' && long.TryParse(ident.Substring(1), out _segpos)) // this is very unlikely
      //              _segpos += Transaction.TransPos;
      //          else
                    long.TryParse(ident, out _segpos);
            }
        }
        internal Ident(Ident lf,Ident sb)
        {
            ident = lf.ident;
            blockid = lf.blockid;
            type = lf.type;
            reqpos = lf.reqpos;
            lexer = lf.lexer;
            lxrstt = lf.lxrstt;
            lxrpos = lf.lxrpos;
            long sg = 0;
            if (ident.Length > 0)
            {
      //          if (ident[0] == '\'' && long.TryParse(ident.Substring(1), out sg)) // this is very unlikely
      //              sg  += Transaction.TransPos;
      //          else
                    long.TryParse(ident, out sg);
            }
            segpos = (sg>0)?sg:lf.segpos;
      //      if (_segpos > 0 && Renamable() && lexer!=null)
      //          ATree<Ident,long?>.Add(ref lexer.ctx.refs, this, segpos);
            sub = sb;
        }
        internal Ident(string s, IDType t, long dp)
        {
            ident = s; type = t;   _segpos = dp;
        }
        bool Renamable()
        {
            switch (type)
            {
                case IDType.NoInput:
                case IDType.Lexer:
                case IDType.Alias:
                case IDType.Loop:
                case IDType.Block:
                case IDType.Role:
                case IDType.Check:
                case IDType.Period:
                case IDType.Metadata:
                case IDType.Variable:
                    return false;
                default: // Table, Column, Procedure, Type, Method
                    return true;
            }
        }
        internal Ident Prefix(int len,Ident append=null)
        {
            if (len == 0)
                return null;
            return new Ident(ident, segpos, IDType.NoInput, sub?.Prefix(len - 1)??append);
        }
        internal int Length()
        {
            return 1 + (sub?.Length()?? 0);
        }
        internal Ident Trim(Ident alias)
        {
            if (alias == null)
                return this;
            if (ident == alias.ident)
                return sub?.Trim(alias.sub);
            return this;
        }
        public void Set(long dp, IDType tp)
        {
            if (dp == 0 || tp == IDType.Alias || tp == IDType.NoInput)
                return;
            var fi = Final();
            if (fi.segpos != 0)
                return;
            fi.segpos = dp;
            if (type != IDType.Alias)
            {
                type = tp;
        //        if (lexer?.ctx is Context c)
         //           ATree<Ident, long?>.Add(ref c.refs, this, dp);
            }
        }
        public void SetHead(long dp, IDType tp)
        {
            if (dp == 0 || tp == IDType.Alias || tp == IDType.NoInput)
                return;
            segpos = dp; 
            if (type != IDType.Alias)
            {
                type = tp;
       //         if (lexer?.ctx is Context cx)
        //            ATree<Ident, long?>.Add(ref cx.refs, this, dp);
            }
        }
        /// <summary>
        /// Idents support the renaming machinery: where possible we fill in
        /// defpos information so that internal SQL can use defining positions
        /// instead of actual names. 
        /// </summary>
        /// <param name="t"></param>
        public void Set(Ident t)
        {
            if (t == null)
                return;
   //         if (ident==cnx.context?.cur?.alias.ident)
   //         {
   //             type = Ident.IDType.Alias;
   //             sub.Set(cnx, t);
   //             return;
   //         }
            segpos = t.segpos;
            if (t.type == IDType.Alias || t.type == IDType.NoInput)
                return;
            if (type == IDType.Lexer||type==IDType.NoInput)
                type = t.type;
   //         if (lexer != null)
    //            ATree<Ident, long?>.Add(ref lexer.ctx.refs, this, segpos);
        }
        internal Ident Final()
        {
            return sub?.Final() ?? this;
        }
        public Ident Target()
        {
            if (sub == null)
                return null;
            return new Ident(lexer, lxrstt, lxrpos, segpos, sub.Target()) { ident=ident, type = type };
        }
        public static Ident Append(Ident a,Ident b)
        {
            if (b == null)
                return a;
            if (a == null)
                return b;
            return a.Prefix(a.Length(), b);
        }
        public bool HeadsMatch(Ident a)
        {
            if (a == null)
                return false;
            if (segpos!=0 && segpos == a.segpos)
                return true;
            if (((segpos == 0 || segpos == a.segpos) && ident == a.ident))
            {
                if (segpos == 0)
                    SetHead(a.segpos, a.type);
                else if (a.segpos == 0)
                    a.SetHead(segpos, type);
                return true;
            }
            return false;
        }
        public bool _Match(Ident a)
        {
            return HeadsMatch(a) && ((sub == null && a.sub == null) || sub?._Match(a.sub)==true);
        }
        public Ident Suffix(Ident pre)
        {
            if (pre == null)
                return this;
            if (!HeadsMatch(pre) || sub==null)
                return null;
            return sub.Suffix(pre.sub);
        }
        public long Defpos()
        {
            return sub?.Defpos() ?? segpos;
        }
        public long Defpos(IDType tp)
        {
            if (blockid==null && (tp == type ||tp==IDType.Lexer||type==IDType.Alias))
                return segpos;
            return sub?.Defpos(tp) ?? segpos;
        }
        internal void Fix(Reloc r)
        {
            for (; r != null; r = r.next)
                if (segpos == r.was)
                {
                    segpos = r.now;
                    break;
                }
            lexer = null;
        }
        /// <summary>
        /// Instead of this, use a new Ident that has $arity appended if necessary.
        /// Check Metdata $seq usage. XXX
        /// </summary>
        /// <returns></returns>
        internal Ident Suffix(int x)
        {
            if (segpos > 0 || ident.Contains("$"))
                return this;
            if (ident!="" && Char.IsDigit(ident[0]))
            {
                segpos = long.Parse(ident);
                return this;
            }
            return new Ident(ident + "$" + x, 0, type);
        }
        /// <summary>
        /// RowTypes should not have dots
        /// </summary>
        /// <returns>The dot-less version of the Ident</returns>
        internal Ident ForTableType()
        {
            return (sub is Ident id) ? id : this;
        }
        public override string ToString()
        {
            if (ident == "...") // special case for anonymous row types: use NameInSession for readable version
                return "(...)";
            var sb = new StringBuilder();
            if (ident != null)
                sb.Append(ident);
            else if (segpos > 0)
                sb.Append("\"" + segpos + "\"");
            else
                sb.Append("??");
            if (sub != null)
            {
                sb.Append(".");
                sb.Append(sub.ToString());
            }
            return sb.ToString();
        }
        internal static IDType IdType(Sqlx kind)
        {
            switch(kind)
            {
                case Sqlx.NO:
                case Sqlx.ID:
                case Sqlx.DATABASE: return IDType.Lexer;
                case Sqlx.VIEW:
                case Sqlx.ENTITY:
                case Sqlx.TABLE: return IDType.Table;
                case Sqlx.DOMAIN: 
                case Sqlx.TYPE:return IDType.Type;
                case Sqlx.METHOD:
                case Sqlx.INSTANCE:
                case Sqlx.STATIC:
                case Sqlx.OVERRIDING:
                case Sqlx.CONSTRUCTOR: return IDType.Method;
                case Sqlx.FUNCTION:
                case Sqlx.PROCEDURE: return IDType.Procedure;
                case Sqlx.TRIGGER: return IDType.Trigger;
            }
            return IDType.NoInput; // notraeched
        }
        /// <summary>
        /// When a Domain is placed in the PhysBase we need to remove context information from names
        /// </summary>
        internal void Clean()
        {
            type = IDType.Column;
            lexer = null;
            lxrpos = 0;
            reqpos = 0;
        }

        public int CompareTo(object obj)
        {
            if (obj == null)
                return 1;
            var that = (Ident)obj;
            var c = ident.CompareTo(that.ident);
            if (c != 0)
                return c;
            if (type == IDType.Alias && that.type == IDType.Alias)
                return iix.CompareTo(that.iix);
            return 0;
        }
        /// <summary>
        /// Wraps a structure for managing Idents distinguished by segpos or ident string
        /// </summary>
        /// <typeparam name="V"></typeparam>
        internal class Tree<V>
        {
            ATree<long, ATree<Ident,V>> bTree;
            ATree<Ident, V> idTree;
            Tree(ATree<long, ATree<Ident,V>> bT, ATree<Ident, V> idT) { bTree = bT; idTree = idT; }
            public Tree(Ident k, V v)
                : this(new BTree<long,ATree<Ident,V>>(k.Defpos(), new BTree<Ident,V>(k,v)),
                     (k.ident != null && k.ident != "") ? new IdTree<V>(k, v) : IdTree<V>.Empty)
            { }

            public static readonly Tree<V> Empty = new Tree<V>(BTree<long,ATree<Ident,V>>.Empty, IdTree<V>.Empty);
            public static void Add(ref Tree<V> t, Ident k, V v)
            {
                var bT = t.bTree;
                var idT = t.idTree;
                if (k.Defpos() != 0)
                {
                    ATree<Ident,V> tb = bT[k.Defpos()];
                    if (tb == null)
                        tb = new BTree<Ident, V>(k, v);
                    else
                        ATree<Ident, V>.Add(ref tb, k, v);
                    ATree<long, ATree<Ident, V>>.Add(ref bT, k.Defpos(), tb);
                }
                if (k.ident != null && k.ident != "")
                {
                    if (idT.Contains(k))
                    {
                        var b = idT.PositionAt(k);
                        if (b.key().Defpos() == 0 && k.Defpos() > 0)
                            ATree<Ident, V>.Remove(ref idT, k);
                    }
                    ATree<Ident, V>.Add(ref idT, k, v);
                }
                t = new Tree<V>(bT, idT);
            }
            public static Tree<V> operator+(Tree<V>t,(Ident, V) v)
            {
                Add(ref t,v.Item1, v.Item2);
                return t;
            }
            public static void AddNN(ref Tree<V> tree, Ident k, V v)
            {
                if (v == null)
                    throw new Exception("PE000");
                Add(ref tree, k, v);
            }
            public static void Update(ref Tree<V> t,Ident k,V v)
            {
                var bT = t.bTree;
                var idT = t.idTree;
                if (bT[k.Defpos()] is ATree<Ident,V> tb)
                {
                    ATree<Ident, V>.Update(ref tb, k, v);
                    ATree<long, ATree<Ident, V>>.Update(ref bT, k.Defpos(), tb);
                }
                if (idT.Contains(k))
                    ATree<Ident, V>.Update(ref idT, k, v);
                t = new Tree<V>(bT, idT);
            }
            public static void Remove(ref Tree<V> t, Ident k)
            {
                var bT = t.bTree;
                var idT = t.idTree;
                bool done = false;
                if (bT[k.Defpos()] is ATree<Ident, V> tb)
                {
                    ATree<Ident, V>.Remove(ref tb, k);
                    if (tb == null||tb.Count==0)
                        ATree<long, ATree<Ident, V>>.Remove(ref bT, k.Defpos());
                    else
                        ATree<long, ATree<Ident,V>>.Update(ref bT, k.Defpos(), tb);
                    done = true;
                }
                if (idT.Contains(k))
                {
                    ATree<Ident, V>.Remove(ref idT, k);
                    done = true;
                }
                if (done)
                    t = (idT.Count == 0) ? Empty : new Tree<V>(bT, idT);
            }
            public int Count { get { return (int)idTree.Count; } }
            public bool Contains(Ident k)
            {
                return bContains(k) || idTree.Contains(k);
            }
            public bool bContains(Ident k)
            {
                return (bTree[k.Defpos()] is ATree<Ident, V> tb) ? tb.Contains(k): false;
            }
            public V this[Ident k]
            { get
                {
                    if (k == null)
                        return default(V);
                    return (bTree.Contains(k.segpos)) ? bTree[k.segpos].First().value() : idTree[k];
                }
            }
            public Bookmark First()
            {
                return Bookmark.New(this);
            }

            internal Tree<V> Add(Tree<V> tree)
            {
                var r = this;
                for (var b = tree.First(); b != null; b = b.Next())
                    Add(ref r, b.key(), b.value());
                return r;
            }

            internal class Bookmark
            {
                readonly Tree<V> _t;
                readonly ABookmark<long, ATree<Ident,V>> _bB;
                readonly ABookmark<Ident,V> _idB;
                Bookmark(Tree<V> t, ABookmark<long,ATree<Ident,V>> b, ABookmark<Ident, V> id)
                {
                    _t = t; _bB = b; _idB = id;
                }
                internal static Bookmark New(Tree<V> t)
                {
                    if (t.bTree.Count == 0 && t.idTree.Count == 0)
                        return null;
                    return (t.bTree.Count > t.idTree.Count) ? new Bookmark(t, t.bTree.First(), null) : new Bookmark(t,null, t.idTree.First());
                }
                internal Bookmark Next()
                {
                    if (_bB != null) return (_bB.Next() is ABookmark<long,ATree<Ident,V>>b) ? new Bookmark(_t,b,null) : null;
                    return (_idB.Next() is ABookmark<Ident, V> c) ? new Bookmark(_t, null,c) : null;
                }
                public Ident key() { return _bB?.value()?.First().key()?? _idB.key(); }
                public V value() { return (_bB?.value() is ATree<Ident, V> tb) ? tb.First().value() : _idB.value(); }
            }
        }
        /// <summary>
        /// Define an ordering based on identifier, dbix, segpos
        /// </summary>
        /// <typeparam name="V"></typeparam>
        internal class PosTree<V> : ATree<Ident, V>
        {
            public readonly static ATree<Ident, V> Empty = new PosTree<V>();
            public PosTree() : base(null) { }
            public PosTree(Ident k, V v) : base(new Leaf<Ident, V>(new KeyValuePair<Ident, V>(k, v))) { }
            protected PosTree(Bucket<Ident, V> b) : base(b) { }
            public override int Compare(Ident a, Ident b)
            {
                int c = a.ident.CompareTo(b.ident);
                if (c != 0)
                    return c;
                return a.segpos.CompareTo(b.segpos);
            }
            public static void Add(ref PosTree<V> t, Ident k, V v)
            {
                t = (PosTree<V>)t.Add(k, v);
            }
            public static PosTree<V> operator+ (PosTree<V> t,(Ident,V)v)
            {
                return (PosTree<V>)t.Add(v.Item1, v.Item2);
            }
            public static void Remove(ref PosTree<V> t, Ident k)
            {
                t.Remove(k);
            }
            protected override ATree<Ident, V> Add(Ident k, V v)
            {
                if (Contains(k))
                    return new PosTree<V>(root.Update(this, k, v));
                return Insert(k, v);
            }

            protected override ATree<Ident, V> Insert(Ident k, V v)
            {
                if (root == null || root.total == 0)  // empty BTree
                    return new PosTree<V>(k, v);
                if (root.count == Size)
                    return new PosTree<V>(root.Split()).Add(k, v);
                return new PosTree<V>(root.Add(this, k, v));
            }

            protected override ATree<Ident, V> Remove(Ident k)
            {
                if (k == null)
                    return this;
                k.indexed = false;
                if (!Contains(k))
                    return this;
                if (root.total == 1) // empty index
                    return Empty;
                // note: we allow root to have 1 entry
                return new PosTree<V>(root.Remove(this, k));
            }

            protected override ATree<Ident, V> Update(Ident k, V v)
            {
                if (!Contains(k))
                    throw new Exception("PE01");
                return new PosTree<V>(root.Update(this, k, v));
            }
        }
        /// <summary>
        /// Define a tree using an ordering based on the identifier chain
        /// </summary>
        /// <typeparam name="V"></typeparam>
        internal class IdTree<V> : ATree<Ident, V>
        {
            public readonly static ATree<Ident,V> Empty = new IdTree<V>();
            public IdTree() : base(null) { }
            public IdTree(Ident k, V v) : base(new Leaf<Ident, V>(new KeyValuePair<Ident, V>(k, v))) { }
            protected IdTree(Bucket<Ident, V> b) : base(b) { }
            public override int Compare(Ident a, Ident b)
            {
                int c = a.ident.CompareTo(b.ident);
                if (c != 0)
                    return c;
                if (a.type == Ident.IDType.Alias && b.type == Ident.IDType.Alias)
                    return a.iix.CompareTo(b.iix);
                if (a.sub == null)
                    return (b.sub == null) ? 0 : -1;
                if (b.sub == null)
                    return 1;
                return Compare(a.sub, b.sub);
            }
            public static void Add(ref IdTree<V> t,Ident k,V v)
            {
                t = (IdTree<V>)t.Add(k, v);
            }
            public static IdTree<V> operator+(IdTree<V> t,(Ident,V) v)
            {
                return (IdTree<V>)t.Add(v.Item1, v.Item2);
            }
            protected override ATree<Ident, V> Add(Ident k, V v)
            {
                if (Contains(k))
                    return new IdTree<V>(root.Update(this, k, v));
                return Insert(k, v);
            }

            protected override ATree<Ident, V> Insert(Ident k, V v)
            {
                k.indexed = true;
                if (root == null || root.total == 0)  // empty BTree
                    return new IdTree<V>(k, v);
                if (root.count == Size)
                    return new IdTree<V>(root.Split()).Add(k, v);
                return new IdTree<V>(root.Add(this, k, v));
            }

            protected override ATree<Ident, V> Remove(Ident k)
            {
                if (k == null)
                    return this;
                k.indexed = false;
                if (!Contains(k))
                    return this;
                if (root.total == 1) // empty index
                    return Empty;
                // note: we allow root to have 1 entry
                return new IdTree<V>(root.Remove(this, k));
            }
            public static void Remove(ref IdTree<V> t,Ident k)
            {
                t.Remove(k);
            }
            protected override ATree<Ident, V> Update(Ident k, V v)
            {
                if (!Contains(k))
                    throw new Exception("PE01");
                return new IdTree<V>(root.Update(this, k, v));
            }
        }
        /// <summary>
        /// This class is used for renaming support and is mentioned only once in 
        /// defining the refs tree in the Context class.
        /// </summary>
        internal class RenTree : ATree<Ident, long?>
        {
            public readonly static ATree<Ident,long?> Empty = new RenTree();
            public RenTree() : base(null) { }
            public RenTree(Ident k, long? v) : base(new Leaf<Ident,long?>(new KeyValuePair<Ident,long?>(k,v))) { }
            protected RenTree(Bucket<Ident, long?> b) : base(b) { }
            public override int Compare(Ident a, Ident b)
            {
                if (a == b)
                    return 0;
                if (a == null)
                    return -1;
                if (b == null)
                    return 1;
                return a.lxrstt.CompareTo(b.lxrstt);
            }
            protected override ATree<Ident, long?> Add(Ident k, long? v)
            {
                if (Contains(k))
                    return new RenTree(root.Update(this, k, v));
                return Insert(k, v);
            }

            protected override ATree<Ident, long?> Insert(Ident k, long? v)
            {
                if (root == null || root.total == 0)  // empty BTree
                    return new RenTree(k, v);
                if (root.count == Size)
                    return new RenTree(root.Split()).Add(k, v);
                return new RenTree(root.Add(this, k, v));
            }

            protected override ATree<Ident, long?> Remove(Ident k)
            {
                if (!Contains(k))
                    return this;
                if (root.total == 1) // empty index
                    return Empty;
                // note: we allow root to have 1 entry
                return new RenTree(root.Remove(this, k));
            }

            protected override ATree<Ident, long?> Update(Ident k, long? v)
            {
                if (!Contains(k))
                    throw new Exception("PE01");
                return new RenTree(root.Update(this, k, v));
            }
        }
        internal Ident Sub(Ident nm)
        {
            Ident n = this;
            for (; n != null && nm != null; n = n.sub, nm = nm.sub)
                if (n.ident!=nm.ident)
                    break;
            return n;
        }
    }

    /// <summary>
    /// Lexical analysis for SQL
    /// </summary>
    internal class Lexer
	{
        /// <summary>
        /// The entire input string
        /// </summary>
		public char[] input;
        /// <summary>
        /// The current position (just after tok) in the input string
        /// </summary>
		public int pos,pushPos;
        /// <summary>
        /// The start of tok in the input string
        /// </summary>
		public int start = 0, pushStart; 
        /// <summary>
        /// the current character in the input string
        /// </summary>
		char ch,pushCh;
        /// <summary>
        /// The current token's identifier
        /// </summary>
		public Sqlx tok;
        public Sqlx pushBack = Sqlx.Null;
        /// <summary>
        /// The current token's value
        /// </summary>
		public TypedValue val = null;
        public TypedValue pushVal;
        /// <summary>
        /// Entries in the reserved word table
        /// If there are more than 2048 reserved words, the hp will hang
        /// </summary>
		class ResWd
		{
			public Sqlx typ;
			public string spell;
			public ResWd(Sqlx t,string s) { typ=t; spell=s; }
		}
 		static ResWd[] resWds = new ResWd[0x800]; // open hash
        static Lexer()
        {
            int h;
            for (Sqlx t = Sqlx.ABS; t <= Sqlx.YEAR; t++)
                if (t != Sqlx.TYPE) // TYPE is not a reserved word but is in this range
                {
                    string s = t.ToString();
                    h = s.GetHashCode() & 0x7ff;
                    while (resWds[h] != null)
                        h = (h + 1) & 0x7ff;
                    resWds[h] = new ResWd(t, s);
                }
            // while XML is a reserved word and is not in the above range
            h = "XML".GetHashCode() & 0x7ff; 
            while (resWds[h] != null)
                h = (h + 1) & 0x7ff;
            resWds[h] = new ResWd(Sqlx.XML, "XML");
        }
        /// <summary>
        /// Check if a string matches a reserved word.
        /// tok is set if it is a reserved word.
        /// </summary>
        /// <param name="s">The given string</param>
        /// <returns>true if it is a reserved word</returns>
		internal bool CheckResWd(string s)
		{
			int h = s.GetHashCode() & 0x7ff;
			for(;;)
			{
				ResWd r = resWds[h];
				if (r==null)
					return false;
				if (r.spell==s)
				{
					tok = r.typ;
					return true;
				}
				h = (h+1)&0x7ff;
			}
		}
        internal object Diag { get { if (val == TNull.Value) return tok; return val; } }
       /// <summary>
        /// Constructor: Start a new lexer
        /// </summary>
        /// <param name="s">the input string</param>
        internal Lexer(string s)
        {
   		    input = s.ToCharArray();
			pos = -1;
			Advance();
			tok = Next();
        }
        /// <summary>
        /// Mutator: Advance one position in the input
        /// ch is set to the new character
        /// </summary>
        /// <returns>The new value of ch</returns>
		public char Advance()
		{
			if (pos>=input.Length)
				throw new DBException("42150").Mix();
			if (++pos>=input.Length)
				ch = (char)0;
			else
				ch = input[pos];
			return ch;
		}
        /// <summary>
        /// Decode a hexadecimal digit
        /// </summary>
        /// <param name="c">[0-9a-fA-F]</param>
        /// <returns>0..15</returns>
		internal static int Hexit(char c)
		{
			switch (c)
			{
				case '0': return 0;
				case '1': return 1;
				case '2': return 2;
				case '3': return 3;
				case '4': return 4;
				case '5': return 5;
				case '6': return 6;
				case '7': return 7;
				case '8': return 8;
				case '9': return 9;
				case 'a': return 10;
				case 'b': return 11;
				case 'c': return 12;
				case 'd': return 13;
				case 'e': return 14;
				case 'f': return 15;
				case 'A': return 10;
				case 'B': return 11;
				case 'C': return 12;
				case 'D': return 13;
				case 'E': return 14;
				case 'F': return 15;
				default: return -1;
			}
		}
        public Sqlx PushBack(Sqlx old)
        {
            pushBack = tok;
            pushVal = val;
            pushStart = start;
            pushPos = pos;
            pushCh = ch;
            tok = old;
            return tok;
        }
        public Sqlx PushBack(Sqlx old,TypedValue oldVal)
        {
            val = oldVal;
            return PushBack(old);
        }
        /// <summary>
        /// Advance to the next token in the input.
        /// tok and val are set for the new token
        /// </summary>
        /// <returns>The new value of tok</returns>
		public Sqlx Next()
		{
            if (pushBack != Sqlx.Null)
            {
                tok = pushBack;
                val = pushVal;
                start = pushStart;
                pos = pushPos;
                ch = pushCh;
                pushBack = Sqlx.Null;
                return tok;
            }
            val = TNull.Value;
			while (char.IsWhiteSpace(ch))
				Advance();
			start = pos;
			if (char.IsLetter(ch))
			{
				char c = ch;
				Advance();
				if (c=='X' && ch=='\'')
				{
					int n = 0;
					if (Hexit(Advance())>=0)
						n++;
					while (ch!='\'')
						if (Hexit(Advance())>=0)
							n++;
					n = n/2;
					byte[] b = new byte[n];
					int end = pos;
					pos = start+1;
					for (int j=0;j<n;j++)
					{
						while (Hexit(Advance())<0)
							;
						int d = Hexit(ch)<<4;
						d += Hexit(Advance());
						b[j] = (byte)d;
					}
					while (pos!=end)
						Advance();
					tok = Sqlx.BLOBLITERAL;
					val = new TBlob(b);
					Advance();
					return tok;
				}
				while (char.IsLetterOrDigit(ch) || ch=='_')
					Advance();
				string s0 = new string(input,start,pos-start);
                string s = s0.ToUpper();
				if (CheckResWd(s))
				{
					switch(tok)
					{
						case Sqlx.TRUE: val = TBool.True; return Sqlx.BOOLEANLITERAL;
						case Sqlx.FALSE: val = TBool.False; return Sqlx.BOOLEANLITERAL;
						case Sqlx.UNKNOWN: val = null; return Sqlx.BOOLEANLITERAL;
                        case Sqlx.CURRENT_DATE: val = new TDateTime(DateTime.Today); return tok;
                        case Sqlx.CURRENT_TIME: val = new TTimeSpan(DateTime.Now - DateTime.Today); return tok;
                        case Sqlx.CURRENT_TIMESTAMP: val = new TDateTime(DateTime.Now); return tok;
					}
					return tok;
				}
				val = new TChar(s);
				return tok=Sqlx.ID;
			}
			string str;
			if (char.IsDigit(ch))
			{
				start = pos;
				while (char.IsDigit(Advance()))
					;
				if (ch!='.')
				{
					str = new string(input,start,pos-start);
					if (pos-start>18)
						val = new TInteger(Integer.Parse(str));
					else
						val = new TInt(long.Parse(str));
					tok=Sqlx.INTEGERLITERAL;
					return tok;
				}
				while (char.IsDigit(Advance()))
					;
				if (ch!='e' && ch!='E')
				{
					str = new string(input,start,pos-start);
					val = new TNumeric(Common.Numeric.Parse(str));
					tok=Sqlx.NUMERICLITERAL;
					return tok;
				}
				if (Advance()=='-'||ch=='+')
					Advance();
				if (!char.IsDigit(ch))
					throw new DBException("22107").Mix();
				while (char.IsDigit(Advance()))
					;
				str = new string(input,start,pos-start);
				val = new TReal(Common.Numeric.Parse(str));
				tok=Sqlx.REALLITERAL;
				return tok;
			}
			switch (ch)
			{
				case '[':	Advance(); return tok=Sqlx.LBRACK;
				case ']':	Advance(); return tok=Sqlx.RBRACK;
				case '(':	Advance(); return tok=Sqlx.LPAREN;
				case ')':	Advance(); return tok=Sqlx.RPAREN;
				case '{':	Advance(); return tok=Sqlx.LBRACE;
				case '}':	Advance(); return tok=Sqlx.RBRACE;
				case '+':	Advance(); return tok=Sqlx.PLUS;
				case '*':	Advance(); return tok=Sqlx.TIMES;
				case '/':	Advance(); return tok=Sqlx.DIVIDE;
				case ',':	Advance(); return tok=Sqlx.COMMA;
				case '.':	Advance(); return tok=Sqlx.DOT;
				case ';':	Advance(); return tok=Sqlx.SEMICOLON;
/* from v5.5 Document syntax allows exposed SQL expressions
                case '{':
                    {
                        var braces = 1;
                        var quote = '\0';
                        while (pos<input.Length)
                        {
                            Advance();
                            if (ch == '\\')
                            {
                                Advance();
                                continue;
                            }
                            else if (ch == quote)
                                quote = '\0';
                            else if (quote == '\0')
                            {
                                if (ch == '{')
                                    braces++;
                                else if (ch == '}' && --braces == 0)
                                {
                                    Advance();
                                    val = new TDocument(ctx,new string(input, st, pos - st));
                                    return tok = Sqlx.DOCUMENTLITERAL;
                                }
                                else if (ch == '\'' || ch == '"')
                                    quote = ch;
                            }
                        }
                        throw new DBException("42150",new string(input,st,pos-st));
                    } */
				case ':':	Advance(); 
					if (ch==':')
					{
						Advance();
						return tok=Sqlx.DOUBLECOLON;
					}
					return tok=Sqlx.COLON;
				case '-':	
					if (Advance()=='-')
					{
						Advance();    // -- comment
						while (pos<input.Length) 
							Advance();
						return Next();
					}
					return tok=Sqlx.MINUS;
				case '|':	
					if (Advance()=='|')
					{
						Advance();
						return tok=Sqlx.CONCATENATE;
					}
					return tok=Sqlx.VBAR;
				case '<' : 
					if (Advance()=='=')
					{
						Advance();
						return tok=Sqlx.LEQ; 
					}
					else if (ch=='>')
					{
						Advance();
						return tok=Sqlx.NEQ;
					}
					return tok=Sqlx.LSS;
				case '=':	Advance(); return tok=Sqlx.EQL;
				case '>':
					if (Advance()=='=')
					{
						Advance();
						return tok=Sqlx.GEQ;
					}
					return tok=Sqlx.GTR;
				case '"':	// delimited identifier
				{
					start = pos;
					while (Advance()!='"')
						;
					val = new TChar(new string(input,start+1,pos-start-1));
                    Advance();
                    while (ch == '"')
                    {
                        var fq = pos;
                        while (Advance() != '"')
                            ;
                        val = new TChar(val.ToString()+new string(input, fq, pos - fq));
                        Advance();
                    }
					tok=Sqlx.ID;
         //           CheckForRdfLiteral();
                    return tok;
				}
				case '\'': 
				{
					start = pos;
					var qs = new Stack<int>();
                    qs.Push(-1);
					int qn = 0;
					for (;;)
					{
						while (Advance()!='\'')
							;
						if (Advance()!='\'')
							break;
                        qs.Push(pos);
						qn++;
					}
					char[] rb = new char[pos-start-2-qn];
					int k=pos-start-3-qn;
					int p = -1;
					if (qs.Count>1)
						p = qs.Pop();
					for (int j=pos-2;j>start;j--)
					{
                        if (j == p)
                            p = qs.Pop();
                        else
                            rb[k--] = input[j];
					}
					val = new TChar(new string(rb));
					return tok=Sqlx.CHARLITERAL;
				}
        /*        case '^': // ^^uri can occur in Type
                {
                    val = new TChar("");
                    tok = Sqlx.ID;
                    CheckForRdfLiteral();
                    return tok;
                } */
				case '\0':
					return tok=Sqlx.EOF;
			}
			throw new DBException("42101",ch).Mix();
		}
 /*       /// <summary>
        /// Pyrrho 4.4 if we seem to have an ID, it may be followed by ^^
        /// in which case it is an RdfLiteral
        /// </summary>
        private void CheckForRdfLiteral()
        {
            if (ch != '^')
                return;
            if (Advance() != '^')
                throw new DBException("22041", "^").Mix();
            string valu = val.ToString();
            Domain t = null;
            string iri = null;
            int pp = pos;
            Ident ic = null;
            if (Advance() == '<')
            {
                StringBuilder irs = new StringBuilder();
                while (Advance() != '>')
                    irs.Append(ch);
                Advance();
                iri = irs.ToString();
            }
    /*        else if (ch == ':')
            {
                Next();// pass the colon
                Next();
                if (tok != Sqlx.ID)
                    throw new DBException("22041", tok).Mix();
                var nsp = ctx.nsps[""];
                if (nsp == null)
                    throw new DBException("2201M", "\"\"").ISO();
                iri = nsp + val as string;
            } else 
            {
                Next();
                if (tok != Sqlx.ID)
                    throw new DBException("22041", tok).Mix();
    /*            if (ch == ':')
                {
                    Advance();
                    iri = ctx.nsps[val.ToString()];
                    if (iri == null)
                        iri = PhysBase.DefaultNamespaces[val.ToString()];
                    if (iri == null)
                        throw new DBException("2201M", val).ISO();
                    Next();
                    if (tok != Sqlx.ID)
                        throw new DBException("22041", tok).Mix();
                    iri = iri + val as string;
                } 
            }
            if (iri != null)
            {
                t = ctx.types[iri];
                if (t==null) // a surprise: ok in provenance and other Row
                {
                    t = Domain.Iri.Copy(iri);
                    ATree<string, Domain>.AddNN(ref ctx.types, iri, t);
                }
                ic = new Ident(this,Ident.IDType.Type,iri);
            }
            val = RdfLiteral.New(t, valu);
            tok = Sqlx.RDFLITERAL;
        } */
        /// <summary>
        /// This function is used for XML parsing (e.g. in XPATH)
        /// It stops at the first of the given characters it encounters or )
        /// if the stop character is unquoted and unparenthesised 
        /// ' " are processed and do not nest
        /// unquoted () {} [] &lt;&gt; nest. (Exception if bad nesting)
        /// Exception at EOF.
        /// </summary>
        /// <param name="stop">Characters to stop at</param>
        /// <returns>the stop character</returns>
        public char XmlNext(params char[] stop)
        {
            var nest = new Stack<char>();
            char quote = (char)0;
            int n = stop.Length;
            int start = pos;
            char prev = (char)0;
            for (; ; )
            {
                if (nest == null && quote == (char)0)
                    for (int j = 0; j < n; j++)
                        if (ch == stop[j])
                            goto done;
                switch (ch)
                {
                    case '\0': throw new DBException("2200N").ISO();
                    case '\\': Advance(); break;
                    case '\'': if (quote == ch)
                            quote = (char)0;
                        else if (quote==(char)0)
                            quote = ch;
                        break;
                    case '"': goto case '\'';
                    case '(':  if (quote == (char)0)
                            nest.Push(')'); 
                        break;
                    case '[': if (quote == (char)0)
                            nest.Push(']');
                        break;
                    case '{': if (quote == (char)0)
                            nest.Push('}');
                        break;
                    //     case '<': nest = MTree.Add('>', nest); break; < and > can appear in FILTER
                    case ')': if (quote==(char)0 && nest.Count==0)
                            goto done;
                        goto case ']';
                    case ']': if (quote != (char)0) break;
                        if (nest == null || ch != nest.Peek())
                            throw new DBException("2200N").ISO();
                        nest.Pop();
                        break;
                    case '}': goto case ']';
               //     case '>': goto case ']';
                    case '#': 
                        if (prev=='\r' || prev=='\n')
                            while (ch != '\r' && ch != '\n')
                                Advance();
                        break;
                }
                prev = ch;
                Advance();
            }
        done:
            val = new TChar(new string(input, start, pos - start).Trim());
            return ch;
        }
        public static string UnLex(Sqlx s)
        {
            switch (s)
            {
                default: return s.ToString();
                case Sqlx.EQL: return "=";
                case Sqlx.NEQ: return "<>";
                case Sqlx.LSS: return "<";
                case Sqlx.GTR: return ">";
                case Sqlx.LEQ: return "<=";
                case Sqlx.GEQ: return ">=";
                case Sqlx.PLUS: return "+";
                case Sqlx.MINUS: return "-";
                case Sqlx.TIMES: return "*";
                case Sqlx.DIVIDE: return "/";
            }
        }
     }
}
