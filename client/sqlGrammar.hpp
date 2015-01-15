#ifndef _SQL_GRAMMER_H_
#define _SQL_GRAMMER_H_


#include <boost/spirit/include/classic_core.hpp>
#include <boost/spirit/include/classic_symbols.hpp>
#include <boost/spirit/include/classic_lists.hpp>
#include <boost/spirit/include/classic_escape_char.hpp>
#include <boost/spirit/include/classic_distinct.hpp>
#include <boost/spirit/include/classic_grammar_def.hpp>

#include <string>


//============================================================================================ command grammar

enum sql_parser_ids
{
//select
	select_clause_id = 1,
	fields_list_id,
	order_pair_id,
//insert
	insert_clause_id ,
	insert_fields_list_id,	
	insert_values_list_id,
//update 
	update_clause_id,
	update_field_id,
//delete 
	delete_clause_id,

	table_name_id,

};



//============================================================================================ condition grammar


// This enum specifies ids for the parse tree nodes created for each rule.
enum parser_ids
{
    boolean_const_id = 1,
    integer_const_id,
    long_const_id,
    double_const_id,
    string_const_id,
    constant_id,

    function_call_id,
    function_identifier_id,

    varname_id,

    atom_expr_id,

    unary_expr_id,
    mul_expr_id,
    add_expr_id,

    cast_expr_id,
    cast_spec_id,


    comp_expr_id,
    in_expr_id,
    value_list_id,
    and_expr_id,
    or_expr_id,

    expr_id,
    exprlist_id,

    placeholder_const_id,	
};




//using namespace std;
using namespace boost::spirit::classic;


struct sql_grammar :
	public boost::spirit::classic::grammar<sql_grammar>
{
    template <typename ScannerT>
    struct definition
    {

        definition(sql_grammar const& self)
        {
            (void)self;
            //-----------------------------------------------------------------
            // KEYWORDS
            //-----------------------------------------------------------------
            keywords =
                "select", "distinct", "from" , "where" , "in" , 
                "and" , "or" , "as" , "left" , "join" , "on" ,
                "is" , "not" , "null" ,"insert into","values","update","set","delete from","order by","asc","desc",
	// SQL function:
	      "dms"
				
				;
            //-----------------------------------------------------------------
            // OPERATORS
            //-----------------------------------------------------------------

            chlit<>     STAR('*');
            chlit<>     COMMA(',');
            chlit<>     LPAREN('(');
            chlit<>     RPAREN(')'); 
            chlit<>     SEMI(';');
            chlit<>     LT('<');
            chlit<>     GT('>');
            strlit<>    LE("<=");
            strlit<>    GE(">=");
            chlit<>     EQUAL('=');
            strlit<>    NE("!=");    
            chlit<>     DOT('.');       
	

            //---------------------------------------
            // TOKENS
            //---------------------------------------
            typedef inhibit_case<strlit<> > token_t;

            token_t SELECT      = as_lower_d["select"];
            token_t ORDER       = as_lower_d["order by"];		
       	    token_t ASC         = as_lower_d["asc"];
            token_t DESC        = as_lower_d["desc"];	   
 
            token_t INSERT      = as_lower_d["insert into"];
            token_t VALUES      = as_lower_d["values"];

            token_t UPDATE      = as_lower_d["update"];			
            token_t SET         = as_lower_d["set"];	
			
            token_t DELETE      = as_lower_d["delete from"];	

			
            token_t DISTINCT    = as_lower_d["distinct"];
            token_t FROM        = as_lower_d["from"];
            token_t LEFT        = as_lower_d["left"];
            token_t JOIN        = as_lower_d["join"];
            token_t ON          = as_lower_d["on"];
            token_t WHERE       = as_lower_d["where"];
            token_t IN_         = as_lower_d["in"];
            token_t AND         = as_lower_d["and"];
            token_t OR          = as_lower_d["or"];
            token_t AS          = as_lower_d["as"];
            token_t IS          = as_lower_d["is"];
            token_t NOT         = as_lower_d["not"]; 
            token_t NULL_       = as_lower_d["null"];  
            token_t DMS		= as_lower_d["dms"];    

          //-----------------------------------------
          //	Start grammar definition
          //-----------------------------------------

		constant
		=  double_const
		| integer_const
		| long_const
		| boolean_const
		| string_const
		 ;
		distinct_parser<> keyword_p("a-zA-Z0-9_");

	    boolean_const
		= as_lower_d[keyword_p("true") | keyword_p("false")]
		;

	    integer_const
		= int_p
		;

	    // this is needed because spirit's int_parsers don't work with
	    // these long numbers
	    long_const
		= token_node_d[ lexeme_d[ !( ch_p('+') | ch_p('-' ) ) >> +( range_p('0','9') ) ] ]
		;

	    double_const
		= strict_real_p
		;

        string_const
            = lexeme_d[
						token_node_d[( '\'' >> *(c_escape_ch_p - '\'') >> '\'')|('"' >> *(c_escape_ch_p - '"') >> '"') ]
                ];

        identifier = nocase_d[ lexeme_d[(alpha_p  >> *(alnum_p | ch_p('_')))]];

 //-----------------------------------------------------------------
 // RULES
 //-----------------------------------------------------------------

    program =  +(query) ; 
                     
    query
    = root_node_d[ 
	 select_from_clause 
	|insert_clause 
	|update_clause
	|delete_clause
	];   //  >> SEMI


//  <select------------------------------------------------------------			
    select_from_clause
    = root_node_d[ select_clause] >> from_clause >> !order_clause ; 
            
    select_clause 
		= root_node_d[SELECT]>> fields_list  ;

	fields_list
		= ( STAR | lexeme_d[list_p( fields_name >> !alias_clause, no_node_d[COMMA] )] );
            
	fields_name = leaf_node_d[identifier] ;

    from_clause 
    =  root_node_d[FROM] >> table_name ; 
    

	order_clause = root_node_d[ORDER]>> list_p(order_pair % ch_p(','));

	order_pair = leaf_node_d[fields_name]>> root_node_d[ASC|DESC];
	

//<insert ------------------------------------------------------------	

insert_clause
	= root_node_d[INSERT]>> insert_fields_list   >> insert_values_list  ;

//need to judge wether have child
insert_fields_list
	= ( root_node_d [ table_name ] >>  
	 no_node_d[LPAREN] >>  
	 lexeme_d[list_p( fields_name >> !alias_clause, no_node_d[COMMA] )]
	>> no_node_d[RPAREN] ) | root_node_d [ table_name ]
;

insert_values_list
	= root_node_d [ VALUES ] >>  	               
	  discard_node_d[ ch_p('(') ] >>  infix_node_d[ constant%ch_p(',')] >> discard_node_d[ ch_p(')') ];

		
//<update-----------------------------------------------------------
update_clause
	= root_node_d[UPDATE]>>  table_name   >> update_fields_list  ;

update_fields_list
	= root_node_d[SET] >>  list_p( update_field % no_node_d[COMMA] );
update_field
	=  leaf_node_d[fields_name]>>root_node_d[EQUAL]>> leaf_node_d[constant];


//<delete-----------------------------------------------------------
delete_clause
	= root_node_d[DELETE]>>  table_name    ;


//<common---------------------------------------------------------

	alias_clause 
    = AS >> alias ; 

	table_name = leaf_node_d[identifier] ;
    alias = identifier ; 

	function_value_list   // 多一条中间规则，树就会多一层
  = root_node_d[!(DMS)]>>  
      no_node_d[LPAREN] >>  
	  leaf_node_d[ constant ] % no_node_d[COMMA] 
	>> no_node_d[RPAREN] ;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
    op = +( LT | LE | EQUAL | GE | GT | NE 
        | "lt" | "le" | "eq" | "ge" | "gt" | "ne" ) ;      
                                                                                                                                                                                                                                                                              
   }

      rule<ScannerT> const&
        start() const { return program; }
                  
//  Declare the keywords here         
        symbols<> keywords;
        
        rule<ScannerT> 
        program, query, select_from_clause, from_clause,  
        alias_clause, table_alias, alias, 
        fields_name,	function_value_list,
		op, identifier, 
		constant,boolean_const,integer_const,long_const,double_const,string_const,
		update_fields_list,order_clause
		;   
          
// select
		rule<ScannerT, parser_context<>, parser_tag<select_clause_id> > select_clause;
		rule<ScannerT, parser_context<>, parser_tag<fields_list_id> > fields_list;
		rule<ScannerT, parser_context<>, parser_tag<table_name_id> > table_name;

		rule<ScannerT, parser_context<>, parser_tag<order_pair_id> > order_pair;			
		
//insert into 
		rule<ScannerT, parser_context<>, parser_tag<insert_clause_id> > insert_clause;
		rule<ScannerT, parser_context<>, parser_tag<insert_fields_list_id> > insert_fields_list;             
		rule<ScannerT, parser_context<>, parser_tag<insert_values_list_id> > insert_values_list;   

// update 
		rule<ScannerT, parser_context<>, parser_tag<update_clause_id> > update_clause;
		rule<ScannerT, parser_context<>, parser_tag<update_field_id> > update_field;			 
		
//delete
		rule<ScannerT, parser_context<>, parser_tag<delete_clause_id> > delete_clause;


    };
};

//============================================================================================ condition grammar


/// Keyword parser used for matching words with () and spaces as separators.
distinct_parser<> keyword_p("a-zA-Z0-9_");

/// The boost::spirit expression parser grammar
 struct ExpressionGrammar : public grammar<ExpressionGrammar>
{
    /// The boost::spirit expression parser grammar definition (for a specific
    /// scanner) with two entry points.
    template <typename ScannerT>
    struct definition : public grammar_def<rule<ScannerT, parser_context<>, parser_tag<expr_id> >,
					   rule<ScannerT, parser_context<>, parser_tag<exprlist_id> > >
    {
	/// Real definition function of the grammar.
	definition(ExpressionGrammar const& /*self*/)
	{
	    // *** Constants

	    constant
		= placeholder_const   //优先匹配place holder
		| double_const
		| integer_const
		| long_const
		| boolean_const
		| string_const
		 ;
	    
	    boolean_const
		= as_lower_d[keyword_p("true") | keyword_p("false")]
		;

	    integer_const
		= int_p
		;

	    // this is needed because spirit's int_parsers don't work with
	    // these long numbers
	    long_const
		= token_node_d[ lexeme_d[ !( ch_p('+') | ch_p('-' ) ) >> +( range_p('0','9') ) ] ]
		;

	    double_const
		= strict_real_p
		;

        string_const
            = lexeme_d[
						token_node_d[( '\'' >> *(c_escape_ch_p - '\'') >> '\'')|('"' >> *(c_escape_ch_p - '"') >> '"') ]
                ];

	     placeholder_const
		=   lexeme_d[
 		       token_node_d[ ('"' >> *ch_p('?')  >> '"') |ch_p('?') ]
 		   ]
 	      ;



	    // *** Function call and function identifier

            function_call
                = root_node_d[function_identifier]
		>> discard_node_d[ ch_p('(') ] >> exprlist >> discard_node_d[ ch_p(')') ]
                ;

            function_identifier
                = lexeme_d[ 
		    token_node_d[ alpha_p >> *(alnum_p | ch_p('_')) ]
                    ]
                ;

	    // *** Expression names

            varname
                = lexeme_d[ 
		    token_node_d[ alpha_p >> *(alnum_p | ch_p('_')) ]
                    ]
                ;

	    // *** Valid Expressions, from small to large

            atom_expr
                = constant
                | inner_node_d[ ch_p('(') >> expr >> ch_p(')') ]
                | function_call
	       | varname
                ;

            unary_expr
                = !( root_node_d[ as_lower_d[ch_p('+') | ch_p('-') | ch_p('!') | str_p("not")] ] )
		>> atom_expr
                ;

	    cast_spec
		= discard_node_d[ ch_p('(') ]
		>> (
		    keyword_p("bool") |
		    keyword_p("char") | keyword_p("short") | keyword_p("int") | keyword_p("integer") | keyword_p("long") |
		    keyword_p("byte") | keyword_p("word") | keyword_p("dword") | keyword_p("qword") |
		    keyword_p("float") | keyword_p("double") |
		    keyword_p("string")
		    )
		>> discard_node_d[ ch_p(')') ]
		;

            cast_expr
		= root_node_d[ !cast_spec ] >> unary_expr
		;

            mul_expr
                = cast_expr
		>> *( root_node_d[ch_p('*') | ch_p('/')] >> cast_expr )
                ;

	    add_expr
		= mul_expr
		>> *( root_node_d[ch_p('+') | ch_p('-')] >> mul_expr )
		;

		value_list
		= discard_node_d[ ch_p('(') ] >>  infix_node_d[ constant%ch_p(',')] >> discard_node_d[ ch_p(')') ];

	    comp_expr
		= add_expr
		>> *( root_node_d[( str_p("==") | str_p("!=") |str_p("in")|
				    str_p("<=") | str_p(">=") | str_p("=<") | str_p("=>") |ch_p('=') | ch_p('<') | ch_p('>') )] 
		>> (add_expr | value_list))
		;

	    and_expr
		= (comp_expr |in_expr)
		>> *( root_node_d[ as_lower_d[str_p("and") | str_p("&&")] ] >> (comp_expr |in_expr ) )
		;

	    or_expr
		= and_expr
		>> *( root_node_d[ as_lower_d[str_p("or") | str_p("||")] ] >> and_expr )
		;

	    // *** Base Expression and List

	    expr
		= or_expr
		;

	    exprlist
		= infix_node_d[ !list_p(expr, ch_p(',')) ]
		;

	    // Special spirit feature to declare multiple grammar entry points
	    this->start_parsers(expr, exprlist); 

#ifdef  DEBUG_PARSER
	    BOOST_SPIRIT_DEBUG_RULE(constant);

	    BOOST_SPIRIT_DEBUG_RULE(boolean_const);
	    BOOST_SPIRIT_DEBUG_RULE(integer_const);
	    BOOST_SPIRIT_DEBUG_RULE(long_const);
	    BOOST_SPIRIT_DEBUG_RULE(double_const);
	    BOOST_SPIRIT_DEBUG_RULE(string_const);
		
	    BOOST_SPIRIT_DEBUG_RULE(placeholder_const);

	    BOOST_SPIRIT_DEBUG_RULE(function_call);
	    BOOST_SPIRIT_DEBUG_RULE(function_identifier);
	    
	    BOOST_SPIRIT_DEBUG_RULE(varname);

	    BOOST_SPIRIT_DEBUG_RULE(atom_expr);

	    BOOST_SPIRIT_DEBUG_RULE(unary_expr);
	    BOOST_SPIRIT_DEBUG_RULE(mul_expr);
	    BOOST_SPIRIT_DEBUG_RULE(add_expr);

	    BOOST_SPIRIT_DEBUG_RULE(cast_spec);
	    BOOST_SPIRIT_DEBUG_RULE(cast_expr);

	    BOOST_SPIRIT_DEBUG_RULE(comp_expr);
	    BOOST_SPIRIT_DEBUG_RULE(in_expr);		
	    BOOST_SPIRIT_DEBUG_RULE(and_expr);
	    BOOST_SPIRIT_DEBUG_RULE(or_expr);

	    BOOST_SPIRIT_DEBUG_RULE(expr);
	    BOOST_SPIRIT_DEBUG_RULE(exprlist);
#endif
	}

	/// Rule for a constant: one of the three scalar types integer_const,
	/// double_const or string_const
        rule<ScannerT, parser_context<>, parser_tag<constant_id> > 		constant;

	/// Boolean value constant rule: "true" or "false"
        rule<ScannerT, parser_context<>, parser_tag<boolean_const_id> > 	boolean_const;
	/// Integer constant rule: "1234"
        rule<ScannerT, parser_context<>, parser_tag<integer_const_id> > 	integer_const;
	/// Long integer constant rule: "12345452154"
        rule<ScannerT, parser_context<>, parser_tag<long_const_id> > 		long_const;
	/// Float constant rule: "1234.3"
        rule<ScannerT, parser_context<>, parser_tag<double_const_id> > 		double_const;
	/// String constant rule: with quotes "abc"
        rule<ScannerT, parser_context<>, parser_tag<string_const_id> > 		string_const;

        rule<ScannerT, parser_context<>, parser_tag<placeholder_const_id> > placeholder_const;	

	/// Function call rule: func1(a,b,c) where a,b,c is a list of exprs
        rule<ScannerT, parser_context<>, parser_tag<function_call_id> > 	function_call;
	/// Function rule to match a function identifier: alphanumeric and _
	/// are allowed.
        rule<ScannerT, parser_context<>, parser_tag<function_identifier_id> > 	function_identifier;

	/// Rule to match a variable name: alphanumeric with _
        rule<ScannerT, parser_context<>, parser_tag<varname_id> > 		varname;

	/// Helper rule which implements () bracket grouping.
        rule<ScannerT, parser_context<>, parser_tag<atom_expr_id> > 		atom_expr;

	/// Unary operator rule: recognizes + - ! and "not".
        rule<ScannerT, parser_context<>, parser_tag<unary_expr_id> > 		unary_expr;
	/// Binary operator rule taking precedent before add_expr:
	/// recognizes * and /
        rule<ScannerT, parser_context<>, parser_tag<mul_expr_id> > 		mul_expr;
	/// Binary operator rule: recognizes + and -
        rule<ScannerT, parser_context<>, parser_tag<add_expr_id> > 		add_expr;

	/// Match all the allowed cast types: short, double, etc.
        rule<ScannerT, parser_context<>, parser_tag<cast_spec_id> >        	cast_spec;
	/// Cast operator written like in C: (short)
        rule<ScannerT, parser_context<>, parser_tag<cast_expr_id> >        	cast_expr;

	/// Comparison operator: recognizes == = != <= >= < > => =<
        rule<ScannerT, parser_context<>, parser_tag<comp_expr_id> >        	comp_expr;


	  rule<ScannerT, parser_context<>, parser_tag<in_expr_id> >	        in_expr;
	  rule<ScannerT, parser_context<>, parser_tag<value_list_id> > 		value_list;



	
	/// Boolean operator: recognizes && and "and" and works only on boolean
	/// values
        rule<ScannerT, parser_context<>, parser_tag<and_expr_id> >        	and_expr;
	/// Boolean operator: recognizes || and "or" and works only on boolean
	/// values
        rule<ScannerT, parser_context<>, parser_tag<or_expr_id> >        	or_expr;

	/// Base rule to match an expression 
        rule<ScannerT, parser_context<>, parser_tag<expr_id> >        		expr;
	/// Base rule to match a comma-separated list of expressions (used for
	/// function arguments and lists of expressions)
        rule<ScannerT, parser_context<>, parser_tag<exprlist_id> >    		exprlist;
    };
};

#endif
