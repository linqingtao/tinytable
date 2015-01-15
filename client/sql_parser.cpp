#include "sql_parser.h"
#include "sqlGrammar.hpp"

#include <stdio.h>
#include <fstream>
#include <map>
#include <iostream>
#include <sstream>
#include <cwctype>
#include <boost/lexical_cast.hpp>


namespace tinytable {

    std::string& trim(std::string& s) {
   if (s.empty()) {
     return s;
   }
   
   std::string::iterator c;
   // Erase whitespace before the string
   for (c = s.begin(); c != s.end() && iswspace(*c++););
     s.erase(s.begin(), --c);
 
   // Erase whitespace after the string
   for (c = s.end(); c != s.begin() && iswspace(*--c););
     s.erase(++c, s.end());
     // trim ' or "
    int len = s.length();
    if ((s[0] == '\'' && s[len - 1] == '\'') || (s[0] == '\"' && s[len - 1] == '\"')) {
        s = s.substr(1, len - 2);
    }
   return s;
 }

 ParseNode::ParseNode( TreeIterT const& i ,node_t _Type,int _index )
	 	 :ast_node_ptr(i),NodeType(_Type),placeholder_index(_index)
 {
	 TypeNum_t valueType;
	 switch (_Type)
	 {
	 case BOOL:
		 {
			 valueType = ANY_BOOL;
			 break;
		 }
	 case INT:
		 {
			 valueType = ANY_INT;
			 break;
		 }
	 case LONG:
		 {
			 valueType = ANY_LONG_64;
			 break;
		 }
	 case DOUBLE:
		 {
			 valueType = ANY_DOUBLE;
			 break;
		 }
	 case OP:
	 case STRING:
	 case PLACEHOLDER:
	 default:
		 {
			 valueType = ANY_STRING;
			 break;
		 }
	 }
     std::string str = std::string(i->value.begin(), i->value.end());
     trim(str);
	 m_valuePtr = new anyType( valueType ,str);
	
 }

 ParseNode::~ParseNode() {
    if (m_valuePtr != NULL) {
        delete m_valuePtr;
        m_valuePtr = NULL;
    }
 }


 ParseNode::ParseNode( TreeIterT const& i ,node_t _Type,int _index ,ParseNodeBase* _left,ParseNodeBase* _right)
	 	 : left(_left),right(_right), ast_node_ptr(i),NodeType(_Type),placeholder_index(_index)
 {
	 TypeNum_t valueType;
	 switch (_Type)
	 {
	 case BOOL:
		 {
			 valueType = ANY_BOOL;
			 break;
		 }
	 case INT:
		 {
			 valueType = ANY_INT;
			 break;
		 }
	 case LONG:
		 {
			 valueType = ANY_LONG_64;
			 break;
		 }
	 case DOUBLE:
		 {
			 valueType = ANY_DOUBLE;
			 break;
		 }
	 case OP:
	 case STRING:
	 case PLACEHOLDER:
	 default:
		 {
			 valueType = ANY_STRING;
			 break;
		 }
	 }
	 m_valuePtr = new anyType( valueType ,std::string(i->value.begin(), i->value.end()) );
 }


 node_t ParseNode::GetNodeType()
 {
	return NodeType;
 }

 ParseFunc::ParseFunc( TreeIterT const& i ,std::string _funcname, std::vector<anyType*> _paramlist)
	 :ast_node_ptr(i),funcname(_funcname),paramlist(_paramlist)
 {

	 NodeType = FUNC;
 }


 node_t ParseFunc::GetNodeType()
 {
	return NodeType;
 }

 ParseFunc::~ParseFunc() {
     std::vector<anyType*>::iterator it = paramlist.begin();
     std::vector<anyType*>::iterator it_end = paramlist.end();
     for (; it != it_end; ++it) {
         delete (*it);
     }
     paramlist.clear();
 }

sql_parser::sql_parser(void)
: m_begin(1)
, m_end(-1)
{
}

sql_parser::~sql_parser(void)
{
    clear();
}

void sql_parser::clear() {
    m_begin = 1;
    m_end = -1;
    commandtype = 0;
    m_tablename = "";
    std::vector<std::string> tmp_fields;
    m_fields.swap(tmp_fields);
    m_fields.clear();
    fieldsMapType tmp_maps;
    m_fields_map.swap(tmp_maps);
    m_fields_map.clear();
    std::vector<struct order> tmp_order;
    m_orders.swap(tmp_order);
    m_orders.clear();
    std::list<ParseNodeBase*>::iterator iter = m_treestack.begin();
    std::list<ParseNodeBase*>::iterator end = m_treestack.end();
    for (; iter != end; ++iter) {
        if (*iter != NULL) {
            delete *iter;
        }
    }
    m_treestack.clear();
}

void sql_parser::ScanTree(TreeIterT & i)
{
    switch(i->value.id().to_long())
    {
		case select_clause_id:
		{
			commandtype = select_t;
			int childnum = i->children.size() ;
			for ( int n =0;n<childnum;n++)
				{
                    TreeIterT it = i->children.begin()+n;
				    ScanTree(it);			
				} 
			break;
		}
		case fields_list_id:
		{
			int childnum = i->children.size();
			for(int index = 0; index<childnum;index++)
			{
                std::string str = std::string(i->children[index].value.begin(), i->children[index].value.end() );
                trim(str);
				m_fields.push_back(str);
			}
			break;

		}
		case order_pair_id:
		{
			struct order current;
            std::string dir = std::string(i->value.begin(),i->value.end());
			if(dir == "desc" ||dir =="DESC" )
				current.direction = DESC;
			else
				current.direction = ASC;		
			current.varname = std::string( i->children[0].value.begin(), i->children[0].value.end() );
			m_orders.push_back(current);
			break;

			}
		case delete_clause_id:
		{
			commandtype = delete_t;
            TreeIterT it = i->children.begin();
			ScanTree(it);
			break;
		}
		case table_name_id:
		{
			//没有联表查询
			m_tablename = std::string(i->value.begin(), i->value.end());
            trim(m_tablename);
			int childnum = i->children.size() ;
			for(int index = 0; index<childnum;index++)
			{
                std::string str = std::string(    i->children[index].value.begin(), i->children[index].value.end() );
                trim(str);
				m_fields.push_back(str);
			}
			break;
		}
		case insert_clause_id:
		{
			commandtype = insert_t;
            TreeIterT it = i->children.begin();
			ScanTree(it);
            TreeIterT iter = i->children.begin() + 1;
			ScanTree(iter);
			break;
		} 
		case insert_values_list_id:
		{
			int childnum = i->children.size() ;
			for(int index = 0; index<childnum;index++)
			{
                std::string str = std::string( i->children[index].value.begin(), i->children[index].value.end() );
                trim(str);
				m_fields_map.insert( make_pair(m_fields[index], str));
			}
			break;
		}
		case update_clause_id:
		{
			commandtype = update_t;
            TreeIterT it = i->children.begin();
			ScanTree(it);
            it = i->children.begin() + 1;
			ScanTree(it);	
			break;			
		}
		case update_field_id:
		{
			int childnum = i->children.size() ;
			
			for(int index = 0; index<childnum;index++)
			{
                std::string str1 = std::string(  i->children.begin()->value.begin(),     i->children.begin()->value.end() );
                std::string str2 = std::string( (i->children.begin()+1)->value.begin(), (i->children.begin()+1)->value.end() );
                trim(str1);
                trim(str2);
				m_fields_map.insert( make_pair(str1, str2));
			}								
			break;	
		}
    default:
		{
			int childnum = i->children.size() ;
			for ( int n =0;n<childnum;n++)
				{
                    TreeIterT it = i->children.begin()+n;
				    ScanTree(it);			
				} 
			break;
		}

	}
}


bool sql_parser::ParseSQLString()
{
        // get limit
    int len = metaString.length();
    if (metaString[len - 1] == ';') {
       metaString  = metaString.substr(0, len - 1);
    }
    std::string::size_type nFind = metaString.find("limit");
        if (nFind==std::string::npos) {
            nFind = metaString.find("LIMIT");
        }
        if (nFind!=std::string::npos) {
            std::string limit = metaString.substr(nFind);
            metaString = metaString.substr(0, nFind - 1);
            nFind = limit.find(",");
            if (nFind == std::string::npos) {
                m_end = atoi(limit.substr(6).c_str());
                if (m_end < 0) {
                    m_end = -1;
                }
            } else {
                m_begin = atoi(limit.substr(6, nFind).c_str());
                m_end = atoi(limit.substr(nFind + 1).c_str());
                if (m_end < 0) {
                    m_end = -1;
                }
                if (m_begin <= 0) {
                    m_begin = 1;
                }
            }
        }
        //nFind = metaString.find_last_not_of(" ");
        //metaString = metaString.substr(0, nFind + 1);
		trim(metaString);
        nFind = metaString.find("where");
		if(nFind==std::string::npos)
	    nFind = metaString.find("WHERE");
        std::string beforeWhere;
        std::string afterWhere;

		if (nFind != std::string::npos) {
		//还需要做一些 字串规范性处理
			beforeWhere = metaString.substr(0,nFind-1) ; 
			afterWhere = metaString.substr(nFind +5,metaString.length() ) ;  //exclude "where" ";" 
            std::string::size_type nFind = afterWhere.rfind(';');
			if (nFind != std::string::npos) afterWhere = afterWhere.substr(0,nFind) ;
			
			trim(beforeWhere);
			trim(afterWhere);	
		  
		}
		else
		{
			beforeWhere = metaString;
			afterWhere = "";
		}	

	sql_grammar command_grammar;

#ifdef STX_DEBUG_PARSER
    BOOST_SPIRIT_DEBUG_GRAMMAR(command_grammar);
#endif

	const std::string temp = beforeWhere;
	tree_parse_info<InputIterT> info = ast_parse(temp.begin(), temp.end(), command_grammar, space_p);

    if (!info.full)
    {
		std::ostringstream oss;
		oss << "Syntax error at position "
			<< static_cast<int>(info.stop - temp.begin())
			<< " near " 
			<< std::string(info.stop, temp.end());

        std::cout << oss.str();
	//	throw(stx::BadSyntaxException(oss.str()));
		return false;
    }

	else
    {
        TreeIterT it = info.trees.begin();
		ScanTree(it);
    }


	// condition tree
	if(afterWhere!="")
	{



			ExpressionGrammar condition_grammar;

	#ifdef STX_DEBUG_PARSER
		BOOST_SPIRIT_DEBUG_GRAMMAR(command_grammar);
	#endif

		const std::string temp = afterWhere;
		tree_parse_info<InputIterT> ConditionTreeInfo = ast_parse(temp.begin(), temp.end(), condition_grammar, space_p);

		if (!info.full)
		{
			std::ostringstream oss;
			oss << "Syntax error at position "
				<< static_cast<int>(info.stop - temp.begin())
				<< " near " 
				<< std::string(info.stop, temp.end());

            std::cout << oss.str();
		//	throw(stx::BadSyntaxException(oss.str()));
			return false;
		}

		else
		{
			int index=0;
			BuildTreeStackByAST( ConditionTreeInfo.trees.begin(),index );
		}

	}
	/*DumpTreeStack();
    printf("commandtype %d table %s being %d end %d\n", commandtype, m_tablename.c_str(), m_begin, m_end);
    fieldsVectorType::iterator field_iter = m_fields.begin();
    for (; field_iter != m_fields.end(); ++field_iter) {
        printf("field %s\n", field_iter->c_str());
    }
    fieldsMapType::iterator map_iter = m_fields_map.begin();
    for (; map_iter != m_fields_map.end(); ++map_iter) {
        printf("map %s %s\n", map_iter->first.c_str(), map_iter->second.c_str());
    }
    vector<struct order>::iterator order_iter = m_orders.begin();
    for (; order_iter != m_orders.end(); ++order_iter) {
        printf("order %s %d\n", order_iter->varname.c_str(), order_iter->direction);
    }*/
    return true;

}

ParseNodeBase* sql_parser::BuildTreeStackByAST( TreeIterT const& i ,int &index)
{
	ParseNodeBase* tempNode;
  switch(i->value.id().to_long())
    {
		case boolean_const_id:
		{
			tempNode = new ParseNode( i, BOOL, -1 );
			m_treestack.push_back(tempNode);  
			return tempNode;

		 }
		case integer_const_id:
		{
			tempNode = new ParseNode( i, INT, -1 );
			m_treestack.push_back(tempNode);
			return tempNode;

		 }
		case long_const_id:
		{
			tempNode = new ParseNode( i, LONG, -1 );
			m_treestack.push_back(tempNode);
			return tempNode;

		 }
	    case double_const_id:
		{
			tempNode = new ParseNode( i, DOUBLE, -1 );
			m_treestack.push_back(tempNode);
			return tempNode;

		 }
		case string_const_id:
		{
			tempNode = new ParseNode( i, STRING, -1 );
			m_treestack.push_back(tempNode);
			return tempNode;

		 }
		case varname_id:
		{
			tempNode = new ParseNode( i, VARNAME, -1 );
			m_treestack.push_back(tempNode);
			return tempNode;

		 }
		case placeholder_const_id:
		{
			index++;
			tempNode = new ParseNode( i, PLACEHOLDER, index );
			m_treestack.push_back(tempNode);
			return tempNode;
		} 
		case comp_expr_id:
		case and_expr_id:
		case or_expr_id:
			{

				ParseNodeBase* left =  BuildTreeStackByAST(i->children.begin(),index);
				ParseNodeBase* right = BuildTreeStackByAST(i->children.begin()+1,index);
				tempNode = new ParseNode( i, OP, -1,left,right );

				m_treestack.push_back(tempNode);
				return tempNode;

			}
    case function_identifier_id:
    {
	std::string funcname(i->value.begin(), i->value.end());

	std::vector<anyType*> paramlist;


	if (i->children.size() > 0)
	{
	    TreeIterT const& paramlistchild = i->children.begin();

	    if (paramlistchild->value.id().to_long() == exprlist_id)
	    {
			try
			{
				for(TreeIterT ci = paramlistchild->children.begin(); ci != paramlistchild->children.end(); ++ci)
				{
					switch ( ci->value.id().to_long() )
					{
					case boolean_const_id:
						  {
							  // why we recognized it as a boolean? beause it looks lick  true | false ?  
                              std::string temp = std::string(ci->value.begin(), ci->value.end());
							  if(temp == "true"|| temp == "TRUE")
							  paramlist.push_back( new anyType( ANY_BOOL, true ));

							  else if(temp == "false"|| temp == "FALSE")
							  paramlist.push_back( new anyType( ANY_BOOL, false));
							  else
							  {
								  //throw 
							  }
							  continue;
						  }
					case integer_const_id:
						  {  
							  paramlist.push_back(  new
										  anyType( ANY_INT, 
													boost::lexical_cast<int>( std::string(ci->value.begin(), ci->value.end()) ) 
												 )
												);
							  continue;

						  }
					case long_const_id:
						  {  
							  paramlist.push_back(  new
											  anyType( ANY_LONG_64, 
													   boost::lexical_cast<long long>( std::string(ci->value.begin(), ci->value.end()) ) 
												 )
												);
							  continue;

						  }
					case double_const_id:
						  {  
							  paramlist.push_back(  new
										  anyType( ANY_DOUBLE, 
												   boost::lexical_cast<double>( std::string(ci->value.begin(), ci->value.end()) ) 
												 )
												);
							  continue;

						  }
					case string_const_id:
					case varname_id:
						  {
							  paramlist.push_back( new anyType( ANY_STRING,std::string(ci->value.begin(), ci->value.end()) ));
							  continue;
						  }
					default:
						{
							  paramlist.push_back( new anyType( ANY_STRING,std::string(ci->value.begin(), ci->value.end()) ));
							  continue;
						}
					}
				}
			}
			catch (...) // need to clean-up
			{
				throw;
			}
		}
	    else
	    {
		// just one subnode and its not a full expression list
		 paramlist.push_back( new anyType( ANY_STRING,std::string(paramlistchild->value.begin(), paramlistchild->value.end()) ));
	    }


		}//if (i->children.size() > 0)

		tempNode = new ParseFunc(i,funcname,paramlist);
		m_treestack.push_back(tempNode);
        return tempNode;

	}//case func

    default:
		{
            std::cout<<"Unknown AST parse tree node found. This should never happen."<<std::endl;
		//	throw(ExpressionParserException("Unknown AST parse tree node found. This should never happen."));
		return tempNode =0;
		}
	}
}


void sql_parser::DumpTreeStack()
{
    std::cout<<"DumpTreeStack:"<<std::endl;
	while( !m_treestack.empty())
	{
		ParseNodeBase* curNode =  m_treestack.front();
		if( curNode->GetNodeType() == FUNC)
		{
            std::cout<<"ParseFunc: "<< ((ParseFunc*)curNode)->funcname<<std::endl;
		}
		else
		{
            std::cout<<"ParseNode: "<< *(((ParseNode*)curNode)->m_valuePtr) <<"Type: "<<((ParseNode*)curNode)->NodeType<<std::endl;
		}
		m_treestack.pop_front();
	}

}


void sql_parser::SetSQLString(const std::string &s)
{
    clear();

	metaString = s; 
}



 anyType ParseNode::evaluate(CObject *obj) const
{
	//{  BOOL=0,INT ,LONG,DOUBLE,STRING,VARNAME ,OP,FUNC,PLACEHOLDER }
	switch( NodeType )	
	{
	case BOOL:
	case INT:
	case LONG:
	case DOUBLE:
	case STRING:
		{
			return *m_valuePtr;
		}
	case VARNAME:
		{
			return obj->GetValueByName( m_valuePtr->getString() );
		}
	case OP:
		{
			//can deal with "Var in DMS(a,b,c)" except "Var in (a,b,c)"
			anyType vl = left->evaluate(obj); //value
			anyType vr = right->evaluate(obj);		
            std::string opStr = m_valuePtr->getString();

			if(opStr == "==") return anyType(ANY_BOOL, vl==vr );
			else if(opStr == "!=")	return anyType(ANY_BOOL, vl!=vr );
			else if(opStr == ">=")  return anyType(ANY_BOOL, vl>=vr );
			else if(opStr == "<=") return anyType(ANY_BOOL, vl<=vr );
			else if(opStr == ">") return anyType(ANY_BOOL, vl>vr );
			else if(opStr == "<") return anyType(ANY_BOOL, vl<vr );
			else if(opStr == "or" || opStr == "OR") 
			{
				if( vl.getType()!=ANY_BOOL || vr.getType()!=ANY_BOOL )
				{
					//throw
				}
				else
					return anyType(ANY_BOOL, vl.getBoolean() || vr.getBoolean() );
			}
			else if(opStr == "and" || opStr == "AND")
			{
				if( vl.getType()!=ANY_BOOL || vr.getType()!=ANY_BOOL )
				{
					//throw
				}
				else
					return anyType(ANY_BOOL, vl.getBoolean() && vr.getBoolean() );
			}
			else{ //throw illegal op 
				}

		}
	
	default: 
		{
            throw;
			// throw illegal node type
		}
	}
    return anyType();
}

 anyType ParseFunc::evaluate(CObject *obj) const
{
    (void)obj;
	return  anyType(ANY_BOOL,true );
}

}
