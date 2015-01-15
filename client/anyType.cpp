#include "anyType.h"
#include <boost/lexical_cast.hpp>

//using namespace boost;
//using namespace std;

//will not change the origin value.just get a copy in another type.
template<typename T>
bool converseType(T &temp,int originType,	const boost::any m_val) 
{
						
	try
	{

		switch (originType)
		{

		case ANY_INT:
			{
				 temp = boost::lexical_cast<T>( boost::any_cast<int>(m_val) );
				 return true;
			}
		case ANY_UINT:
			{
				temp = boost::lexical_cast<T>( boost::any_cast<unsigned>(m_val) );
				return true;
			}
		case ANY_LONG_64:
			{
				temp = boost::lexical_cast<T>( boost::any_cast<long long>(m_val) );
				return true;
			}
		case ANY_DOUBLE:
			{
				temp = boost::lexical_cast<T>( boost::any_cast<double>(m_val) );
				return true;
			}
		case ANY_FLOAT:
			{
				temp = boost::lexical_cast<T>( boost::any_cast<float>(m_val) );
					return true;
			}
		case ANY_STRING:
			{
				temp = boost::lexical_cast<T>( boost::any_cast<std::string>(m_val) );
					return true;
			}
		default:
			{
				//error
				return false;
			}
		}//switch
	}//try
	 catch(boost::bad_lexical_cast& e )
	 {
         std::cout << "anyType::converseType():\n"<<e.what()<<std::endl; 
				return false;
	 }
	
}




void anyType::setValue( int _typeNum , std::string _value  )
{
 try 
 {  
	switch(_typeNum)
	{
	case ANY_BOOL:
		 {
			 m_value =  boost::lexical_cast<bool>(_value);
			 return;
		 }
	case ANY_INT:
		{
			 m_value =  boost::lexical_cast<int>(_value);
			 return;
		}
	case ANY_LONG_64:
		{
			 m_value =  boost::lexical_cast<long long>(_value);
			 return;
		}
	case ANY_DOUBLE:
		{
			 m_value =  boost::lexical_cast<double>(_value);
			 return;
		}
	case ANY_FLOAT:
		{
			 m_value =  boost::lexical_cast<float>(_value);
			 return;
		}
	case ANY_STRING:
		{
			 m_value =  _value;
			 return;
		}

	case ANY_UINT:
		{
			 m_value =  boost::lexical_cast<unsigned int>(_value);
			 return;
		}
	default:
		{
			//throw
		}

	} //switch
 }//try
 catch(boost::bad_lexical_cast& e )
 {
     std::cout << "anyType initail error:!\n"<<e.what()<<std::endl; 
 }
}

anyType::~anyType(void)
{
}



bool anyType::getBoolean() const
{
	try
	{
	return boost::any_cast<bool>(m_value);
	}
	catch(boost::bad_any_cast& e) 
	{
        std::cout << "any_cast fail,cannot converse into bool type"<<e.what()<<std::endl; 
	return false;
	}
}

int anyType::getInt() const
{

	try
	{
	return boost::any_cast<int>(m_value);
	}
	catch(boost::bad_any_cast& e) 
	{
	//cout << "any_cast fail,cannot converse into int type"<<e.what()<<endl; 
	//return -1;
	int temp;
	converseType(temp,this->getType(),m_value);
	return temp;

	}
}

unsigned int anyType::getUInt() const
{

	try
	{
	return boost::any_cast<unsigned int>(m_value);
	}
	catch(boost::bad_any_cast& e) 
	{
//	cout << "any_cast fail,cannot converse into unsigned int type"<<e.what()<<endl; 
//		return -1;
	unsigned temp;
	converseType(temp,this->getType(),m_value);
	return temp;
	}
}

long long anyType::getLong() const
{

	try
	{
	return boost::any_cast<long long>(m_value);
	}
	catch(boost::bad_any_cast& e) 
	{
//	cout << "any_cast fail,cannot converse into  long long type"<<e.what()<<endl;
//		return -1;
	long long temp;
	converseType(temp,this->getType(),m_value);
	return temp;
	}
}

double anyType::getDouble() const
{

	try
	{
	return boost::any_cast<double>(m_value);
	}
	catch(boost::bad_any_cast& e) 
	{
	//cout << "any_cast fail,cannot converse into double type"<<e.what()<<endl; 
	//	return -1;
	double  temp;
	converseType(temp,this->getType(),m_value);
	return temp;
		
	}
}
float anyType::getFloat() const
{

	try
	{
	return boost::any_cast<float>(m_value);
	}
	catch(boost::bad_any_cast& e) 
	{
//	cout << "any_cast fail,cannot converse into float type"<<e.what()<<endl; 
//		return -1;
	float  temp;
	converseType(temp,this->getType(),m_value);
	return temp;
	}
}
std::string anyType::getString() const
{
	try
	{
	return boost::any_cast<std::string>(m_value);
	}
	catch(boost::bad_any_cast& e) 
	{
//	cout << "any_cast fail,cannot converse into string type"<<e.what()<<endl; 
//	return "error";
        std::string  temp;
	converseType(temp,this->getType(),m_value);
	return temp;
	}
}

int anyType::getType() const
{
	if(m_value.type() == typeid(bool)) return ANY_BOOL;
	else if(m_value.type() == typeid(int)) return ANY_INT;
	else if(m_value.type() == typeid(unsigned int)) return ANY_UINT;
	else if(m_value.type() == typeid(long long)) return ANY_LONG_64;
	else if(m_value.type() == typeid(double)) return ANY_DOUBLE;
	else if(m_value.type() == typeid(float)) return  ANY_FLOAT;
	else if(m_value.type() == typeid(std::string)) return ANY_STRING;
	else return -1;
}

//OpNum used for build exception
template <template <typename Type> class Operator, int OpNum>
bool anyType::binary_comp_op(const anyType &b) const
{
    switch( this->getType() )
    {
	case ANY_BOOL:
		{
			switch( b.getType() )
			{
			case ANY_BOOL:
				{
				Operator<bool> op;
				return op(this->getBoolean(), b.getBoolean() );
				}
			default:
				{
					// throw
				}
			}
			break;
		}//CASE BOOL
	case ANY_INT:
		{
			switch( b.getType() )
			{
			case ANY_BOOL:
				{
					//throw
				}
			case ANY_INT:
			case ANY_STRING:
				{
				Operator<int> op;
				return op(this->getInt(), b.getInt() );
				}
			case ANY_UINT:
				{
				Operator<unsigned> op;
				return op(this->getUInt(), b.getUInt() );
				}
			case ANY_LONG_64:
				{
				Operator<long long> op;
				return op(this->getLong(), b.getLong() );				
				}
			case ANY_DOUBLE:
				{
				Operator<double> op;
				return op(this->getDouble(), b.getDouble() );				
				}
			case ANY_FLOAT:
				{
				Operator<float> op;
				return op(this->getFloat(), b.getFloat() );				
				}

			default:
				{
					// throw
				}
			}//switch b.type
			break;
		}//case INT
	case ANY_UINT:
		{
		switch( b.getType() )
			{
			case ANY_BOOL:
				{
					//throw
				}
			case ANY_INT:
			case ANY_STRING:
			case ANY_UINT:
				{
				Operator<unsigned> op;
				return op(this->getUInt(), b.getUInt() );
				}
			case ANY_LONG_64:
				{
				Operator<long long> op;
				return op(this->getLong(), b.getLong() );				
				}
			case ANY_DOUBLE:
				{
				Operator<double> op;
				return op(this->getDouble(), b.getDouble() );				
				}
			case ANY_FLOAT:
				{
				Operator<float> op;
				return op(this->getFloat(), b.getFloat() );				
				}

			default:
				{
					// throw
				}
			}//switch b.type
			break;
		}//CASE UNIT
	case ANY_LONG_64:
		{
		switch( b.getType() )
			{
			case ANY_BOOL:
				{
					//throw
				}
			case ANY_INT:
			case ANY_STRING:
			case ANY_UINT:
			case ANY_LONG_64:
				{
				Operator<long long> op;
				return op(this->getLong(), b.getLong() );				
				}
			case ANY_DOUBLE:
				{
				Operator<double> op;
				return op(this->getDouble(), b.getDouble() );				
				}
			case ANY_FLOAT:
				{
				Operator<float> op;
				return op(this->getFloat(), b.getFloat() );				
				}

			default:
				{
					// throw
				}
			}//switch b.type
			break;
		}//case LONG_64
	case ANY_FLOAT:
		{
		switch( b.getType() )
			{
			case ANY_BOOL:
				{
					//throw
				}
			case ANY_INT:
			case ANY_STRING:
			case ANY_UINT:
			case ANY_LONG_64:
			case ANY_FLOAT:
				{
				Operator<float> op;
				return op(this->getFloat(), b.getFloat() );				
				}
			case ANY_DOUBLE:
				{
				Operator<double> op;
				return op(this->getDouble(), b.getDouble() );				
				}
			default:
				{
					// throw
				}
			}//switch b.type
			break;
		}//case FLOAT
	case ANY_DOUBLE:
		{
		switch( b.getType() )
			{
			case ANY_BOOL:
				{
					//throw
				}
			case ANY_INT:
			case ANY_STRING:
			case ANY_UINT:
			case ANY_LONG_64:
			case ANY_FLOAT:
			case ANY_DOUBLE:
				{
				Operator<double> op;
				return op(this->getDouble(), b.getDouble() );				
				}
			default:
				{
					// throw
				}
			}//switch b.type
			break;
		}
	case ANY_STRING:
		{
	switch( b.getType() )
			{
			case ANY_BOOL:
				{
					//throw
				}
			case ANY_STRING:
				{
				Operator<std::string> op;
				return op(this->getString(), b.getString() );
				}
			case ANY_INT:
				{
				Operator<int> op;
				return op(this->getInt(), b.getInt() );
				}
			case ANY_UINT:
				{
				Operator<unsigned> op;
				return op(this->getUInt(), b.getUInt() );
				}
			case ANY_LONG_64:
				{
				Operator<long long> op;
				return op(this->getLong(), b.getLong() );				
				}
			case ANY_DOUBLE:
				{
				Operator<double> op;
				return op(this->getDouble(), b.getDouble() );				
				}
			case ANY_FLOAT:
				{
				Operator<float> op;
				return op(this->getFloat(), b.getFloat() );				
				}

			default:
				{
					// throw
					return false;
				}
			}//switch b.type

			break;
		}//case STRING
	
	}//switch this.type
    return false;
}

//enum TypeOp_t { EQ,NE,LT,GT,LE,GE };

/// Forced instantiation of binary_comp_op for AnyScalar::equal_to()
template bool anyType::binary_comp_op<std::equal_to, EQ>(const anyType &b) const;

/// Forced instantiation of binary_comp_op for AnyScalar::not_equal_to()
template bool anyType::binary_comp_op<std::not_equal_to, NE>(const anyType &b) const;

/// Forced instantiation of binary_comp_op for AnyScalar::less()
template bool anyType::binary_comp_op<std::less, LT>(const anyType &b) const;

/// Forced instantiation of binary_comp_op for AnyScalar::greater()
template bool anyType::binary_comp_op<std::greater, GT>(const anyType &b) const;

/// Forced instantiation of binary_comp_op for AnyScalar::less_equal()
template bool anyType::binary_comp_op<std::less_equal, LE>(const anyType &b) const;

/// Forced instantiation of binary_comp_op for AnyScalar::greater_equal()
template bool anyType::binary_comp_op<std::greater_equal, GE>(const anyType &b) const;


//int main()
//{
//	anyType A(ANY_INT,"123");
//	anyType B(ANY_FLOAT,"123.5");
//	if(A<=B)
//	{
//		cout<<"A<=B\n";
//	}
//	else
//	{
//		cout<<"error\n";
//	}
//	anyType X(ANY_INT,"123");
//	anyType Y(ANY_STRING,"123");
//	if(X==Y)
//	{
//		cout<<"X==Y\n";
//	}
//	else
//	{
//		cout<<"error\n";
//	}
//	system("pause");
//}
