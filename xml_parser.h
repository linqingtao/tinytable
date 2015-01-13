/**
 *   @file:     xml_parser.h  
 *   @author:   chenwq
 *   @date:     
 *   @brief:    
 *   
 **/

#ifndef _XML_PARSER_H_
#define _XML_PARSER_H_

#include <tinyxml.h>
#include <iostream>
#include <string>
#include <sstream>

namespace google {
namespace protobuf {
class Message;
class FieldDescriptor;
class Reflection;
}
}

namespace tinytable {
/*
 * convert form xml to pb
 */
bool xml2pb(const char * xml, google::protobuf::Message * msg)
    throw();

bool xml2pb(const char* path, const char* file, google::protobuf::Message * msg)
	throw();
}

#endif
