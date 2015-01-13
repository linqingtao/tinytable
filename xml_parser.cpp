/**
 *   @file:     xml_parser.cpp
 *   @author:   chenwq
 *   @date:     
 *   @brief:    
 *   
 **/

#include "xml_parser.h"

#include <google/protobuf/repeated_field.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

#include <typeinfo>       // std::bad_cast

namespace tinytable {

bool var2pb(const TiXmlNode* var, google::protobuf::Message* msg);
bool set_value(google::protobuf::Message * msg,
        const google::protobuf::FieldDescriptor * field_descriptor,
        const google::protobuf::Reflection * reflection,
        const TiXmlNode* value);
bool add_value(google::protobuf::Message * msg,
        const google::protobuf::FieldDescriptor * field_descriptor,
        const google::protobuf::Reflection * reflection,
        const TiXmlNode* value);

//bool var should be 0 for false and others for true
template <typename R>
const R lexical_cast(const std::string& s)
{
    std::stringstream ss(s);

    R result;
    if ((ss >> result).fail() || !(ss >> std::ws).eof())
    {
        throw std::bad_cast();
    }
    return result;
}

#define _PS_SKIP_NULL(item) \
    if(NULL == item) return true;


bool set_value(google::protobuf::Message * msg,
        const google::protobuf::FieldDescriptor * field_descriptor,
        const google::protobuf::Reflection * reflection,
        const TiXmlNode* value)
{
    bool reuired = field_descriptor->is_required();
    const char* field_name = field_descriptor->name().c_str();

    #define _PS_SKIP_NULL_REQ(item, required) \
    if (NULL == item) {    \
        if(required) { return false;}  \
        return true; \
    }

    _PS_SKIP_NULL_REQ(value, reuired);

    try {

        if (field_descriptor->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) 
        {
            return var2pb(value, reflection->MutableMessage(msg,field_descriptor));
        }

        const char* attr_value = value->ToElement()->Attribute(field_name);

        _PS_SKIP_NULL_REQ(attr_value, reuired);

        if (field_descriptor->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_ENUM)
        {
            int tmp = lexical_cast< int32_t >(attr_value);
            const google::protobuf::EnumValueDescriptor * enum_value = field_descriptor->enum_type()->FindValueByNumber(tmp);
            if (!enum_value)
            {
                //printf("enum value[%d] not existed",tmp);
                return false;
            }
            reflection->SetEnum(msg,field_descriptor,enum_value);
            return true;
        }

        if (field_descriptor->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING)
        {
            reflection->SetString(msg,field_descriptor, attr_value);
            return true;
        }

        #define SET_VALUE(des_type,value_type,set_type) \
        if (field_descriptor->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_##des_type) { \
            reflection->Set##set_type(msg, field_descriptor, lexical_cast< value_type >(attr_value)); \
            return true; \
        }

        SET_VALUE(INT32,int32_t,Int32);
        SET_VALUE(INT64,int64_t,Int64);
        SET_VALUE(UINT32,uint32_t,UInt32);
        SET_VALUE(UINT64,uint64_t,UInt64);
        SET_VALUE(DOUBLE,double,Double);
        SET_VALUE(FLOAT,float,Float);
        SET_VALUE(BOOL,bool,Bool);
    }
    catch (const std::exception& cre)
    {
        //printf("set value failed, %s\n",cre.what());
        return false;
    }
    return true;
}

bool add_value(google::protobuf::Message * msg,
        const google::protobuf::FieldDescriptor * field_descriptor,
        const google::protobuf::Reflection * reflection,
        const TiXmlNode* value)
{
    _PS_SKIP_NULL(value);
 
    try {

        if (field_descriptor->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE)
        {
            return var2pb(value, reflection->AddMessage(msg,field_descriptor));
        }

        const char* ele_value = value->ToElement()->GetText();
        _PS_SKIP_NULL(ele_value);

        if (field_descriptor->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING)
        {
            reflection->AddString(msg,field_descriptor,ele_value);
        }

        if (field_descriptor->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_ENUM)
        {
            int tmp = lexical_cast< int32_t >(ele_value);
            const google::protobuf::EnumValueDescriptor * enum_value
                = field_descriptor->enum_type()->FindValueByNumber(tmp);
            if (!enum_value)
            {
                //printf("enum value[%d] not existed\n",tmp);
                return false;
            }
            reflection->AddEnum(msg,field_descriptor,enum_value);
            return true;
        }

        #define ADD_VALUE(des_type,value_type,set_type)\
        if (field_descriptor->cpp_type() \
                == google::protobuf::FieldDescriptor::CPPTYPE_##des_type) { \
            reflection->Add##set_type(msg,field_descriptor, \
                    lexical_cast< value_type >(ele_value));    \
            return true;   \
        }

        ADD_VALUE(INT32,int32_t,Int32);
        ADD_VALUE(INT64,int64_t,Int64);
        ADD_VALUE(UINT32,uint32_t,UInt32);
        ADD_VALUE(UINT64,uint64_t,UInt64);
        ADD_VALUE(DOUBLE,double,Double);
        ADD_VALUE(FLOAT,float,Float);
        ADD_VALUE(BOOL,bool,Bool);
    } catch (const std::exception& cre) {
        //printf("add value failed, %s\n",cre.what());
        return false;
    }
    return true;
}

bool var2pb(const TiXmlNode* xml, google::protobuf::Message* msg)
{
    if (!xml) {
        //printf("param var xml is not a valid xml document");
        return false;
    }
    const google::protobuf::Descriptor* descriptor  = msg->GetDescriptor();
    const google::protobuf::Reflection * reflection = msg->GetReflection();
    int field_num = descriptor->field_count();
    for(int i=0;i<field_num;i++) {
        const google::protobuf::FieldDescriptor * field_descriptor = descriptor->field(i);
        const char* field_name = field_descriptor->name().c_str();

        //judge to get attribute or element
        if(field_descriptor->is_repeated())
        {
            //printf("field %s repeated\n", field_name);
            const TiXmlNode *previous = NULL, *current = NULL;
            current = xml->FirstChild(field_name);
            while(NULL != current)
            {
                if (!add_value(msg, field_descriptor, reflection, current)) {
                    return false;
                }
                previous = current;
                current = xml->IterateChildren(field_name, previous);
            }
        }
        else if(field_descriptor->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE)
        {
            const TiXmlNode *msg_xml = xml->FirstChild(field_name);
            if(!field_descriptor->is_required() && msg_xml == NULL)
                continue;
            if(!var2pb(msg_xml, reflection->MutableMessage(msg,field_descriptor)))
                return false;
        }
        else
        {
            if (!set_value(msg, field_descriptor, reflection, xml)) 
            {
                return false;
            }
        }
    }
    return true;
}

bool xml2pb(const char* xml, google::protobuf::Message * msg)
    throw ()
{
    if (!msg || !xml) {
        //printf("xml2pb param null");
        return false;
    }

    try {
        TiXmlDocument xml_doc;
        xml_doc.Parse(xml, 0, TIXML_ENCODING_UTF8);
        return var2pb(xml_doc.FirstChild(msg->GetTypeName().c_str()), msg);
    }
    catch (const std::exception& cre) {
        //printf("deserialize var from xml failed: %s", cre.what());
        return false;
    }
    catch (...) {
        //printf("deserialize var from xml failed.");
        //printf("deserialize var from xml failed: %s", xml);
        return false;
    }
}

void combine(char* destination, const char* path1, const char* path2)
{
    if(path1 == NULL && path2 == NULL) {
        strcpy(destination, "");;
    }
    else if(path2 == NULL || strlen(path2) == 0) {
        strcpy(destination, path1);
    }
    else if(path1 == NULL || strlen(path1) == 0) {
        strcpy(destination, path2);
    } 
    else {
        char directory_separator[] = "/";
#ifdef WIN32
        directory_separator[0] = '\\';
#endif
        const char *last_char = path1;
        while(*last_char != '\0')
            last_char++;        
        int append_directory_separator = 0;
        if(strcmp(last_char, directory_separator) != 0) {
            append_directory_separator = 1;
        }
        strcpy(destination, path1);
        if(append_directory_separator)
            strcat(destination, directory_separator);
        strcat(destination, path2);
    }
}

bool xml2pb(const char* path, const char* file, google::protobuf::Message * msg)
    throw ()
{
    char filepath[strlen(path) + strlen(file) + 2];
    combine(filepath, path, file);

    try {
        TiXmlDocument xml_doc;
        if(!xml_doc.LoadFile(filepath, TIXML_ENCODING_UTF8))
            return false;
        // get name
        std::string name = msg->GetTypeName();
        size_t pos = name.find_last_of(".");
        if (pos != std::string::npos) {
            name = name.substr(pos + 1);
        }
        return var2pb(xml_doc.FirstChild(name.c_str()), msg);
    }
    catch (const std::exception& cre) {
        return false;
    }
    catch (...) {
        return false;
    }
}

}   // end namespace tinytable
