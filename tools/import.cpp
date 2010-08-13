// import.cpp

/**
*    Copyright (C) 2008 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "pch.h"
#include "client/dbclient.h"
#include "db/json.h"

#include "tool.h"
#include "../util/text.h"
#include "../util/httpclient.h"
#include "../bson/bsontypes.h"

#include <vector>
#include <fstream>
#include <iostream>
#include <algorithm>

#include <boost/program_options.hpp>

using namespace mongo;

namespace po = boost::program_options;

struct element_to_object {
	vector<BSONObj> *_container;
	
	element_to_object( vector<BSONObj> *container )
	: _container( container )
	{}
	
	void operator()(BSONElement e){
		_container->push_back(e.Obj());
	}
};

class Import : public Tool {
    
    enum Type { JSON , CSV , TSV };
    Type _type;

    const char * _sep;
    bool _ignoreBlanks;
    bool _headerLine;
    bool _upsert;
    bool _doimport;
    bool _jsonArray;
	string _root;
    vector<string> _upsertFields;
    
    void _append( BSONObjBuilder& b , const string& fieldName , const string& data ){
        if ( b.appendAsNumber( fieldName , data ) )
            return;
        
        if ( _ignoreBlanks && data.size() == 0 )
            return;

        // TODO: other types?
        b.append( fieldName , data );
    }
    
    void parseLine( char * line, vector<BSONObj> *objects ){
        uassert(13289, "Invalid UTF8 character detected", isValidUTF8(line));

        if ( _type == JSON ){
            char * end = ( line + strlen( line ) ) - 1;
            while ( isspace(*end) ){
                *end = 0;
                end--;
            }
			BSONObj bson = fromjson( line );
			if ( _root.size() > 0 ) {
				BSONElement e = bson.getFieldDotted( _root.c_str() );
				if ( e.eoo() )
					cerr << "json root element is not available in json data" << endl;
				else {
					switch ( e.type() ) {
						case Object:
							objects->push_back( e.Obj() );
							break;
						case Array:{
							vector<BSONElement> elements = e.Array();
							std::for_each( elements.begin(), elements.end(), element_to_object(objects) );
							break;
						}
						default:
							cerr << "json root element is not an object or array" << endl;
							break;
					}
				}
			} else {
				objects->push_back(bson);
			}
            return;
        }
        
        BSONObjBuilder b;

        unsigned int pos=0;
        while ( line[0] ){
            string name;
            if ( pos < _fields.size() ){
                name = _fields[pos];
            }
            else {
                stringstream ss;
                ss << "field" << pos;
                name = ss.str();
            }
            pos++;
            
            bool done = false;
            string data;
            char * end;
            if ( _type == CSV && line[0] == '"' ){
                line++; //skip first '"'

                while (true) {
                    end = strchr( line , '"' );
                    if (!end){
                        data += line;
                        done = true;
                        break;
                    } else if (end[1] == '"') {
                        // two '"'s get appended as one
                        data.append(line, end-line+1); //include '"'
                        line = end+2; //skip both '"'s
                    } else if (end[-1] == '\\') {
                        // "\\\"" gets appended as '"'
                        data.append(line, end-line-1); //exclude '\\'
                        data.append("\"");
                        line = end+1; //skip the '"'
                    } else {
                        data.append(line, end-line);
                        line = end+2; //skip '"' and ','
                        break;
                    }
                }
            } else {
                end = strstr( line , _sep );
                if ( ! end ){
                    done = true;
                    data = string( line );
                } else {
                    data = string( line , end - line );
                    line = end+1;
                }
            }

            if ( _headerLine ){
                while ( isspace( data[0] ) )
                    data = data.substr( 1 );
                _fields.push_back( data );
            }
            else
                _append( b , name , data );
            
            if ( done )
                break;
        }
		objects->push_back(b.obj());
    }
    
public:
    Import() : Tool( "import" ){
        addFieldOptions();
        add_options()
            ("ignoreBlanks","if given, empty fields in csv and tsv will be ignored")
            ("type",po::value<string>() , "type of file to import.  default: json (json,csv,tsv)")
            ("file",po::value<string>() , "file to import from; if this and url not specified stdin is used" )
            ("url",po::value<string>() , "url to import from; if this and file not specified stdin is used" )
            ("drop", "drop collection first " )
            ("headerline","CSV,TSV only - use first line as headers")
            ("jsonRoot",po::value<string>(),"JSON only - take data from within this root (useful with url)" )
            ("upsert", "insert or update objects that already exist" )
            ("upsertFields", po::value<string>(), "comma-separated fields for the query part of the upsert. You should make sure this is indexed" )
            ("stopOnError", "stop importing at first error rather than continuing" )
            ("jsonArray", "load a json array, not one item per line. Currently limited to 4MB." )
            ;
        add_hidden_options()
            ("noimport", "don't actually import. useful for benchmarking parser" )
            ;
        addPositionArg( "file" , 1 );
        _type = JSON;
        _ignoreBlanks = false;
        _headerLine = false;
        _upsert = false;
        _doimport = true;
        _jsonArray = false;
    }
    
    int run(){
        string filename = getParam( "file" );
		string url = getParam( "url" );
        long long fileSize = -1;

        istream * in = &cin;

		if ( url.size() > 0 && filename.size() > 0 ){
			cerr << "both url and file cannot be specified" << endl;
			return -1;
		}
		
		_root = getParam( "jsonRoot" );
		
		stringstream iss;
		ifstream file( filename.c_str() , ios_base::in );
		
		if ( url.size() > 0 ){
			HttpClient c;
			HttpClient::Result r;
			int rc = c.get( url , &r );
			log(1) << "url fetch response code: " << rc << endl;
			if ( rc != 200 ){
				log() << "url fetch error response code:" << rc << endl;
				log(1) << "url fetch error body:" << r.getEntireResponse() << endl;
				return -1;
			}
			iss << r.getBody();
			in = &iss;
			fileSize = r.getBody().size();
		}
		else if ( filename.size() > 0 && filename != "-" ){
            if ( ! exists( filename ) ){
                cerr << "file doesn't exist: " << filename << endl;
                return -1;
            }
            in = &file;
            fileSize = file_size( filename );
        }

        string ns;

        try {
            ns = getNS();
        } catch (...) {
            printHelp(cerr);
            return -1;
        }
        
        log(1) << "ns: " << ns << endl;
        
        auth();

        if ( hasParam( "drop" ) ){
            cout << "dropping: " << ns << endl;
            conn().dropCollection( ns.c_str() );
        }

        if ( hasParam( "ignoreBlanks" ) ){
            _ignoreBlanks = true;
        }

        if ( hasParam( "upsert" ) ){
            _upsert = true;

            string uf = getParam("upsertFields");
            if (uf.empty()){
                _upsertFields.push_back("_id");
            } else {
                StringSplitter(uf.c_str(), ",").split(_upsertFields);
            }
        }

        if ( hasParam( "noimport" ) ){
            _doimport = false;
        }

        if ( hasParam( "type" ) ){
            string type = getParam( "type" );
            if ( type == "json" )
                _type = JSON;
            else if ( type == "csv" ){
                _type = CSV;
                _sep = ",";
            }
            else if ( type == "tsv" ){
                _type = TSV;
                _sep = "\t";
            }
            else {
                cerr << "don't know what type [" << type << "] is" << endl;
                return -1;
            }
        }
        
        if ( _type == CSV || _type == TSV ){
            _headerLine = hasParam( "headerline" );
            if ( ! _headerLine )
                needFields();
        }

        if (_type == JSON && hasParam("jsonArray")){
            _jsonArray = true;
        }

        int errors = 0;
        
        int num = 0;
        
        time_t start = time(0);

        log(1) << "filesize: " << fileSize << endl;
        ProgressMeter pm( fileSize );
        const int BUF_SIZE = 1024 * 1024 * 4;
        boost::scoped_array<char> line(new char[BUF_SIZE+2]);
        char * buf = line.get();
        while ( _jsonArray || in->rdstate() == 0 ){
            if (_jsonArray){
                if (buf == line.get()){ //first pass
                    in->read(buf, BUF_SIZE);
                    uassert(13295, "JSONArray file too large", (in->rdstate() & ios_base::eofbit));
                    buf[ in->gcount() ] = '\0';
                }
            } else {
                buf = line.get();
                in->getline( buf , BUF_SIZE );
                log(1) << "got line:" << buf << endl;
            }
            uassert( 10263 ,  "unknown error reading file" ,
                    (!(in->rdstate() & ios_base::badbit)) &&
                    (!(in->rdstate() & ios_base::failbit) || (in->rdstate() & ios_base::eofbit)) );

            int len = 0;
            if (strncmp("\xEF\xBB\xBF", buf, 3) == 0){ // UTF-8 BOM (notepad is stupid)
                buf += 3;
                len += 3;
            }

            if (_jsonArray){
                while (buf[0] != '{' && buf[0] != '\0') {
                    len++;
                    buf++;
                }
                if (buf[0] == '\0')
                    break;
            } else {
                while (isspace( buf[0] )){
                    len++;
                    buf++;
                }
                if (buf[0] == '\0')
                    continue;
                len += strlen( buf );
            }

            try {
				vector<BSONObj> objects;
				
                if (_jsonArray){
                    int jslen;
					objects.push_back( fromjson(buf, &jslen) );
                    len += jslen;
                    buf += jslen;
                } else {
                    parseLine( buf, &objects );
                }

                if ( _headerLine ){
                    _headerLine = false;
                } else if (_doimport) {
					for (vector<BSONObj>::const_iterator b_it=objects.begin(), end=objects.end(); b_it!=end; ++b_it){
						bool doUpsert = _upsert;
						BSONObjBuilder b;
						if (_upsert){
							for (vector<string>::const_iterator it=_upsertFields.begin(), end=_upsertFields.end(); it!=end; ++it){
								BSONElement e = b_it->getFieldDotted(it->c_str());
								if (e.eoo()){
									doUpsert = false;
									break;
								}
								b.appendAs(e, *it);
							}
						}

						if (doUpsert){
							conn().update(ns, Query(b.obj()), *b_it, true);
						} else {
							conn().insert( ns.c_str() , *b_it );
						}
						++num;
					}
                }
            }
            catch ( std::exception& e ){
                cout << "exception:" << e.what() << endl;
                cout << buf << endl;
                errors++;
                
                if (hasParam("stopOnError") || _jsonArray)
                    break;
            }

            if ( pm.hit( len + 1 ) ){
                cout << "\t\t\t" << num << "\t" << ( num / ( time(0) - start ) ) << "/second" << endl;
            }
        }

        cout << "imported " << num << " objects" << endl;

        conn().getLastError();
        
        if ( errors == 0 )
            return 0;
        
        cerr << "encountered " << errors << " error" << ( errors == 1 ? "" : "s" ) << endl;
        return -1;
    }
};

int main( int argc , char ** argv ) {
    Import import;
    return import.main( argc , argv );
}
