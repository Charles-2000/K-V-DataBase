#include <iostream>
#include <fstream>
#include <stdint.h>
#include "KVDB.h"
using namespace kvdb;
using namespace std;

char tmp[100000];

KVDBHandler::KVDBHandler(const std::string& db_file)
{
	file.open(db_file.c_str(), ios::out | ios::binary | ios::app); //If file doesn't exist, create file.
	RUNTIME_MESSAGE = KVDB_OK;
	if (file.fail())
	{
		RUNTIME_MESSAGE = KVDB_INVALID_AOF_PATH;
	}
	else
	{
		file_path = db_file;
		file.close();
		file.open(file_path.c_str(), ios::binary | ios::in | ios::out | ios::app);
	}
}

KVDBHandler::~KVDBHandler()
{
	if (file)
		file.close();
}

string KVDBHandler::getFilePath()
{
	return file_path;
}

fstream& KVDBHandler::get_db_file()
{
	return this->file;
}

int kvdb::set(KVDBHandler* handler, const std::string& key, const std::string& value)
{
	if (key.length() == 0)
		return KVDB_INVALID_KEY;

	fstream &f = handler->get_db_file();
	f.seekg(0, ios::end);	//locate the pointer to the end of file

	//set data messages
	uint32_t key_length = key.length();
	uint32_t value_length = value.length();
	string _key = key;
	string _value = value;

	f.write(reinterpret_cast<char*>(&key_length), sizeof(uint32_t));
	f.write(reinterpret_cast<char*>(&value_length), sizeof(uint32_t));
	f.write(_key.c_str(), key_length * sizeof(char));
	f.write(_value.c_str(), value_length * sizeof(char));

	return KVDB_OK;
}

int kvdb::get(KVDBHandler* handler, const std::string& key, std::string& value)
{
	if (key.length() == 0)
		return KVDB_INVALID_KEY;

	fstream& f = handler->get_db_file();
	f.seekg(0, ios::beg);	//locate the pointer to the begin of file

	uint32_t keyl, valuel;
	string _key, _value = "";
	
	while (f.peek() != EOF)
	{
		f.read(reinterpret_cast<char*>(&keyl), sizeof(uint32_t));
		f.read(reinterpret_cast<char*>(&valuel), sizeof(uint32_t));
		f.read(tmp, keyl * sizeof(char));
		_key = tmp;
		_key = _key.substr(0, keyl);
		
		if (_key == key)	//if found
		{
			//skip deleted pair
			if (valuel == 0)
			{
				_value = ""; //update the value to none
				continue;
			}

			//set value
			f.read(tmp, valuel * sizeof(char));
			_value = tmp;
			_value = _value.substr(0, valuel);
		}
		else if(valuel != 0)
			f.seekg(valuel, ios::cur);	//skip value
	}

	if (!_value.empty())
	{
		value = _value;
		return KVDB_OK;
	}
	return KVDB_INVALID_KEY;
}

int kvdb::del(KVDBHandler* handler, const std::string& key)
{
	if (key.length() == 0)
		return KVDB_INVALID_KEY;

	fstream& f = handler->get_db_file();
	f.seekg(0, ios::beg);	//locate the pointer to the end of file

	uint32_t keyl, valuel;
	string _key;
	int FOUND = 0;

	while (f.peek() != EOF)
	{
		f.read(reinterpret_cast<char*>(&keyl), sizeof(uint32_t));
		f.read(reinterpret_cast<char*>(&valuel), sizeof(uint32_t));
		f.read(tmp, keyl * sizeof(char));
		_key = tmp;
		_key = _key.substr(0, keyl);

		if (_key == key)	//if found
		{
			//if is deleted key
			if (valuel == 0)
			{
				FOUND = 0;
				continue;
			}
			
			FOUND = 1;

		}
		f.seekg(valuel, ios::cur);	//skip value
	}

	if (!FOUND)
		return KVDB_INVALID_KEY;

	//If found, set data messages.
	uint32_t key_length = key.length();
	uint32_t value_length = 0;
	_key = key;

	f.seekg(0, ios::end);
	f.write(reinterpret_cast<char*>(&key_length), sizeof(uint32_t));
	f.write(reinterpret_cast<char*>(&value_length), sizeof(uint32_t));
	f.write(_key.c_str(), key_length * sizeof(char));

	return KVDB_OK;
}

int kvdb::purge(KVDBHandler* handler)
{


	return 0;
}
