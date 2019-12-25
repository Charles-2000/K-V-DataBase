#include <iostream>
#include <fstream>
#include "KVDB.h"
using namespace kvdb;
using namespace std;


KVDBHandler::KVDBHandler(const std::string& db_file)
{
	file.open(db_file.c_str(), ios::out | ios::binary | ios::app); //If file doesn't exist, create file.
	if (this->file.fail())  //if fail, throws exception and exits program
	{
		throw "Create file \"" + db_file + "\" error.\nThere's no space left on devices!";
		exit(KVDB_NO_SPACE_LEFT_ON_DEVICES);
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
	if (key.length() == 0 || key.length() > MAX_SIZE)
		return KVDB_INVALID_KEY;

	fstream& f = handler->get_db_file();
	f.seekg(0, ios::end);	//locate the pointer to the end of file

	//set data messages
	int key_length = key.length();
	int value_length = value.length();
	string _key = key;
	string _value = value;

	f.write(reinterpret_cast<char*>(&key_length), sizeof(int));
	f.write(reinterpret_cast<char*>(&value_length), sizeof(int));
	f.write(_key.c_str(), key_length * sizeof(char));
	f.write(_value.c_str(), value_length * sizeof(char));

	return KVDB_OK;
}

int kvdb::get(KVDBHandler* handler, const std::string& key, std::string& value)
{
	if (key.length() == 0 || key.length() > MAX_SIZE)
		return KVDB_INVALID_KEY;

	fstream& f = handler->get_db_file();
	f.seekg(0, ios::beg);	//locate the pointer to the begin of file

	int keyl, valuel;
	string _key;

	while (f.peek() != EOF)
	{
		f.read(reinterpret_cast<char*>(&keyl), sizeof(int));
		f.read(reinterpret_cast<char*>(&valuel), sizeof(int));

		_key.resize(keyl);
		f.read(&_key[0], keyl * sizeof(char));

		if (_key == key)	//if found
		{
			//skip deleted pair
			if (valuel == 0)
			{
				value.clear(); //update the value to none
				continue;
			}

			//set value
			value.resize(valuel);
			f.read(&value[0], valuel * sizeof(char));
		}
		else if (valuel != 0)
			f.seekg(valuel, ios::cur);	//skip value
	}

	if (!value.empty())
		return KVDB_OK;
	return KVDB_INVALID_KEY;
}

int kvdb::del(KVDBHandler* handler, const std::string& key)
{
	if (key.length() == 0 || key.length() > MAX_SIZE)
		return KVDB_INVALID_KEY;

	fstream& f = handler->get_db_file();
	f.seekg(0, ios::beg);	//locate the pointer to the end of file

	int keyl, valuel;
	string _key;
	int FOUND = 0;

	while (f.peek() != EOF)
	{
		f.read(reinterpret_cast<char*>(&keyl), sizeof(int));
		f.read(reinterpret_cast<char*>(&valuel), sizeof(int));

		_key.resize(keyl);
		f.read(&_key[0], keyl * sizeof(char));

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
	int key_length = key.length();
	int value_length = 0;
	_key = key;

	f.seekg(0, ios::end);
	f.write(reinterpret_cast<char*>(&key_length), sizeof(int));
	f.write(reinterpret_cast<char*>(&value_length), sizeof(int));
	f.write(_key.c_str(), key_length * sizeof(char));

	return KVDB_OK;
}

int kvdb::purge(KVDBHandler* handler)
{


	return 0;
}
