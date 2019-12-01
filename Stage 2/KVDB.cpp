#include <iostream>
#include <fstream>
#include <stdint.h>
#include <unordered_map>
#include "KVDB.h"
using namespace kvdb;
using namespace std;


KVDBHandler::KVDBHandler(const std::string& db_file)
{
	file.open(db_file.c_str(), ios::out | ios::binary | ios::app); //If file doesn't exist, create file.
	if (file.fail())
		exit(KVDB_NO_SPACE_LEFT_ON_DEVICES);
	else
	{
		file_path = db_file;
		file.close();
		file.open(file_path.c_str(), ios::binary | ios::in | ios::out | ios::app);
		this->createAOFIndex();
	}
}

KVDBHandler::~KVDBHandler()
{
	if (file)
		file.close();
	AOF_index.clear();
}

string KVDBHandler::getFilePath()
{
	return file_path;
}

fstream& KVDBHandler::get_db_file()
{
	return this->file;
}

int KVDBHandler::createAOFIndex()
{
	fstream& f = this->get_db_file();
	f.seekg(0, ios::beg);

	uint32_t keyl, valuel;
	string _key;

	while (f.peek() != EOF)
	{
		int pos = f.tellg();  //records the offset of a new K-V pair

		f.read(reinterpret_cast<char*>(&keyl), sizeof(uint32_t));
		f.read(reinterpret_cast<char*>(&valuel), sizeof(uint32_t));

		char* tmp = new char[keyl + 1];
		f.read(tmp, keyl * sizeof(char));
		tmp[keyl] = '\0';
		_key = tmp;
		delete[]tmp;

		//if value exists, update position; else delete key
		if (valuel != 0)
			AOF_index[_key] = pos;
		else
			AOF_index.erase(_key);

		f.seekg(valuel, ios::cur);  //skips value
	}

	/*//for test
	unordered_map<string, int>::iterator it;
	for (it = AOF_index.begin(); it != AOF_index.end(); it++)
		cout << it->first << " : " << it->second << endl; */

	return KVDB_OK;
}

/*unordered_map<string, int> KVDBHandler::getAOFIndex()
{
	return this->AOF_index;
}*/

void KVDBHandler::setOffset(const std::string key, const int offset)
{
	this->AOF_index[key] = offset;
}

int KVDBHandler::getOffset(const std::string key)
{
	unordered_map<string, int>::iterator it = AOF_index.find(key);
	if (it != AOF_index.end())
		return AOF_index[key];
	return -1;
}

void KVDBHandler::deleteOffset(const std::string key)
{
	this->AOF_index.erase(key);
}

int kvdb::set(KVDBHandler* handler, const std::string& key, const std::string& value)
{
	if (key.length() == 0 || key.length() > MAX_SIZE)
		return KVDB_INVALID_KEY;
	
	fstream &f = handler->get_db_file();
	f.seekg(0, ios::end);	//locate the pointer to the end of file
	int pos = f.tellg();  //records current position
	
	//set data messages
	uint32_t key_length = key.length();
	uint32_t value_length = value.length();
	string _key = key;
	string _value = value;

	f.write(reinterpret_cast<char*>(&key_length), sizeof(uint32_t));
	f.write(reinterpret_cast<char*>(&value_length), sizeof(uint32_t));
	f.write(_key.c_str(), key_length * sizeof(char));
	f.write(_value.c_str(), value_length * sizeof(char));

	handler->setOffset(_key, pos);

	return KVDB_OK;
}

int kvdb::get(KVDBHandler* handler, const std::string& key, std::string& value)
{
	if (key.length() == 0 || key.length() > MAX_SIZE)
		return KVDB_INVALID_KEY;

	int pos = handler->getOffset(key);
	if(pos == -1)  //key doesn't exist
		return KVDB_INVALID_KEY;

	fstream& f = handler->get_db_file();
	f.seekg(pos, ios::beg);	//locate the pointer to the begin of file

	uint32_t keyl, valuel;
	string _key;
	
	f.read(reinterpret_cast<char*>(&keyl), sizeof(uint32_t));
	f.read(reinterpret_cast<char*>(&valuel), sizeof(uint32_t));

	char* tmp = new char[keyl+1];
	f.read(tmp, keyl * sizeof(char));
	tmp[keyl] = '\0';
	_key = tmp;
	delete[]tmp;

	//set value
	value.resize(valuel);
	f.read(&value[0], valuel * sizeof(char));

	return KVDB_OK;
}

int kvdb::del(KVDBHandler* handler, const std::string& key)
{
	if (key.length() == 0 || key.length() > MAX_SIZE)
		return KVDB_INVALID_KEY;

	int pos = handler->getOffset(key);
	if (pos == -1)  //key doesn't exist
		return KVDB_INVALID_KEY;

	//If key exists, set data messages.
	uint32_t key_length = key.length();
	uint32_t value_length = 0;
	string _key = key;

	fstream& f = handler->get_db_file();
	f.seekg(0, ios::end);
	f.write(reinterpret_cast<char*>(&key_length), sizeof(uint32_t));
	f.write(reinterpret_cast<char*>(&value_length), sizeof(uint32_t));
	f.write(_key.c_str(), key_length * sizeof(char));

	handler->deleteOffset(_key);

	return KVDB_OK;
}

int kvdb::purge(KVDBHandler* handler)
{
	//create temporary file
	string tmp_path = "tmp.txt";
	fstream tmp_file(tmp_path.c_str(), ios::out | ios::binary);
	if (tmp_file.fail())
		return KVDB_NO_SPACE_LEFT_ON_DEVICES;

	//handler->createAOFIndex();
	//unordered_map<string, int> Index = handler->getAOFIndex();
	fstream& f = handler->get_db_file();
	kvdb::KVDBHandler tmp_kv(tmp_path);

	unordered_map<string, int>::iterator it;
	for (it = handler->AOF_index.begin(); it != handler->AOF_index.end(); it++)
	{
		string key = it->first;
		string value;
		if (get(handler, key, value) == KVDB_OK) 
		{
			set(&tmp_kv, key, value);
		}
	}

	//update the new Append-Only file and delete the old one
	string oldpath = handler->getFilePath();
	rename(oldpath.c_str(), tmp_path.c_str());
	remove(oldpath.c_str());

	return KVDB_OK;
}
