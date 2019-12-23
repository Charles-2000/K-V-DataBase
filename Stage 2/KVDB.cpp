#include <iostream>
#include <fstream>
#include <stdint.h>
#include <time.h>
#include <unordered_map>
#include "KVDB.h"
using namespace kvdb;
using namespace std;

//overload operator '>' to create min-heap
inline bool kvdb::operator>(const TimeStamp& t1, const TimeStamp& t2)
{
	return t1.time > t2.time;
}


KVDBHandler::KVDBHandler(const std::string& db_file)
{
	file.open(db_file.c_str(), ios::out | ios::binary | ios::app); //If file doesn't exist, create file.
	if (file.fail())  //if fail, throws exception and exits program
	{
		throw "Create file \"" + db_file + "\"\nThere's no space left on devices!";
		exit(KVDB_NO_SPACE_LEFT_ON_DEVICES);
	}	
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
	this->AOF_index.clear();
}

const string KVDBHandler::getFilePath()
{
	return file_path;
}

fstream* KVDBHandler::get_db_file()
{
	return &this->file;
}

void KVDBHandler::openFile()
{
	if(!file)
		file.open(file_path.c_str(), ios::binary | ios::in | ios::out | ios::app);
}

void KVDBHandler::closeFile()
{
	if (file)
		file.close();
}

int KVDBHandler::createAOFIndex()
{
	fstream* f = this->get_db_file();
	f->seekg(0, ios::beg);

	int keyl, valuel;
	string key;

	while (f->peek() != EOF)
	{
		int pos = f->tellg();  //records the offset of a new K-V pair

		f->read(reinterpret_cast<char*>(&keyl), sizeof(int));
		f->read(reinterpret_cast<char*>(&valuel), sizeof(int));

		key.resize(keyl);
		f->read(&key[0], keyl * sizeof(char));

		if (valuel > 0) //if value exists, update index
		{
			Index index(pos);
			this->AOF_index[key] = index;
			f->seekg(valuel, ios::cur);  //skips value
		}
		else if (valuel == KVDB_VL_DELETE)
			this->AOF_index.erase(key);
		else if (valuel == KVDB_VL_EXPIRES)
		{
			unsigned int time;
			f->read(reinterpret_cast<char*>(&time), sizeof(unsigned int));
			this->setExpiredTime(key, time);
		}	
	}

	/*//for test
	unordered_map<string, int>::iterator it;
	for (it = AOF_index.begin(); it != AOF_index.end(); it++)
		cout << it->first << " : " << it->second << endl; */

	return KVDB_OK;
}

unordered_map<string, Index>* KVDBHandler::getAOFIndex()
{
	return &this->AOF_index;
}

void KVDBHandler::setOffset(const std::string key, int pos)
{
	this->AOF_index[key] = Index(pos);
}

int KVDBHandler::getOffset(const std::string key)
{
	unordered_map<string, Index>::iterator it = this->AOF_index.find(key);
	if (it != this->AOF_index.end())
	{
		//cout << "End:  " << it->second.time << endl;
		return it->second.offset;
	}
		
	return -1;
}

void KVDBHandler::deleteIndex(const std::string key)
{
	this->AOF_index.erase(key);
}

void kvdb::KVDBHandler::setExpiredTime(const std::string& key, int time)
{
	int pos = this->getOffset(key);
	AOF_index[key] = Index(pos, time);
	AOF_time.push(TimeStamp(key, time));
}

void kvdb::KVDBHandler::update()
{
	time_t curTime;
	unsigned int curtime, endtime;
	TimeStamp t;
	
	while (!this->AOF_time.empty()) 
	{
		curTime = time(NULL);
		curtime = curTime;
		t = this->AOF_time.top();

		if (t.time != 0 && t.time <= curtime) //if already expired
		{
			this->AOF_time.pop();

			int pos = this->getOffset(t.key);
			if (pos == -1)  //key doesn't exist
				continue;

			// if t.key exists
			endtime = this->AOF_index[t.key].time;
			if (endtime != 0 && endtime < curtime) //if already expired
				del(this, t.key);
		}
		else
			break;	
	}
}

int kvdb::set(KVDBHandler* handler, const std::string& key, const std::string& value)
{
	if (key.length() == 0 || key.length() > MAX_SIZE)
		return KVDB_INVALID_KEY;
	
	fstream* f = handler->get_db_file();
	f->seekg(0, ios::end);	//locate the pointer to the end of file
	int pos = f->tellg();  //records current position
	
	//set data messages
	int key_length = key.length();
	int value_length = value.length();
	string _key = key;
	string _value = value;

	f->write(reinterpret_cast<char*>(&key_length), sizeof(int));
	f->write(reinterpret_cast<char*>(&value_length), sizeof(int));
	f->write(_key.c_str(), key_length * sizeof(char));
	f->write(_value.c_str(), value_length * sizeof(char));

	handler->setOffset(_key, pos);

	return KVDB_OK;
}

int kvdb::get(KVDBHandler* handler, const std::string& key, std::string& value)
{
	handler->update();

	if (key.length() == 0 || key.length() > MAX_SIZE)
		return KVDB_INVALID_KEY;

	int pos = handler->getOffset(key);
	if(pos == -1)  //key doesn't exist
		return KVDB_INVALID_KEY;

	fstream* f = handler->get_db_file();
	f->seekg(pos, ios::beg);	//locate the pointer to the begin of file

	int keyl, valuel;
	
	f->read(reinterpret_cast<char*>(&keyl), sizeof(int));
	f->read(reinterpret_cast<char*>(&valuel), sizeof(int));

	f->seekg(keyl, ios::cur);

	//set value
	value.resize(valuel);
	f->read(&value[0], valuel * sizeof(char));

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
	int key_length = key.length();
	int value_length = KVDB_VL_DELETE;

	fstream* f = handler->get_db_file();
	f->seekg(0, ios::end);
	f->write(reinterpret_cast<char*>(&key_length), sizeof(int));
	f->write(reinterpret_cast<char*>(&value_length), sizeof(int));
	f->write(key.c_str(), key_length * sizeof(char));

	handler->deleteIndex(key);

	return KVDB_OK;
}

int kvdb::purge(KVDBHandler* handler)
{
	//create temporary file
	string tmp_path = "tmp";

	//determine whether file "tmp" exists
	fstream tmp_file(tmp_path.c_str(), ios::in);
	if (!tmp_file.fail())  //if exists, remove file "tmp"
		remove(tmp_path.c_str());

	kvdb::KVDBHandler tmp_kv(tmp_path);
	fstream* f = tmp_kv.get_db_file();

	handler->update();

	unordered_map<string, Index>* index = handler->getAOFIndex();
	unordered_map<string, Index>::iterator it;
	for (it = index->begin(); it != index->end(); it++)
	{
		string key = it->first;
		string value;
		unsigned int _time = it->second.time;

		get(handler, key, value);
		set(&tmp_kv, key, value);

		if (_time == 0)
			continue;

		//write expired time
		int key_length = key.length();
		int value_length = KVDB_VL_EXPIRES;
		f->write(reinterpret_cast<char*>(&key_length), sizeof(int));
		f->write(reinterpret_cast<char*>(&value_length), sizeof(int));
		f->write(key.c_str(), key_length * sizeof(char));
		f->write(reinterpret_cast<char*>(&_time), sizeof(unsigned int));
		/*time_t t = time(NULL);
		expires(&tmp_kv, key, _time - t);*/
	}

	//update the new Append-Only file and delete the old one
	string oldpath = handler->getFilePath();
	handler->closeFile();
	tmp_kv.closeFile();
	remove(oldpath.c_str());
	rename(tmp_path.c_str(), oldpath.c_str());
	
	return KVDB_OK;
}

int kvdb::expires(KVDBHandler* handler, const std::string& key, int n)
{
	if (key.length() == 0 || key.length() > MAX_SIZE)
		return KVDB_INVALID_KEY;

	int pos = handler->getOffset(key);
	if (pos == -1)  //key doesn't exist
		return KVDB_INVALID_KEY;

	fstream* f = handler->get_db_file();
	f->seekg(0, ios::end);	//locate the pointer to the end of file

	int key_length = key.length();
	int value_length = KVDB_VL_EXPIRES;
	string _key = key;

	f->write(reinterpret_cast<char*>(&key_length), sizeof(int));
	f->write(reinterpret_cast<char*>(&value_length), sizeof(int));
	f->write(_key.c_str(), key_length * sizeof(char));

	time_t t = time(NULL);  //get current time 
	unsigned int _time = t;
	_time += n;
	f->write(reinterpret_cast<char*>(&_time), sizeof(unsigned int));

	handler->setExpiredTime(key, _time);

	return 0;
}

