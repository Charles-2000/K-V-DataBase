#include<iostream>
#include<Windows.h>
#include<time.h>
#include "KVDB.h"

int main(int, char**) {
	std::string value;
	kvdb::KVDBHandler kv("db_path");

	//test 1
	kvdb::set(&kv, "asdf", "123");
	kvdb::expires(&kv, "asdf", 5);
	time_t t = time(NULL);
	std::cout << "Cur:  " << t << std::endl;
	if (kvdb::KVDB_OK == kvdb::get(&kv, "asdf", value)) {
		std::cout << value << std::endl;
	}else
		std::cout << "Not Found!" << std::endl;

	Sleep(6000);

	if (kvdb::KVDB_OK == kvdb::get(&kv, "asdf", value)) {
		std::cout << value << std::endl;
	}else
		std::cout << "Not Found!" << std::endl;
	std::cout << "------------------------" << std::endl;


	//test 2
	//set expired time for a nonexistent key
	kvdb::expires(&kv, "asd", 10);
	if (kvdb::KVDB_OK == kvdb::get(&kv, "asd", value)) {
		std::cout << value << std::endl;
	}else
		std::cout << "Not Found!" << std::endl;
	std::cout << "------------------------" << std::endl;


	//test 3
	//set expired time(5s) for a key 
	//reset the key
	//get key at 6s
	kvdb::set(&kv, "asdf", "123");
	kvdb::expires(&kv, "asdf", 5);

	Sleep(3000);
	kvdb::set(&kv, "asdf", "456");
	Sleep(3000);

	if (kvdb::KVDB_OK == kvdb::get(&kv, "asdf", value)) {
		std::cout << value << std::endl;
	}else
		std::cout << "Not Found!" << std::endl;


	//test 4
	//test for purge
	kvdb::set(&kv, "key", "value");
	kvdb::expires(&kv, "key", 5000);

	if(kvdb::purge(&kv) == kvdb::KVDB_OK)
		std::cout << "Purge OK" << std::endl;

	if (kvdb::KVDB_OK == kvdb::get(&kv, "key", value)) {
		std::cout << value << std::endl;
	}else
		std::cout << "Not Found!" << std::endl;


	return 0;
}
