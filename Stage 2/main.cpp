#include<iostream>
#include<Windows.h>
#include<time.h>
#include "KVDB.h"

int main(int, char**) {
	std::string value;
	kvdb::KVDBHandler kv("db_path");

	kvdb::set(&kv, "asdf", "123");
	kvdb::expires(&kv, "asdf", 5);
	time_t t = time(NULL);
	std::cout << "Cur:  " << t << std::endl;
	if (kvdb::KVDB_OK == kvdb::get(&kv, "asdf", value)) {
		std::cout << value << std::endl;
	}
	else
		std::cout << "Not Found!" << std::endl;

	Sleep(6000);
	t = time(NULL);
	std::cout << "Cur:  " << t << std::endl;
	if (kvdb::KVDB_OK == kvdb::get(&kv, "asdf", value)) {
		std::cout << value << std::endl;
	}
	else
		std::cout << "Not Found!" << std::endl;


	return 0;
}
