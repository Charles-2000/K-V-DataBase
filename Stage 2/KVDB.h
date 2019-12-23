/*
 * Course Project for Data-Structure
 * Simple File Based K-V Database
 */

#include <exception>
#include <string>
#include <vector>
#include <fstream>
#include <queue>
#include <unordered_map>

namespace kvdb {
    // Def of return code
    // OK
    const int KVDB_OK = 0;
    // Invalid path of append-only file
    const int KVDB_INVALID_AOF_PATH = 1;
    // Invalid KEY
    const int KVDB_INVALID_KEY = 2;
    // No space on devices for purging.
    const int KVDB_NO_SPACE_LEFT_ON_DEVICES = 3;
    // The max size of key
	const int MAX_SIZE = 10000;

	// delete the key
	const int KVDB_VL_DELETE = 0;
	// is a timestamp
	const int KVDB_VL_EXPIRES = -1;

	//struct of key's time stamp
	struct TimeStamp
	{
		std::string key;
		unsigned int time;

		//if not exists expire time, initialize as 0
		TimeStamp(){}
		TimeStamp(std::string _key) :key(_key) { time = 0; }
		TimeStamp(std::string _key, unsigned int _time) :key(_key), time(_time) {}
		friend bool operator > (const TimeStamp& t1, const TimeStamp& t2);
	};

	//the value of AOF_index
	struct Index
	{
		int offset;
		unsigned int time;

		Index(){}
		Index(int _offset) :offset(_offset) { time = 0; }
		Index(int _offset, unsigned int _time) :offset(_offset), time(_time) {}
	};

    // File Handler for KVDB
    class KVDBHandler {
	private:
		std::string file_path;
		std::fstream file; 	

		//Records the offset of keys.
		std::unordered_map<std::string, Index> AOF_index;

		//Record each key's time stamp
		std::priority_queue<TimeStamp, std::vector<TimeStamp>, std::greater<TimeStamp> > AOF_time;

    public:

		//create AOF_index
		int createAOFIndex();

		//get AOF_index
		std::unordered_map<std::string, Index>* getAOFIndex();

		//set key-offset 
		void setOffset(const std::string key, int pos);
		//get the offset of key
		int getOffset(const std::string key);
		//delete key-Index pair
		void deleteIndex(const std::string key);

		//set expired time
		void setExpiredTime(const std::string& key, int time);

		//delete expired keys
		void update();

        // Constructor, creates DB handler
        // @param db_file {const std::string&} path of the append-only file for database.
        KVDBHandler(const std::string& db_file);

		//get the path of database.
		const std::string getFilePath();
		//get the file of database.
		std::fstream* get_db_file();

		//open AOF
		void openFile();
		//close AOF
		void closeFile();

        // Closes DB handler
        ~KVDBHandler();
    };

    // Set the string value of a key
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key of a string
    // @param value {const std::string&} the value of a string
    // @return {int} return code
    int set(KVDBHandler* handler, const std::string& key, const std::string& value);

    // Get the value of a key
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key of a string
    // @param value {std::string&} store the value of a key after GET executed correctly
    // @return {int} return code    
    int get(KVDBHandler* handler, const std::string& key, std::string* value);

    // Delete a key
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key to be deleted
    // @return {int} return code
    int del(KVDBHandler* handler, const std::string& key);

	// Purge the append-only file for database.
	// @param handler {KVDBHandler*} the handler of KVDB
	// @return {int} return code
	int purge(KVDBHandler* handler);

    // Set a key's time to live in seconds
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param n {int} life cycle in seconds
    // @return {int} return code
    int expires(KVDBHandler* handler, const std::string& key, int n);

    // Set a member from a Set or SortedSet's time to live in seconds
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param n {int} life cycle in seconds
    // @return {int} return code
    int expires(KVDBHandler* handler, const std::string& key, const std::string& member, int n);

    // Remove and get a element from list head
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param value {std::string&} store the element when successful.
    // @return {int} return code
    int lpop(KVDBHandler* handler, const std::string& key, std::string& value); 

    // Remove and get a element from list head or block until one is available
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param value {std::string&} store the element when successful.
    // @return {int} return code
    int blpop(KVDBHandler* handler, const std::string& key, std::string& value);

    // Remove and get a element from list tail
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param value {std::string&} store the element when successful.
    // @return {int} return code
    int rpop(KVDBHandler* handler, const std::string& key, std::string& value);    

    // Remove and get a element from list tail or block until one is available
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param value {std::string&} store the element when successful.
    // @return {int} return code
    int brpop(KVDBHandler* handler, const std::string& key, std::string& value);

    // Add one element to the head of a list
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param value {const std::string&} the element
    // @return {int} return code
    int lpush(KVDBHandler* handler, const std::string& key, const std::string& value);

    // Add one element to the tail of a list
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param value {const std::string&} the element
    // @return {int} return code
    int rpush(KVDBHandler* handler, const std::string& key, const std::string& value);

    // Get the number of elements in list specified the given key
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @return {int} return the number of elements in list, <0 if error
    int llen(KVDBHandler* handler, const std::string& key);

    // Add one or more member(s) to a set, or update its score if it already exists
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param members {const std::vector<std::string>&} the set storing members
    // @return {int} return code
    int sadd(
            KVDBHandler* handler, 
            const std::string& key,
            const std::vector<std::string>& members);

    // Remove one or more member(s) from a set
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param members {const std::vector<std::string>&} the removing members of a set
    // @return {int} return code
    int srem(
            KVDBHandler* handler, 
            const std::string& key,
            const std::vector<std::string>& members);
    
    // Return the members of a set resulting from the union of all the given sets.
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::vector<std::string> &} the key of all the sets
    // @param members {std::vector<std::string>*} stores the result
    int zunion(
            KVDBHandler* handler, 
            const std::vector<std::string>& key,
            std::vector<std::string>* members);

    // Return the members of a set resulting from the intersection of all the given sets.
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::vector<std::string> &} the key of all the sets
    // @param members {std::vector<std::string>*} stores the result
    int zinter(
            KVDBHandler* handler, 
            const std::vector<std::string>& key,
            std::vector<std::string>* members);

    // Get the number of elements in a set  
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @return {int} return the number of elmenets in a set, <0 if error
    int zcount(
            KVDBHandler* handler, 
            const std::string& key);

}   // namespace kvdb

