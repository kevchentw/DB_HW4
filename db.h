#include <string>
#include <unordered_map>
#include <vector>


class db {
public:
    void init();                                     //Do your db initialization.

    void setTempFileDir(
            std::string);                //All the files that created by your program should be located under this directory.

    void import(std::string);                        //Inport a csv file to your database.

    void createIndex();                              //Create index on one or two columns.

    double query(std::string,
                 std::string);          //Do the query and return the average ArrDelay of flights from origin to dest.

    void cleanup();                                  //Release memory, close files and anything you should do to clean up your db class.

    int index_created;

    std::string temp_path;

    std::string raw_fn;

    std::unordered_map<long long, std::vector<int>> mapping;

};
