#include <string>
#include <iostream>
#include <fstream>
#include "db.h"

using namespace std;
#define CSV_READ_SIZE 500
string DB_FILE = "temp/raw";
struct raw {
    char origin[6];
    char dest[6];
    char arrdelay[10];
};

void db::init(){
	//Do your db initialization.

}

void db::setTempFileDir(string dir){
	//All the files that created by your program should be located under this directory.
    fstream fout;
    fout.open(DB_FILE, ios::out|ios::trunc);
    fout.close();
}

void db::import_csv(string csvDir, string csvName){
    char buf[CSV_READ_SIZE];
    FILE *rd = fopen ((csvDir+"/"+csvName).c_str(), "r");
    FILE *wd = fopen ((DB_FILE).c_str(), "w");
    int stop=0;
    fgets(buf, sizeof buf, rd);
    raw r;
    while (fgets(buf, sizeof buf, rd) != NULL)
    {
        const char *const delim = ",";
        char *const dupstr = strdup(buf);
        char *sepstr = dupstr;
        char *substr = NULL;
        int count = 0;
        substr = strsep(&sepstr, delim);
        do {
            if(count==14) {
                strcpy(r.arrdelay, substr);
                cout << r.arrdelay << endl;
            }
            else if(count==16){
                strcpy(r.origin, substr);
            }
            else if(count==17){
                strcpy(r.dest, substr);
            }
            char write[50];

//            r.arrdelay + "," + r.origin + "," + r.dest + "\n";
//            fwrite(&write, 50, 1, wd);
            count++;
            substr = strsep(&sepstr, delim);
        } while (substr);
        free(dupstr);
        stop++;
    }
    fclose(rd);
    fclose(wd);
    cout << "stop:" << stop << endl;
//    fin.open(csvDir+"/"+csvName, ios::in);
//    fout.open(DB_FILE, ios::out);
//    fin.getline(buf, sizeof(buf));
//    int cnt;
//    while(fin.getline(buf, sizeof(buf))){
//        char *p;
//        p = strtok(buf, ",");
//        raw tmp_raw;
//        for (int i = 0; i < 14; ++i) {
//            p = strtok(NULL, ",");
//        }
//        tmp_raw.arrdelay = atoi(p);
//        p = strtok(NULL, ",");
//        p = strtok(NULL, ",");
//        strcpy(tmp_raw.origin, p);
//        p = strtok(NULL, ",");
//        strcpy(tmp_raw.dest, p);
//        fout << tmp_raw.arrdelay << "," << tmp_raw.origin << "," << tmp_raw.dest << "\n";
//    }
//    fin.close();
//    fout.close();
}

void db::import(string csvDir){
	//Import csv files under this directory.
    import_csv(csvDir, "2006.csv");
    cout << "2006.csv imported" << endl;
    import_csv(csvDir, "2007.csv");
    cout << "2007.csv imported" << endl;
    import_csv(csvDir, "2008.csv");
    cout << "2008.csv imported" << endl;
}

void db::createIndex(){
	//Create index.

}

double db::query(string origin, string dest, int index){
	//Do the query and return the average ArrDelay of flights from origin to dest.
	//This method will be called multiple times.
//    fstream fin;
//    char buf[CSV_READ_SIZE];
//    char origin_char[4];
//    char dest_char[4];
//    strcpy(origin_char, origin.c_str());
//    strcpy(dest_char, dest.c_str());
//    double ans = 0;
//    fin.open(DB_FILE, ios::in);
//    fin.getline(buf, sizeof(buf));
//    long long total, cnt;
//    cnt = 1;
//    total = 0;
//
//    while(fin.getline(buf, sizeof(buf))) {
//        char* p = strtok(buf, ",");
//        raw tmp_raw;
//        tmp_raw.arrdelay = atoi(p);
//        p = strtok(NULL, ",");
//        strcpy(tmp_raw.origin, p);
//        p = strtok(NULL, ",");
//        strcpy(tmp_raw.dest, p);
//        if(strcmp(tmp_raw.origin, origin_char)==0 && strcmp(tmp_raw.dest, dest_char)==0){
//            total += tmp_raw.arrdelay;
//            cnt += 1;
//        }
//    }
//    ans = 1.0*total/cnt;
	return 0;
}

void db::cleanup(){
	//Release memory, close files and anything you should do to clean up your db class.

}