#include <string>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <stdio.h>
#include "db.h"

using namespace std;
#define CSV_READ_SIZE 500

void db::init(){
    index_created = 0;
    temp_path = "";
    raw_fn = "raw";
    mapping_origin.clear();
    mapping_dest.clear();
}

void db::setTempFileDir(string dir){
	//All the files that created by your program should be located under this directory.
    temp_path = dir;
    FILE *wd = fopen ((temp_path+raw_fn).c_str(), "w");
    fclose(wd);
}


void db::import(string csvFileName){
    char buf[CSV_READ_SIZE];
    FILE *rd = fopen ((csvFileName).c_str(), "r");
    FILE *wd = fopen ((temp_path+raw_fn).c_str(), "ab+");
    fgets(buf, sizeof buf, rd); // ignore first line
    int char_cnt;
    int comma_cnt;
    char now_char;
    char small_buf[10];
    int small_buf_cnt;
    int write_data[3];
    while (fgets(buf, sizeof buf, rd) != NULL)
    {
        char_cnt = 0;
        comma_cnt = 0;
        now_char = buf[char_cnt];
        small_buf_cnt = 0;
        while(now_char!='\n'){
            if(now_char==','){
                comma_cnt++;
                if(comma_cnt==15){
                    if(small_buf[0]=='N'){
                        break;
                    }
                    small_buf[small_buf_cnt+1] = '\0';
                    write_data[0] = atoi(small_buf);
                }
                else if(comma_cnt==17){
                    write_data[1] = small_buf[2]*10000 + small_buf[1]*100 + small_buf[0];
                }
                else if(comma_cnt==18){
                    write_data[2] = small_buf[2]*10000 + small_buf[1]*100 + small_buf[0];
                    fwrite(write_data, 1, sizeof(write_data), wd);
                    break;
                }
                small_buf_cnt = 0;
            }
            else{
                if(comma_cnt==14){
                    small_buf[small_buf_cnt++] = now_char;
                }
                else if(comma_cnt==16){
                    small_buf[small_buf_cnt++] = now_char;
                }
                else if(comma_cnt==17){
                    small_buf[small_buf_cnt++] = now_char;
                }
            }
            char_cnt++;
            now_char = buf[char_cnt];
        }
    }
    fclose(rd);
    fclose(wd);
}

void db::createIndex(){
    int fd = open((temp_path+raw_fn).c_str(), O_RDWR);
    long sz = lseek(fd, 0, SEEK_END);
    int BUF_SIZE = 3*4096;
    int buf[BUF_SIZE];
    int r_origin, r_dest, pos;
    for (pos = 0; pos < sz; pos += BUF_SIZE*4) {
        lseek(fd, pos, SEEK_SET);
        read(fd, &buf, sizeof(buf));
        for (int i = 0; i < BUF_SIZE; i+=3) {
            r_origin = buf[i+1];
            r_dest = buf[i+2];
            if (mapping_origin.find(r_origin) == mapping_origin.end()) {
                vector<int> s;
                mapping_origin[r_origin] = s;
            }
            if (mapping_dest.find(r_dest) == mapping_dest.end()) {
                vector<int> s;
                mapping_dest[r_dest] = s;
            }
            mapping_origin[r_origin].push_back(pos+(4*i));
            mapping_dest[r_dest].push_back(pos+(4*i));
        }
    }
    close(fd);
    index_created = 1;
}

double db::query(string origin, string dest){
    int origin_hash = origin[2]*10000 + origin[1]*100 + origin[0];
    int dest_hash = dest[2]*10000 + dest[1]*100 + dest[0];
    int fd = open((temp_path+raw_fn).c_str(), O_RDWR);
    long sz = lseek(fd, 0, SEEK_END);
    int r_dly, r_origin, r_dest;
    long long total = 0;
    long long sum = 0;
    int cnt = 0;
    if(index_created==0) {
        for (int pos = 0; pos < sz; pos += 12) {
            total++;
            lseek(fd, pos + 4, SEEK_SET);
            read(fd, &r_origin, 4);
            if (r_origin != origin_hash) {
                continue;
            }
            lseek(fd, pos + 8, SEEK_SET);
            read(fd, &r_dest, 4);
            if (r_dest != dest_hash) {
                continue;
            }
            lseek(fd, pos, SEEK_SET);
            read(fd, &r_dly, 4);
            sum += r_dly;
            cnt++;
        }
    }
    else{
        sort(mapping_origin[origin_hash].begin(), mapping_origin[origin_hash].end());
        sort(mapping_dest[dest_hash].begin(), mapping_dest[dest_hash].end());
        unsigned long p_a = 0, p_b = 0;
        int v_a, v_b;
        while(p_a<mapping_origin[origin_hash].size() && p_b<mapping_dest[dest_hash].size()){
            v_a = mapping_origin[origin_hash].at(p_a);
            v_b = mapping_dest[dest_hash].at(p_b);
            if(v_a>v_b){
                p_b++;
            }
            else if(v_a<v_b){
                p_a++;
            }
            else{
                lseek(fd, v_a, SEEK_SET);
                read(fd, &r_dly, 4);
                sum += r_dly;
                cnt++;
                p_a++;
                p_b++;
            }
        }
    }
    cout << "sum: " << sum << endl;
    cout << "cnt: " << cnt << endl;
    cout << "ans: " << 1.0*sum/cnt << endl;
	return 1.0*sum/cnt;
}

void db::cleanup(){

}