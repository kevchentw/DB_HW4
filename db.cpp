#include <string>
#include <iostream>
#include <unistd.h>
#include <sys/file.h>
#include <unordered_map>
#include <vector>
#include "db.h"

using namespace std;
#define CSV_READ_SIZE 500

void db::init(){
    index_created = 0;
    temp_path = "";
    raw_fn = "raw";
}

void db::setTempFileDir(string dir){
    temp_path = dir;
    FILE *wd = fopen ((temp_path+"/"+raw_fn).c_str(), "w");
    fclose(wd);
}


void db::import(string csvFileName){
    char buf[CSV_READ_SIZE];
    FILE *rd = fopen ((csvFileName).c_str(), "r");
    FILE *wd = fopen ((temp_path+"/"+raw_fn).c_str(), "ab+");
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
                if(comma_cnt==14 || comma_cnt==16 || comma_cnt==17){
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
    int fd = open((temp_path+"/"+raw_fn).c_str(), O_RDWR);
    long sz = lseek(fd, 0, SEEK_END);
    int BUF_SIZE = 3*1;
    int buf[BUF_SIZE];
    int pos;
    for (pos = 0; pos < sz; pos += BUF_SIZE*4) {
        lseek(fd, pos, SEEK_SET);
        int r = read(fd, &buf, sizeof(buf));
        for (int i = 0; i < BUF_SIZE; i+=3) {
            long long p = (long long)buf[i+1] * 1000000 + buf[i+2];
            if (mapping.find(p) == mapping.end()){
                vector<int> s;
                mapping[p] = s;
            }
            mapping[p].push_back(pos+(4*i));
        }
    }
    for(std::unordered_map<long long, std::vector<int> >::iterator it = mapping.begin(); it != mapping.end(); ++it) {
        cout << it->first/1000000 << " "<< it->first%1000000 << " " << it->second.size() << endl;
    }
    close(fd);
    index_created = 1;
}

double db::query(string origin, string dest){
    int origin_hash = origin[2]*10000 + origin[1]*100 + origin[0];
    int dest_hash = dest[2]*10000 + dest[1]*100 + dest[0];
    int fd = open((temp_path+"/"+raw_fn).c_str(), O_RDWR);
    long sz = lseek(fd, 0, SEEK_END);
    int r_dly;
    long long sum = 0;
    int cnt = 0;
    double ans;
    if(index_created==0) {
        int BUF_SIZE = 3*1;
        int buf[BUF_SIZE];
        int pos;
        for (pos = 0; pos < sz; pos += BUF_SIZE*4) {
            lseek(fd, pos, SEEK_SET);
            int r = read(fd, &buf, sizeof(buf));
            for (int i = 0; i < BUF_SIZE; i+=3) {
                if(origin_hash == buf[i+1] && dest_hash == buf[i+2]){
                    sum += buf[i];
                    cnt++;
                }
            }
        }
    }
    else{
        long long p = (long long)origin_hash * 1000000 + dest_hash;
        for(std::vector<int>::iterator it = mapping[p].begin(); it != mapping[p].end(); ++it) {
            lseek(fd, *it, SEEK_SET);
            read(fd, &r_dly, 4);
            sum += r_dly;
            cnt++;
        }
    }
    ans = 1.0*sum/cnt;
//    cout << "sum: " << sum << endl;
//    cout << "cnt: " << cnt << endl;
//    cout << "ans: " << ans << endl;
    return ans;
}

void db::cleanup(){

}
