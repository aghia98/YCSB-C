//
//  basic_db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_BASIC_DB_H_
#define YCSB_C_BASIC_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include <mutex>
#include <map>
#include "core/properties.h"

using std::cout;
using std::endl;

//################################################################################################
#include "../../gRPC_module/grpc_client.h"

using namespace std;
map<string, string> myMap;
//################################################################################################

class KVSClient {
 public:
  KVSClient(std::shared_ptr<Channel> channel): stub_(keyvaluestore::KVS::NewStub(channel)) {}

  string Get(const string k) {
    keyvaluestore::Key key;
    key.set_key(k);

    keyvaluestore::Value reply;

    grpc::ClientContext context;

    grpc::Status status = stub_->Get(&context, key, &reply);

    // Act upon its status.
    if (status.ok()) {
      return reply.value();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  string Put(const string k, const string v) {
    // Follows the same pattern as SayHello.
    keyvaluestore::KV_pair request;
    request.set_key(k);
    request.set_value(v);
    keyvaluestore::Value reply;
    ClientContext context;

    // Here we can use the stub's newly available method we just added.
    Status status = stub_->Put(&context, request, &reply);
    if (status.ok()) {
      return reply.value();
    } else {
      cout << status.error_code() << ": " << status.error_message()
                << endl;
      return "RPC failed";
    }
  }

 private:
  unique_ptr<keyvaluestore::KVS::Stub> stub_;
};

int cpt = 0;


namespace ycsbc {

class BasicDB : public DB {
 public:
  void Init() {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "A new thread begins working." << endl;
  }

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
    std::lock_guard<std::mutex> lock(mutex_);
    /*cout << "READ " << table << ' ' << key;
    if (fields) {
      cout << " [ ";
      for (auto f : *fields) {
        cout << f << ' ';
      }
      cout << ']' << endl;
    } else {
      cout  << " < all fields >" << endl;
    }*/
    cout << myMap["yesss33"] << endl;
    return 0;
  }

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "SCAN " << table << ' ' << key << " " << len;
    if (fields) {
      cout << " [ ";
      for (auto f : *fields) {
        cout << f << ' ';
      }
      cout << ']' << endl;
    } else {
      cout  << " < all fields >" << endl;
    }
    return 0;
  }

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "UPDATE " << table << ' ' << key << " [ ";
    for (auto v : values) {
      cout << v.first << '=' << v.second << ' ';
    }
    cout << ']' << endl;
    return 0;
  }

//INSERT usertable user12109387703120580018 [ field0=)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) ]
  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) { 
    std::lock_guard<std::mutex> lock(mutex_);
    /*cout << "INSERT " << table << ' ' << key << " [ ";
    for (auto v : values) {
      cout << v.first << '=' << v.second << ' ';
    }
    cout << ']' << endl; */
    cpt++;
    //myMap["yesss"+to_string(cpt)] = "aghiles.ait-messaoud@insa-lyon.frqsqqqqqqqqqqqqqqsssssssss";
    
    string reply;
    KVSClient* kvs;
    string k,v;
    kvs = new KVSClient(grpc::CreateChannel("localhost:50001", grpc::InsecureChannelCredentials()));
    k= "yesss"+to_string(cpt);
    v = "aghiles.ait-messaoud@insa-lyon.frqsqqqqqqqqqqqqqqsssssssss";
    reply = kvs->Put(k,v);
    delete kvs;
    cout << "Result: " << reply << endl;
    
    //cout << cpt << endl;

    return 0;
  }

  int Delete(const std::string &table, const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "DELETE " << table << ' ' << key << endl;
    return 0; 
  }

 private:
  std::mutex mutex_;
};

} // ycsbc

#endif // YCSB_C_BASIC_DB_H_

