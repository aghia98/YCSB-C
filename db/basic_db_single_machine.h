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
#include <arpa/inet.h> //check if the grpc server port is open
#include "../../gRPC_module/grpc_client.h"
#include "../../SS_no_gmp_module/ss-client.h"
#include "../../HRW_hashing_module/hrw.h"
#include "../../json/json.hpp"
using json = nlohmann::json;

using namespace std;
map<string, string> myMap;
int t=3;
int n=5;
string ip_address = "localhost";
map<int, string> id_to_port_map = {{1, "50001"}, 
                                   {2, "50002"}, 
                                   {3, "50003"},
                                   {4, "50004"},
                                   {5, "50005"}
                                   };

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

  string Delete(const string key){
        ClientContext context;
        keyvaluestore::Key request;
        request.set_key(key);
        keyvaluestore::Value reply;

        Status status = stub_->Delete(&context, request, &reply);

        if (status.ok()) {
            return reply.value();
        } else {
            cout << status.error_code() << ": " << status.error_message() << endl;
            return "RPC failed";
        }
    }

 private:
  unique_ptr<keyvaluestore::KVS::Stub> stub_;
};


bool isPortOpen(const std::string& ipAddress, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        // Failed to create socket
        return false;
    }

    struct sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);
    if (inet_pton(AF_INET, ipAddress.c_str(), &(serverAddress.sin_addr)) <= 0) {
        // Invalid address format
        close(sock);
        return false;
    }

    if (connect(sock, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) == -1) {
        // Connection failed, port is closed
        close(sock);
        return false;
    }

    // Connection successful, port is open
    close(sock);
    return true;
}

map<int, string> parse_json(const string& file_location){

  ifstream file(file_location);
  if (!file.is_open()) {
      std::cerr << "Failed to open JSON file." << std::endl;
  }

  json jsonData;
  try {
      file >> jsonData;
  } catch (json::parse_error& e) {
      std::cerr << "JSON parsing error: " << e.what() << std::endl;
  }

  map<int, std::string> resultMap;

  for (auto it = jsonData.begin(); it != jsonData.end(); ++it) {
      int key = std::stoi(it.key());
      std::string value = it.value();
      resultMap[key] = value;
  }

  return resultMap;

}

void transmit_shares(string k, char** shares, vector<int> x_shares, string ip_address){ //the share (x_share, y_share) is sent to node port offset+x_share
  string v;
  string fixed = ip_address+":";
  string reply;
  KVSClient* kvs;
  int i=0;

  for(int x_share : x_shares){
    kvs = new KVSClient(grpc::CreateChannel(fixed+id_to_port_map[x_share] , grpc::InsecureChannelCredentials()));
    v = shares[i];
    reply = kvs->Put(k,v);
    //cout << "Share transmitted to node id = " << x_share << ": " << endl;
    //cout << v << endl;
    //cout << "Result: " << reply << endl;
    i++;
    //printf("\n");

    delete kvs; 
  }
}

void async_transmit_shares(string k, char** shares, vector<int> x_shares, string ip_address){ //the share (x_share, y_share) is sent to node port offset+x_share
  string v;
  string fixed = ip_address+":";
  string reply;
  KVSClient* kvs;
  int i=0;
  
  CompletionQueue cq;
  unique_ptr<keyvaluestore::KVS::Stub> stub;
  vector<ClientContext> contexts(x_shares.size());
  std::unique_ptr<grpc::ClientAsyncResponseReader<keyvaluestore::Value>> rpc;
  keyvaluestore::KV_pair request;
  vector<keyvaluestore::Value> responses(x_shares.size());
  vector<Status> statuses(x_shares.size());
  for (int i = 0; i < x_shares.size(); i++){
    request.set_key(k);
    request.set_value(shares[i]);
    stub = keyvaluestore::KVS::NewStub(grpc::CreateChannel(fixed+id_to_port_map[x_shares[i]] , grpc::InsecureChannelCredentials()));
    rpc = stub->AsyncPut(&contexts[i], request, &cq);
    rpc->Finish(&responses[i], &statuses[i], (void*)(i+1));
  }
  int num_responses_received = 0;
  while (num_responses_received < x_shares.size()){
    void* got_tag;
    bool ok = false;
    cq.Next(&got_tag, &ok);
    if (ok){
      int response_index = reinterpret_cast<intptr_t>(got_tag) - 1;
      if (statuses[response_index].ok()) {
        //cout << "Share transmitted to node id = " << x_shares[response_index] << ": " << endl;
        //cout << shares[response_index] << endl;
        //printf("\n");
      } else {

      }
      num_responses_received++;
    }
  }

}

string get_shares(vector<int> ids_of_N_active, string secret_id, string ip_address, int t){
    int node_id;
    //int got_shares=0;
    KVSClient* kvs;
    string shares = "";
    string share = "";
    string fixed = ip_address+":";
    vector<string> strings_with_id_of_N_active = convert_ids_to_strings_with_id(ids_of_N_active, "server");
    vector<pair<string, uint32_t>> ordered_strings_with_id_to_hash = order_HRW(strings_with_id_of_N_active,secret_id); //Order according to HRW
    pair<string, uint32_t> pair; 


    for(int i=0; i<t; i++){
        pair = ordered_strings_with_id_to_hash[i];
        node_id = extractNumber(pair.first);
        kvs = new KVSClient(grpc::CreateChannel(fixed+id_to_port_map[node_id], grpc::InsecureChannelCredentials()));
        share = kvs->Get(secret_id);
        //cout << "Got share from node id = " << node_id <<" : " << share << endl;
        shares += share+'\n'; 
        
        delete kvs;
    }

    return shares;
}

string async_get_shares(vector<int> ids_of_N_active, string secret_id, string ip_address, int t){
    int node_id;
    KVSClient* kvs;
    string shares = "";
    string share = "";
    string fixed = ip_address+":";
    vector<string> strings_with_id_of_N_active = convert_ids_to_strings_with_id(ids_of_N_active, "server");
    vector<pair<string, uint32_t>> ordered_strings_with_id_to_hash = order_HRW(strings_with_id_of_N_active,secret_id); //Order according to HRW
    pair<string, uint32_t> pair; 


    CompletionQueue cq;
    unique_ptr<keyvaluestore::KVS::Stub> stub;
    vector<ClientContext> contexts(t);
    std::unique_ptr<grpc::ClientAsyncResponseReader<keyvaluestore::Value>> rpc;
    keyvaluestore::Key request;
    request.set_key(secret_id);
    vector<keyvaluestore::Value> responses(t);
    vector<Status> statuses(t);
    for (int i = 0; i < t; i++){
        pair = ordered_strings_with_id_to_hash[i];
        node_id = extractNumber(pair.first);
        stub = keyvaluestore::KVS::NewStub(grpc::CreateChannel(fixed+id_to_port_map[node_id] , grpc::InsecureChannelCredentials()));
        rpc = stub->AsyncGet(&contexts[i], request, &cq);
        rpc->Finish(&responses[i], &statuses[i], (void*)(i+1));
    }
    int num_responses_received = 0;
    while (num_responses_received < t){
        void* got_tag;
        bool ok = false;
        cq.Next(&got_tag, &ok);
        if (ok){
            int response_index = reinterpret_cast<intptr_t>(got_tag) - 1;
            if (statuses[response_index].ok()) {
                share = responses[response_index].value();
                //cout << "Got share from node id = " << extractNumber(ordered_strings_with_id_to_hash[response_index].first) <<" : " << share << endl;
                shares += share+'\n'; 
            } else {
                 std::cout << statuses[response_index].error_code() << ": " << statuses[response_index].error_message()
                << std::endl;
            }
        } else {
        }
        num_responses_received++;
    }

    return shares;
}


vector<int> get_ids_of_N_active(){
  vector<int> result;

  for (const auto& entry : id_to_port_map) {
    if (isPortOpen("127.0.0.1", stoi(entry.second))) result.push_back(entry.first);
  }

  return result;
}

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

    vector<int> ids_of_N_active;
    vector<string> strings_with_id_of_N_active;
    vector<pair<string, uint32_t>> ordered_strings_with_id_to_hash;
    ids_of_N_active = get_ids_of_N_active();
    if(ids_of_N_active.size() >= t){
        string shares_string = get_shares(ids_of_N_active, key, ip_address, t);
        //cout << shares_string << endl;

    }else{
        cout << "less than t=" << t << " SMS nodes are available. Please retry later." << endl;
    } 
    
    return 0;
  }

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {

      return 0;
    
  }

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    
    Insert(table, key, values);
    
    /*string reply;
    KVSClient* kvs;
    string k,v;
    kvs = new KVSClient(grpc::CreateChannel("localhost:50001", grpc::InsecureChannelCredentials()));
    //k= "yesss"+to_string(cpt);
    k = key;
    //v = "aghiles.ait-messaoud@insa-lyon.frqsqqqqqqqqqqqqqqsssssssss";
    v = values[0].second;
    //std::lock_guard<std::mutex> lock(mutex_);
    reply = kvs->Put(k,v);
    delete kvs;
    cpt++;
    //cout << "Result: " << reply << endl;
    cout << cpt << endl;*/

    return 0;
  }


  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) { 
    
     
    //myMap["yesss"+to_string(cpt)] = "aghiles.ait-messaoud@insa-lyon.frqsqqqqqqqqqqqqqqsssssssss";
    seed_random();
    
    char** shares;
    vector<int> ids_of_N_active;
    vector<string> strings_with_id_of_N_active;
    vector<pair<string, uint32_t>> ordered_strings_with_id_to_hash;
    string k;
    string v;
    char secret[200];
    string fixed = ip_address+":";
    string reply;

    ids_of_N_active = get_ids_of_N_active();

    if(ids_of_N_active.size() >= n){ // enough active SMS nodes
      k = key;
      
      strncpy(secret, values[0].second.c_str(), sizeof(secret) - 1);
      secret[sizeof(secret) - 1] = '\0';
      
      strings_with_id_of_N_active = convert_ids_to_strings_with_id(ids_of_N_active, "server");
      //display_vector(strings_with_id_of_N_active);
      
      ordered_strings_with_id_to_hash = order_HRW(strings_with_id_of_N_active,k); //Order according to HRW

      vector<int> shares_x;

      for(int i=0; i<n;i++){ //extract top-n nodes'id
        auto pair = ordered_strings_with_id_to_hash[i];
        string server_with_id = pair.first;
        //cout << "ID: " << server_with_id << ", Hash: " << pair.second << endl;
        shares_x.push_back(extractNumber(server_with_id));

      }
      char ** shares = generate_share_strings(secret, n, t, shares_x);
      //cout << " ( " << k << " , " << secret << " )\n" << endl;
      transmit_shares(k, shares, shares_x, ip_address);
      //async_transmit_shares(k, shares, shares_x, ip_address);
      //cout << "\n-------------------------------------------\n" << endl;
      free_string_shares(shares, n);
      
      cpt++;
      cout << cpt << endl;

    }else{
      cout << "less than n=" << n << " SMS nodes are available. Please retry later." << endl;
    }
    
    
    /*string reply;
    KVSClient* kvs;
    string k,v;
    kvs = new KVSClient(grpc::CreateChannel("localhost:50001", grpc::InsecureChannelCredentials()));
    k = key;
    v = values[0].second;
    reply = kvs->Put(k,v);
    delete kvs;
    cpt++;
    //cout << "Result: " << reply << endl;
    cout << cpt << endl;*/

    return 0;
  }

  int Delete(const std::string &table, const std::string &key) {
    /*string reply;
    KVSClient* kvs;
    string k,v;
    kvs = new KVSClient(grpc::CreateChannel("localhost:50001", grpc::InsecureChannelCredentials()));
    //k= "yesss"+to_string(cpt);
    k = key;
    reply = kvs->Delete(k);
    delete kvs;
    cpt++;
    //cout << "Result: " << reply << endl;
    cout << cpt << endl;*/

    return 0; 
  }

 private:
  std::mutex mutex_;
};

} // ycsbc

#endif // YCSB_C_BASIC_DB_H_

