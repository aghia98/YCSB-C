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
#include <vector>
#include "core/properties.h"
#include <chrono>
#include <thread>


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
int n=8;
int N=10;
int default_sms_port = 50000;

//Update this list
int ids_of_N_active[] = {1,2,3,4,5,6,7,8,9,10};
vector<string>  strings_with_id_of_N_active = { "server1",
                                                "server2",
                                                "server3",
                                                "server4",
                                                "server5",
                                                "server6",
                                                "server7",
                                                "server8",
                                                "server9",
                                                "server10"
                                                };
map<int, string> id_to_address_map = {{1, "10.0.0.15"}, 
                                      {2, "10.0.0.4"}, 
                                      {3, "10.0.0.10"},
                                      {4, "10.0.0.11"},
                                      {5, "10.0.0.12"},
                                      {6, "10.0.0.9"},
                                      {7, "10.0.0.20"},
                                      {8, "10.0.0.21"},
                                      {9, "10.0.0.22"},
                                      {10,"10.0.0.23"}
                                     };

#define NUM_CHANNELS 8
int cpt = 0;
unique_ptr<keyvaluestore::KVS::Stub> stub;
map<std::thread::id, unique_ptr<keyvaluestore::KVS::Stub>> threadIdtoStub; //rpc = threadIdtoStub[std::this_thread::get_id()]->AsyncPut(&context, request, &cq);
map< std::thread::id, map<int , unique_ptr<keyvaluestore::KVS::Stub> > > threadIdtoMultipleStubs;

grpc::SslCredentialsOptions ssl_opts;

int cpt_put = 0;
int cpt_get = 0;
int total_put_latency = 0;
int total_get_latency = 0; 
int max_get_latency=0;
int max_put_latency=0;
//################################################################################################

std::string readFile(const std::string& filename) {
    std::ifstream file(filename);
    if (!file) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return "";
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

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

/*void transmit_shares(string k, char** shares, vector<int> x_shares){ //the share (x_share, y_share) is sent to node port offset+x_share
  string v;
  string reply;
  KVSClient* kvs;
  int i=0;

  for(int x_share : x_shares){
    kvs = new KVSClient(grpc::CreateChannel(id_to_address_map[x_share]+":"+to_string(default_sms_port) , grpc::InsecureChannelCredentials()));
    v = shares[i];
    reply = kvs->Put(k,v);
    //cout << "Share transmitted to node id = " << x_share << ": " << endl;
    //cout << v << endl;
    //cout << "Result: " << reply << endl;
    i++;
    //printf("\n");
    if(i==1) break;
    delete kvs;
  }
}*/

void transmit_shares_replica(string k, string& value){ //the share (x_share, y_share) is sent to node port offset+x_share
  string v;
  string reply;
  KVSClient* kvs;
  int i=0;

  for(int i = 0; i<n; i++){
    kvs = new KVSClient(grpc::CreateChannel(id_to_address_map[i+1]+":"+to_string(default_sms_port) , grpc::InsecureChannelCredentials()));
    v = value;
    reply = kvs->Put(k,v);
    if(i==n) break;
    delete kvs;
  }
}

void async_transmit_shares(string k, char** shares, vector<int> x_shares){ //the share (x_share, y_share) is sent to node port offset+x_share
  string v;
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
    stub = keyvaluestore::KVS::NewStub(grpc::CreateChannel(id_to_address_map[x_shares[i]]+":"+to_string(default_sms_port), grpc::InsecureChannelCredentials()));
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

void async_transmit_shares_replica(string k, string& value){ 
  string v;
  string reply;
  KVSClient* kvs;
  int i=0;
  
  CompletionQueue cq;
  unique_ptr<keyvaluestore::KVS::Stub> stub;
  vector<ClientContext> contexts(n);
  std::unique_ptr<grpc::ClientAsyncResponseReader<keyvaluestore::Value>> rpc;
  keyvaluestore::KV_pair request;
  vector<keyvaluestore::Value> responses(n);
  vector<Status> statuses(n);
  for (int i = 0; i < n; i++){
    request.set_key(k);
    request.set_value(value);
    stub = keyvaluestore::KVS::NewStub(grpc::CreateChannel(id_to_address_map[i+1]+":"+to_string(default_sms_port), grpc::InsecureChannelCredentials()));
    rpc = stub->AsyncPut(&contexts[i], request, &cq);
    rpc->Finish(&responses[i], &statuses[i], (void*)(i+1));
    //cout << "sent"  << i << endl;
  }
  int num_responses_received = 0;
  while (num_responses_received < n){
    
    void* got_tag;
    bool ok = false;
    cq.Next(&got_tag, &ok);
    if (ok){
      int response_index = reinterpret_cast<intptr_t>(got_tag) - 1;
      /*if (statuses[response_index].ok()) {
        //cout << "Share transmitted to node id = " << x_shares[response_index] << ": " << endl;
        //cout << shares[response_index] << endl;
        //printf("\n");
      } else {

      }*/
      num_responses_received++;
      //cout << "received "  << response_index+1 << endl;
      //if(num_responses_received = 1) break;
    }
  }
  //cout << "end" << endl;

}


int transmitted = 0;
void async_transmit_single_share(string k, string& value){ 
  
  CompletionQueue cq;
  ClientContext context;
  std::unique_ptr<grpc::ClientAsyncResponseReader<keyvaluestore::Value>> rpc;
  keyvaluestore::KV_pair request;
  keyvaluestore::Value response;
  Status status;
  
  request.set_key(k);
  request.set_value(value);

  rpc = threadIdtoStub[std::this_thread::get_id()]->AsyncPut(&context, request, &cq);
  rpc->Finish(&response, &status, (void*)1);
  
  
  void* got_tag;
  bool ok = false;
  cq.Next(&got_tag, &ok);
  if (ok){
    if(status.ok()){
      transmitted++;
      //cout << "transmitted" << transmitted << endl;
    }else{
      cout << "transmitted" << transmitted << endl;
    }
      //cout << "sent ok"  << endl; 
  }else{
    
  }
}

void async_transmit_single_share_multiple_nodes_hrw(string& k, string& value){ 
  
  //Order according to HRW
  vector<pair<string, uint32_t>> ordered_strings_with_id_to_hash = order_HRW(strings_with_id_of_N_active,k); 

  //extract top-n nodes
  vector<int> ordered_node_ids;
  for(int i=0; i<n;i++){ //extract top-n nodes'id
      auto pair = ordered_strings_with_id_to_hash[i];
      string server_with_id = pair.first;
      ordered_node_ids.push_back(extractNumber(server_with_id));
  }

  //send requests
  int sent = 0;
  CompletionQueue cq;
  vector<ClientContext> contexts(n);
  std::unique_ptr<grpc::ClientAsyncResponseReader<keyvaluestore::Value>> rpc;
  keyvaluestore::KV_pair request;
  vector<keyvaluestore::Value> responses(n);
  vector<Status> statuses(n);
  request.set_key(k);
  request.set_value(value);

  for (int node_id: ordered_node_ids){
    rpc = threadIdtoMultipleStubs[std::this_thread::get_id()][node_id]->AsyncPut(&contexts[sent], request, &cq);
    rpc->Finish(&responses[sent], &statuses[sent], (void*)(sent+1));
    sent++;
    if(sent == n) break;
  }

  int num_responses_received = 0;
  while (num_responses_received < n){
    void* got_tag;
    bool ok = false;
    cq.Next(&got_tag, &ok);
    if (ok){
      num_responses_received++;
    }
  }

  /*void* got_tag;
  bool ok = false;
  for(int i=0; i<n; i++){
    cq.Next(&got_tag, &ok);
  }*/
}

string get_shares(vector<int> ids_of_N_active, string secret_id, int t){
    int port;
    int node_id;
    //int got_shares=0;
    KVSClient* kvs;
    string shares = "";
    string share = "";
    vector<string> strings_with_id_of_N_active = convert_ids_to_strings_with_id(ids_of_N_active, "server");
    vector<pair<string, uint32_t>> ordered_strings_with_id_to_hash = order_HRW(strings_with_id_of_N_active,secret_id); //Order according to HRW
    pair<string, uint32_t> pair; 

    for(int i=0; i<t; i++){
        pair = ordered_strings_with_id_to_hash[i];
        node_id = extractNumber(pair.first);
        kvs = new KVSClient(grpc::CreateChannel(id_to_address_map[node_id]+":"+to_string(default_sms_port), grpc::InsecureChannelCredentials()));
        share = kvs->Get(secret_id);
        //cout << "Got share from node id = " << node_id <<" : " << share << endl;
        shares += share+'\n'; 
        
        delete kvs;
    }

  return shares;
}

string async_get_shares(vector<int> ids_of_N_active, string secret_id, int t){
    int node_id;
    KVSClient* kvs;
    string shares = "";
    string share = "";
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
        stub = keyvaluestore::KVS::NewStub(grpc::CreateChannel(id_to_address_map[node_id]+":"+to_string(default_sms_port) , grpc::InsecureChannelCredentials()));
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
                //cout << share << endl;
                //cout << "Got share from node id = " << extractNumber(ordered_strings_with_id_to_hash[response_index].first) <<" for key " << secret_id << " : "<< share << endl;
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

void async_get_single_share_multiple_machines(string& secret_id){
    string shares="";
    string share="";
      //Order according to HRW
    vector<pair<string, uint32_t>> ordered_strings_with_id_to_hash = order_HRW(strings_with_id_of_N_active,secret_id); 

    //extract top-n nodes
    vector<int> ordered_node_ids;
    for(int i=0; i<n;i++){ //extract top-n nodes'id
        auto pair = ordered_strings_with_id_to_hash[i];
        string server_with_id = pair.first;
        ordered_node_ids.push_back(extractNumber(server_with_id));
    }

    //send requests
    int sent = 0;
    CompletionQueue cq;
    vector<ClientContext> contexts(n);
    std::unique_ptr<grpc::ClientAsyncResponseReader<keyvaluestore::Value>> rpc;
    keyvaluestore::Key request;
    vector<keyvaluestore::Value> responses(n);
    vector<Status> statuses(n);
    request.set_key(secret_id);

    for (int node_id: ordered_node_ids){
      //cout << "to send to node_id " << node_id << endl;
      rpc = threadIdtoMultipleStubs[std::this_thread::get_id()][node_id]->AsyncGet(&contexts[sent], request, &cq);
      rpc->Finish(&responses[sent], &statuses[sent], (void*)(sent+1));
      sent++;
      if(sent == n) break;
    }

    int num_responses_received = 0;
    while (num_responses_received < n){
      void* got_tag;
      bool ok = false;
      cq.Next(&got_tag, &ok);
      if (ok){
        int response_index = reinterpret_cast<intptr_t>(got_tag) - 1;
        if (statuses[response_index].ok()) {
            share = responses[response_index].value();
            shares += share+'\n'; 
            
        }
      }
      num_responses_received++;
    }

}

string async_get_single_share(const string& secret_id){

    string share = "";
    CompletionQueue cq;
    ClientContext context;
    std::unique_ptr<grpc::ClientAsyncResponseReader<keyvaluestore::Value>> rpc;
    keyvaluestore::Key request;
    request.set_key(secret_id);
    keyvaluestore::Value response;
    Status status;
    
    //rpc = stub->AsyncGet(&context, request, &cq);
    rpc = threadIdtoStub[std::this_thread::get_id()]->AsyncGet(&context, request, &cq);
    rpc->Finish(&response, &status, (void*)(1));
    
    
    void* got_tag;
    bool ok = false;
    cq.Next(&got_tag, &ok);
    if (ok){
        if (status.ok()) {
            share = response.value();
            //cout << share << endl;
            //cout << "Got share from node id = " << extractNumber(ordered_strings_with_id_to_hash[response_index].first) <<" for key " << secret_id << " : "<< share << endl;
        } else {
              std::cout << status.error_code() << ": " << status.error_message()<< std::endl;
        }
    } else {
    }
    

    return share;
}



vector<int> get_ids_of_N_active(){
  vector<int> result;

  for (const auto& entry : id_to_address_map) {
    if (isPortOpen(entry.second, default_sms_port)) result.push_back(entry.first);
  }

  return result;
}




namespace ycsbc {

class BasicDB : public DB {
 public:
  void Init() {
    std::lock_guard<std::mutex> lock(mutex_);
    if(cpt==0){
      std::string server_cert = "server.crt";
      ssl_opts.pem_root_certs = readFile(server_cert);
    }
    cout << "A new thread begins working." << endl;
    
    //threadIdtoStub[std::this_thread::get_id()] = keyvaluestore::KVS::NewStub(grpc::CreateCustomChannel(id_to_address_map[1]+":"+to_string(default_sms_port), channel_creds, channel_args));
    //Create channels
    for(int node_id=1; node_id<N+1; node_id++){
      cpt++;
      grpc::ChannelArguments channel_args;
      auto channel_creds = grpc::SslCredentials(ssl_opts);
      channel_args.SetInt("channel_number", cpt);
      channel_args.SetSslTargetNameOverride("server");
      threadIdtoMultipleStubs[std::this_thread::get_id()][node_id] = keyvaluestore::KVS::NewStub(grpc::CreateCustomChannel(id_to_address_map[node_id]+":"+to_string(default_sms_port), channel_creds, channel_args));
    }
    
  }

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
    string k = key;

    //string shares_string = myMap[key];

    //vector<int> ids_of_N_active;

    //ids_of_N_active = {1};
    //ids_of_N_active = get_ids_of_N_active();
    //if(ids_of_N_active.size() >= t){
        
    //auto start_time = std::chrono::high_resolution_clock::now();
    //string shares_string = async_get_shares(ids_of_N_active, key, t);
    
    //string shares_string = async_get_single_share(key);
    async_get_single_share_multiple_machines(k);

    //std::cout << shares_string << std::endl;

    //auto end_time = std::chrono::high_resolution_clock::now();
    //auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    //cpt_get++;
    //total_get_latency += duration.count();
    /*auto latency = duration.count();
    if(latency > max_get_latency) {
        max_get_latency = latency;
        std::cout << "Max latency Get: " << max_get_latency << " ms" << std::endl;
    }*/
    
    //cout << shares_string << endl;
    //cout << cpt_get << endl;
    //}else{
        //cout << "less than t=" << t << " SMS nodes are available. Please retry later." << endl;  
    //}
    
    return 0;
  }

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {

      return 0;
    
  }

  

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {

    seed_random();

    //myMap[key] = values[0].second;
    
    char** shares;
    vector<int> ids_of_N_active;
    vector<string> strings_with_id_of_N_active;
    vector<pair<string, uint32_t>> ordered_strings_with_id_to_hash;
    string k;
    string v;
    //char secret[200];
    string reply;

    //ids_of_N_active = {1};
    //ids_of_N_active = get_ids_of_N_active();
    
    //if(ids_of_N_active.size() >= n){ // enough active SMS nodes
      k = key;
      
      /*strncpy(secret, values[0].second.c_str(), sizeof(secret) - 1);
      secret[sizeof(secret) - 1] = '\0';
      
      strings_with_id_of_N_active = convert_ids_to_strings_with_id(ids_of_N_active, "server");
      
      ordered_strings_with_id_to_hash = order_HRW(strings_with_id_of_N_active,k);*/ //Order according to HRW
      //vector<int> shares_x;

      /*for(int i=0; i<n;i++){ //extract top-n nodes'id
        auto pair = ordered_strings_with_id_to_hash[i];
        string server_with_id = pair.first;
        //cout << "ID: " << server_with_id << ", Hash: " << pair.second << endl;
        shares_x.push_back(extractNumber(server_with_id));
      }*/
      //shares = generate_share_strings(secret, n, t, shares_x);
      //transmit_shares(k, shares, shares_x, values[0]);
      //cout << "wriiiiiiiiiiiiiiiiite" << endl;

      //transmit_shares_replica(k, values[0].second);

      //auto start_time = std::chrono::high_resolution_clock::now();

      //async_transmit_shares_replica(k, values[0].second);
      //async_transmit_single_share(k, values[0].second);
      async_transmit_single_share_multiple_nodes_hrw(k, values[0].second);
      //myMap[k]=values[0].second;
      
      //auto end_time = std::chrono::high_resolution_clock::now();
      //auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
      //cpt_put++;
      //total_put_latency += duration.count();
      /*int latency = duration.count();

      if(latency > max_put_latency) {
            max_put_latency = latency;
            std::cout << "Max latency Put: " << max_put_latency << " ms" << std::endl;
        }*/
      //std::cout << cpt_put << std::endl;
      /*if(cpt_put>20000){
        std::cout << "Avg latency Put: " << total_put_latency/cpt_put << " ms" << std::endl;
      }*/
      
      

      //async_transmit_shares(k, shares, shares_x);
      //async_transmit_shares_replica(k, shares, shares_x, values[0].second);
      //free_string_shares(shares, n);
      
      //cpt++;
      

    /*}else{
      std::cout << "Crashhh" << std::endl;
      std::cout << cpt_put << std::endl;
      //cout << cpt << endl;
      //cout << "less than n=" << n << " SMS nodes are available. Please retry later." << endl;
    }*/
    
    

    return 0;
    
  }


  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) { 
    
    Update(table, key, values);

    return 0;

  }

  int Delete(const std::string &table, const std::string &key) {


    return 0; 
  }

 private:
  std::mutex mutex_;
};

} // ycsbc

#endif // YCSB_C_BASIC_DB_H_

