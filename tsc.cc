// -*- coding: utf-8 -*-
// Copyright (C) <2026> <Texas A&M University>
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.
//
// For AGPL-specific network service requirements, see the full license.
//


#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <atomic>
#include <chrono>
#include <grpc++/grpc++.h>
#include "client.h"
#include <functional>
#include <map>

#include "sns.grpc.pb.h"

#define THREADED
#include <zookeeper/zookeeper.h>
#include <sys/stat.h>

#include <glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity);

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using csce_dsc::Message;
using csce_dsc::ListReply;
using csce_dsc::Request;
using csce_dsc::Reply;
using csce_dsc::SNSService;

void sig_ignore(int sig) {
  std::cout << "Signal caught " + sig;
}

Message MakeMessage(const std::string& username, const std::string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}

// watcher callback - required by zookeeper_init but we handle reconnection by re-querying on connect rather than watching continuously.
void zk_watcher(zhandle_t* zh, int type, int state, const char* path, void* ctx) {
    (void)zh;
    (void)type;
    (void)state;
    (void)path;
    (void)ctx;
}
 
// Returns map of cluster_id (1,2,3) -> "host:port"
// Key is the numeric cluster ID parsed from the zNode name e.g. "1_1" -> 1
std::map<int, std::string> getServerMap(const std::string& zk_addr) {
    std::map<int, std::string> server_map;

    zhandle_t* zh = zookeeper_init(zk_addr.c_str(), zk_watcher,
                                   10000, 0, nullptr, 0);
    if (!zh) {
        log(ERROR, "ZooKeeper: failed to create handle for " + zk_addr);
        return server_map;
    }

    int waited = 0;
    while (zoo_state(zh) != ZOO_CONNECTED_STATE && waited < 5000) {
        usleep(100000);
        waited += 100;
    }
    if (zoo_state(zh) != ZOO_CONNECTED_STATE) {
        log(ERROR, "ZooKeeper: timed out connecting to " + zk_addr);
        zookeeper_close(zh);
        return server_map;
    }
    log(INFO, "ZooKeeper: connected to " + zk_addr);

    const char* parent = "/services/TinySNS-service";
    String_vector children;
    memset(&children, 0, sizeof(children));

    int rc = zoo_get_children(zh, parent, 0, &children);
    if (rc != ZOK) {
        log(ERROR, "ZooKeeper: zoo_get_children failed rc=" + std::to_string(rc));
        zookeeper_close(zh);
        return server_map;
    }

    for (int i = 0; i < children.count; ++i) {
        std::string node_name = children.data[i]; // e.g. "1_1", "2_1"

        int cluster_id = std::stoi(node_name.substr(0, node_name.find('_')));

        std::string child_path = std::string(parent) + "/" + node_name;
        char buf[256];
        int buf_len = sizeof(buf) - 1;
        Stat stat;
        rc = zoo_get(zh, child_path.c_str(), 0, buf, &buf_len, &stat);
        if (rc == ZOK && buf_len > 0) {
            buf[buf_len] = '\0';
            server_map[cluster_id] = std::string(buf, buf_len);
            log(INFO, "ZooKeeper: cluster " + std::to_string(cluster_id) +
                " -> " + std::string(buf, buf_len));
        }
    }

    deallocate_String_vector(&children);
    zookeeper_close(zh);
    return server_map;
}

class Client : public IClient
{
public:
    
    Client(const std::string& zk_addr, const std::string& uname) : zk_address(zk_addr), username(uname) {}

protected:
  virtual int    connectTo();
  virtual IReply processCommand(std::string& input);
  virtual void   processTimeline();

private:
    std::string zk_address;
    std::string username;
    std::string server_address;
  
    // client stub
    std::unique_ptr<SNSService::Stub> stub_;
    
    IReply Connect();
    IReply List();
    IReply Follow(const std::string &username);
    IReply UnFollow(const std::string &username);
    void   Timeline(const std::string &username);

    std::thread heartbeat_thread;
    std::atomic<bool> heartbeat_running{false};
    
    // startHeartbeat function for client
    void startHeartbeat() {
        heartbeat_running = true;
        heartbeat_thread = std::thread([this]() {
            while (heartbeat_running) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                if (heartbeat_running) {
                    ClientContext context;
                    Request request;
                    Reply reply;
                    request.set_username(username);
                    stub_->Heartbeat(&context, request, &reply);
                }
            }
        });
    }
};

/*
 * connectTo(): The connectTo() function creates a gRPC stub for a server, selecting server based on client.
 *
 * The function first contacts zookeeper to get a list of all live servers, then selecting one using the hash(username).
 * Then after selecting a server, a gRPC stub is created for the selected server.
 */
int Client::connectTo() {
    log(INFO, "Client: contacting ZooKeeper at " + zk_address);
    std::map<int, std::string> server_map = getServerMap(zk_address);

    if (server_map.empty()) {
        log(ERROR, "Client: no servers available from ZooKeeper");
        return -1;
    }

    int target = (int)(std::hash<std::string>{}(username) % 3);
    int cluster_id = target + 1; // zNode names are 1-based

    log(INFO, "Client: hash(" + username + ") % " +
        std::to_string(3) + " = " + std::to_string(target) +
        " -> cluster " + std::to_string(cluster_id));

    auto it = server_map.find(cluster_id);
    if (it == server_map.end()) {
        log(ERROR, "Client: target cluster " + std::to_string(cluster_id) +
            " is not available in ZooKeeper - cannot connect");
        std::cerr << "Error: assigned server (cluster " << cluster_id
                  << ") is not available. Please try again later.\n";
        return -1;
    }

    server_address = it->second;
    log(INFO, "Client: connecting to cluster " + std::to_string(cluster_id) +
        " at " + server_address);

    stub_ = SNSService::NewStub(
        grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));

    IReply reply = Connect();
    if (reply.grpc_status.ok() && reply.comm_status == SUCCESS) {
        startHeartbeat();
        return 1;
    }
    return -1;
}


/*
 * processCommand(): The processCommand() parses user input and calls the intended function, providing a parameter if it's needed.
 *
 * The function first looks for a space character, to filter for FOLLOW and UNFOLLOW, which have a space and an argument. If there's no space,
 * then the input is either LIST or TIMELINE. For all commands, the appropriate client function is called.
 */
IReply Client::processCommand(std::string& input) {
    IReply ire;

    // index of potential space char in user input cmd
    // only FOLLOW and UNFOLLOW take 2nd param, and thus need space char
    std::size_t ind = input.find_first_of(" ");

    // if space exists, filters for FOLLOW and UNFOLLOW input cmds, and parses arg
    // if not, input is either LIST or TIMELINE
    if (ind != std::string::npos) {
        std::string command = input.substr(0, ind);
        std::string arg = input.substr(ind + 1);

        if (command == "FOLLOW") {
            return Follow(arg);
        } else if (command == "UNFOLLOW") {
            return UnFollow(arg);
        }
    } else {
        if (input == "LIST") {
            return List();
        } else if (input == "TIMELINE") {
            ire.comm_status = SUCCESS;
            return ire;
        }
    }

    ire.comm_status = FAILURE_INVALID;
    return ire;
}


void Client::processTimeline() { 
    Timeline(username);
}

/*
 * List(): The List() function contacts the server to retrieve a list of all users and current user's followers
 *
 * The function first supplies the username of the requesting user in the request, then sends that request to the SERVER's List() function.
 * This function takes gRPC ClientContext, our request object which contains our username. We receive list_reply, which holds a list of all users and
 * all the current user's followers. We populate this in IReply and return it.
 */
IReply Client::List() {

    IReply ire;
    ClientContext context;

    // request is the message sent to the server
    // list_reply is a response from server
    Request request;
    ListReply list_reply;

    request.set_username(username);

    // call List() on server via stub, populate list_reply
    Status status = stub_->List(&context, request, &list_reply);
    ire.grpc_status = status;

    if (status.ok()) {
        ire.comm_status = SUCCESS;

        // add all users to IReply.all_users
        for (const std::string& user : list_reply.all_users()) {
            ire.all_users.push_back(user);
        }

        // add all followers of this user to IReply.followers
        for (const std::string& follower : list_reply.followers()) {
            ire.followers.push_back(follower);
        }
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
    }

    return ire;
}

/*
 * Follow(): The Follow() function calls the server's gRPC Follow() RPC function, which makes the current user follow another specified user.
 *
 * The function supplies the username of the requesting user, and username2, which is the requested user to follow from user argument. This is
 * bundled into request, which is sent to server's Follow() function. 
 *
 */
IReply Client::Follow(const std::string& username2) {

    IReply ire; 
    ClientContext context;
    Request request;
    Reply reply;

    request.set_username(username);
    request.add_arguments(username2);

    // call Follow() on server via stub
    Status status = stub_->Follow(&context, request, &reply);
    ire.grpc_status = status;

    if (status.ok()) {
        if (reply.msg() == "Follow Successful") {
            ire.comm_status = SUCCESS;
        } else if (reply.msg() == "Follow failed -- Can't follow yourself") {
            ire.comm_status = FAILURE_INVALID_USERNAME;
        } else {
            ire.comm_status = FAILURE_ALREADY_EXISTS;
        }
    } else {
        ire.comm_status = FAILURE_INVALID_USERNAME;
    }
      
    return ire;
}

/*
 * UnFollow(): The UnFollow() function calls the server's gRPC UnFollow() RPC function, which makes the current user unfollow another specified user.
 *
 * The function supplies the username of the requesting user, and username2, which is the requested user to unfollow from user argument. This is
 * bundled into request, which is sent to server's UnFollow() function. 
 *
 */
IReply Client::UnFollow(const std::string& username2) {

    IReply ire;
    ClientContext context;
    Request request;
    Reply reply;

    request.set_username(username);
    request.add_arguments(username2);

    // call UnFollow() on server stub
    Status status = stub_->UnFollow(&context, request, &reply);
    ire.grpc_status = status;

    if (status.ok()) {
        if (reply.msg() == "UnFollow Successful") {
            ire.comm_status = SUCCESS;
        } else {
            ire.comm_status = FAILURE_ALREADY_EXISTS;
        }
    } else {
        ire.comm_status = FAILURE_INVALID_USERNAME;
    }

    return ire;
}

/*
 * Connect(): The Connect() function sends request to the server, holding the username requesting to connect. The client uses gRPC to call
 * the Connect() function on the serverside. This function returns a status that's handled accordingly.
 */
IReply Client::Connect() {

    // return object
    IReply ire;
    ClientContext context;

    // request is what's sent to server
    // reply is what's sent back to client
    Request request;
    Reply reply;

    // send client username to server
    request.set_username(username);

    // calls Connect() function on server, RPC call
    Status status = stub_->Connect(&context, request, &reply);
    ire.grpc_status = status;

    if (status.ok()) {
        if (reply.msg() == "you are already connected") {
            ire.comm_status = FAILURE_ALREADY_EXISTS;
        } else {
            ire.comm_status = SUCCESS;
        }
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
    }

    return ire;
}

/*
 * Timeline(): The Timeline() function implements a bidirectional streaming gRPC "timeline" session. 
 * Once in Timeline mode, Timeline opens a persistent streaming connection to the server.
 * You both continuously receive messages from the server, and can send user-written messages.
 *
 */
void Client::Timeline(const std::string& username) {
    ClientContext context;

    // wait 10 seconds so stream fails if server down
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(10));

    auto stream = stub_->Timeline(&context);

    // can establish stream? 
    Message init_msg;
    init_msg.set_username(username);
    if (!stream->Write(init_msg)) {
        std::cerr << "Error: could not connect to timeline - server may be down.\n";
        return;
    }

    std::atomic<bool> stream_alive{true};

    std::thread reader([&stream, &stream_alive]() {
        Message server_msg;
        while (stream->Read(&server_msg)) {
            std::time_t t = server_msg.timestamp().seconds();
            displayPostMessage(server_msg.username(), server_msg.msg(), t);
        }
        stream_alive = false;
    });

    std::thread writer([username, &stream, &stream_alive]() {
        while (stream_alive) {
            std::string input = getPostMessage();
            if (!stream_alive) break;
            Message msg = MakeMessage(username, input);
            if (!stream->Write(msg)) {
                std::cerr << "Error: lost connection to server.\n";
                stream_alive = false;
                break;
            }
        }
        stream->WritesDone();
    });

    writer.join();
    reader.join();

    // return status
    Status status = stream->Finish();
    if (!status.ok()) {
        std::cerr << "Timeline ended: server unavailable ("
                  << status.error_message() << ")\n";
    }
}

int main(int argc, char** argv) {

    // new parameters for zookeeper
    std::string zk_address = "localhost:2181";
    std::string username   = "default";

  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:")) != -1) {
    switch(opt) {
    case 'h': zk_address = optarg; break;
    case 'u': username   = optarg; break;
    default:
      std::cerr << "Usage: " << argv[0] << " -h <zookeeper_host:port> -u <username>\n";
      return 1;
    }
  }

  Client myc(zk_address, username);
  myc.run();
  return 0;
}
