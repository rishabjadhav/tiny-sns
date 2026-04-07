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

#include "sns.grpc.pb.h"
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


class Client : public IClient
{
public:
  Client(const std::string& hname,
	 const std::string& uname,
	 const std::string& p)
    :hostname(hname), username(uname), port(p) {}

protected:
  virtual int    connectTo();
  virtual IReply processCommand(std::string& input);
  virtual void   processTimeline();

private:
    std::string hostname;
    std::string username;
    std::string port;
  
    // You can have an instance of the client stub
    // as a member variable.
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


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo() {
  // ------------------------------------------------------------
  // In this function, you are supposed to create a stub so that
  // you call service methods in the processCommand/porcessTimeline
  // functions. That is, the stub should be accessible when you want
  // to call any service methods in those functions.
  // Please refer to gRpc tutorial how to create a stub.
  // ------------------------------------------------------------

    // create gRPC channel and stub
    std::string login_info = hostname + ":" + port;
    stub_ = SNSService::NewStub(grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials()));

    // call connect RPC to register with server
    IReply reply = Connect();

    // if reply status is good and comm_status is successful, return 1
    if (reply.grpc_status.ok() && reply.comm_status == SUCCESS) {
        startHeartbeat();
        return 1;
    } else {
        return -1;
    }
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
    auto stream = stub_->Timeline(&context);

    Message init_msg;
    init_msg.set_username(username);
    stream->Write(init_msg);

    // set up reader thread
    std::thread reader([&stream]() {

        Message server_msg;

        while (stream->Read(&server_msg)) {

            std::time_t t = server_msg.timestamp().seconds();

            displayPostMessage(
                server_msg.username(),
                server_msg.msg(),
                t
            );
        }
    });

    // set up writer thread
    std::thread writer([username, &stream]() {

        while (true) {
            std::string input = getPostMessage();
            Message msg = MakeMessage(username, input);
            stream->Write(msg);
        }

        stream->WritesDone();
    });

    writer.join();
    reader.join();
}

//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  std::string hostname = "localhost";
  std::string username = "default";
  std::string port = "3010";
    
  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:p:")) != -1){
    switch(opt) {
    case 'h':
      hostname = optarg;break;
    case 'u':
      username = optarg;break;
    case 'p':
      port = optarg;break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }
      
  std::cout << "Logging Initialized. Client starting...";
  
  Client myc(hostname, username, port);
  
  myc.run();
  
  return 0;
}
