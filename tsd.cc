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

#include <ctime>
#include <algorithm> 

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include <mutex>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "ini.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce_dsc::Message;
using csce_dsc::ListReply;
using csce_dsc::Request;
using csce_dsc::Reply;
using csce_dsc::SNSService;


struct Client {
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }

  // used to enforce 1 second connection rule
  time_t last_heartbeat = 0;
  time_t last_disconnect_time = 0;
};

//Vector that stores every client that has been created
std::vector<Client*> client_db;

// mutex added for heartbeat Ctrl-C logic
std::mutex client_db_mu;

class SNSServiceImpl final : public SNSService::Service {

    // getClient helper function
    Client* getClient(const std::string& username) {
        for (Client* c : client_db) {
            if (c->username == username) {
                return c;
            }
        }
        return nullptr;
    }

    // helper function to read last N messages from a file
    std::vector<Message> getLastNMessages(const std::string& filename, int n) {
        std::vector<Message> messages;
        std::ifstream file(filename);
    
        if (!file.is_open()) {
            return messages;
        }
    
        std::vector<std::string> all_lines;
        std::string line;

        while (std::getline(file, line)) {
            all_lines.push_back(line);
        }
        file.close();
    
        // parse messages from the end
        // format: T <timestamp>\nU <username>\nW <msg>\n<empty line>
        std::vector<Message> all_messages;
    
        for (size_t i = 0; i < all_lines.size(); ) {
            if (i < all_lines.size() && all_lines[i].length() > 0 && all_lines[i][0] == 'T' && i + 2 < all_lines.size()) {
                Message msg;
        
                // parse timestamp (line starting with 'T ')
                std::string timestamp_str = all_lines[i].substr(2);
                struct tm tm = {};
                strptime(timestamp_str.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
                time_t t = mktime(&tm);
                google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
                timestamp->set_seconds(t);
                timestamp->set_nanos(0);
                msg.set_allocated_timestamp(timestamp);
            
                // parse username (line starting with 'U ')
                if (i + 1 < all_lines.size() && all_lines[i + 1].length() > 0 && all_lines[i + 1][0] == 'U') {
                    std::string username = all_lines[i + 1].substr(2);  // Skip "U "
                    msg.set_username(username);
                }
            
                // parse message content (line starting with 'W ')
                if (i + 2 < all_lines.size() && all_lines[i + 2].length() > 0 && all_lines[i + 2][0] == 'W') {
                    std::string content = all_lines[i + 2].substr(2);  // Skip "W "
                    msg.set_msg(content);
                }
            
                all_messages.push_back(msg);
                i += 4;  // Skip T, U, W, and empty line
            } else {
                i++;
            }
        }
        
        // get last N messages
        int start_idx = std::max(0, (int)all_messages.size() - n);
        for (size_t i = start_idx; i < all_messages.size(); i++) {
            messages.push_back(all_messages[i]);
        }
        
        return messages;
    }

    // helper function to write message to file
    void writeMessageToFile(const std::string& filename, const Message& msg) {
        std::ofstream file(filename, std::ios::app);  // Append mode
        
        if (!file.is_open()) {
            log(ERROR, "Failed to open file: " + filename);
            return;
        }
    
        // format timestamp
        time_t time = msg.timestamp().seconds();
        struct tm* tm = localtime(&time);
        char time_buf[64];
        strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm);
        
        // write in required format
        file << "T " << time_buf << "\n";
        file << "U " << msg.username() << "\n";
        file << "W " << msg.msg() << "\n";
        file << "\n";  // Empty line separator
        
        file.close();
    }

        void saveFollowingToFile(Client* user) {
            std::string filename = user->username + "_relations.txt";
            std::ofstream file(filename, std::ios::trunc);

            if (!file.is_open()) {
                log(ERROR, "Failed to open relations file for writing");
                return;
            }

            for (Client* following : user->client_following) {
                file << following->username << "\n";
            }

            file.close();
        }

        void loadFollowingFromFile(Client* user) {
            std::string filename = user->username + "_relations.txt";
            std::ifstream file(filename);

            if (!file.is_open()) return;

            std::string line;
            while (std::getline(file, line)) {
                Client* followed = getClient(line);
                if (followed != nullptr) {
                    user->client_following.push_back(followed);
                    followed->client_followers.push_back(user);
                }
            }

            file.close();
    }


public:
  

    /*
    * List(): The List() function is called by the client via stub. It returns lists of all users and all those following the requesting user.
    *
    * The function receives a Request, which holds the user's name. This username identifies the user so we know whose following list to return.
    * We add all users to list_reply, along with all users who follow the requesting user.
    *
    */
    Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
        Client* user = nullptr;

        // find requesting client via username provided in Request object
        for (Client* c : client_db) {
            if (c->username == request->username()) {
                user = c;
                break;
            }
        }

        // add all users to list_reply
        for (Client* c : client_db) {
            list_reply->add_all_users(c->username);
        }

        // add followers of this user to list_reply
        if (user != nullptr) {
            for (Client* follower : user->client_followers) {
                list_reply->add_followers(follower->username);
            }
        }
        return Status::OK;
    }

    /*
    * Follow(): The Follow() function is called by the client via stub. It identifies both the requesting user and the follower, then adds users
    * to eachother's following list.
    *
    * The function identifies the requesting user by Request.username and the requested follower by the first argument provided.
    * Then, both requesting user and requested follower are searched for in client_db. Then it checks if requesting user is already
    * following the requested follower. If not, we add the requested follower to the requested user's following list, and the requested
    * user to the requested follower's followers list.
    *
    */
    Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
        std::string u1 = request->username();
        std::string u2 = request->arguments(0);

        Client* c1 = nullptr;
        Client* c2 = nullptr;

        for (Client* c : client_db) {
            if (c->username == u1) c1 = c;
            if (c->username == u2) c2 = c;
        }

        if (u1 == u2) {
            reply->set_msg("Follow failed -- Can't follow yourself");
            return Status::OK;
        }

        if (c1 == nullptr) {
            reply->set_msg("Follow Failed -- Requesting user not found");
            return Status::OK;
        }

        if (c2 == nullptr) {
            reply->set_msg("Follow Failed -- Invalid Username");
            return Status::OK;
        }

        // check if already following
        for (Client* following : c1->client_following) {
            if (following->username == u2) {
                reply->set_msg("you have already joined");
                return Status::OK;
            }
        }

        // add c2 to c1's following
        // add c1 to c2's followers
        c1->client_following.push_back(c2);
        c2->client_followers.push_back(c1);
        saveFollowingToFile(c1);


        reply->set_msg("Follow Successful");

        return Status::OK; 
    }

    /*
    * UnFollow(): The UnFollow() function is called by the client via stub. It identifies both the requesting user and the follower, then removes users
    * from eachother's respective lists.
    *
    * The function identifies the requesting user by Request.username and the requested follower by the first argument provided.
    * Then, both requesting user and requested follower are searched for in client_db. We search through the requesting user's following to find the
    * requested follower to unfollow. If not found, then just return. If found, remove from following. Then search through requested to unfollow's followers,
    * removing the user if found.
    *
    */
    Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
        std::string u1 = request->username();
        std::string u2 = request->arguments(0);

        Client* c1 = nullptr;
        Client* c2 = nullptr;

        for (Client* c : client_db) {
            if (c->username == u1) c1 = c;
            if (c->username == u2) c2 = c;
        }

        if (c1 == nullptr) {
            reply->set_msg("Follow Failed -- Requesting user not found");
            return Status::OK;
        }

        if (c2 == nullptr) {
            reply->set_msg("UnFollow Failed -- Invalid Username");
            return Status::OK;
        }

        // search through requesting user's following, if not found, return
        auto it1 = std::find(c1->client_following.begin(), c1->client_following.end(), c2);
        if (it1 == c1->client_following.end()) {
            reply->set_msg("You are not a follower");
            return Status::OK;
        }

        // remove requested to unfollow from requesting user's following
        c1->client_following.erase(it1);
        
        // search through requested to unfollow's followers
        auto it2 = std::find(c2->client_followers.begin(), c2->client_followers.end(), c1);
        if (it2 != c2->client_followers.end()) {
            // remove requested user from requested to unfollow's followers
            c2->client_followers.erase(it2);
        }

        saveFollowingToFile(c1);
        reply->set_msg("UnFollow Successful");
        return Status::OK;
    }

    /*
    * Connect(): Handles a client trying to connect to server.
    *
    * First the function checks if the requesting user is already connected.
    * If not, we create a new Client, populating the username and adding to client_db.
    * If already connected and we had a recent heartbeat, or client has some other status, set as connected and return.
    *
    */
    Status Connect(ServerContext* context, const Request* request, Reply* reply) override {
      std::lock_guard<std::mutex> lk(client_db_mu);

      Client* c = getClient(request->username());
      time_t now = time(NULL);
      const time_t HEARTBEAT_TIMEOUT = 2;
      const time_t GRACE_PERIOD = 2;

      if (c == nullptr) {
        c = new Client();
        c->username = request->username();
        c->connected = true;
        c->last_heartbeat = now;
        c->last_disconnect_time = 0;
        client_db.push_back(c);
        loadFollowingFromFile(c);
        reply->set_msg("Connection Successful");
        return Status::OK;
      }

      bool session_alive = c->connected && ((now - c->last_heartbeat) < HEARTBEAT_TIMEOUT);
      if (session_alive) {
        reply->set_msg("you are already connected");
        return Status(grpc::StatusCode::ALREADY_EXISTS, "Already connected");
      }

      if (c->last_disconnect_time != 0 && now < c->last_disconnect_time + GRACE_PERIOD) {
        return Status(grpc::StatusCode::UNAVAILABLE, "Grace period not elapsed");
      }

      c->connected = true;
      c->last_heartbeat = now;
      c->last_disconnect_time = 0;
      reply->set_msg("Connection Successful");
      return Status::OK;
    }

    /*
    * Timeline(): Handles a continuous connection with one or more clients. This function runs as long as the client keeps the stream open.
    *
    * First, we continuously read from the stream sent by the client. The stream allows us to Read() from client, and Write() to client.
    * We read from stream until client DCs or stream breaks. Each iteration is one message from client.
    *
    * If it's the first message we are sending, don't treat it like a real message and use it to initialize user. If not, get message and
    * forward to all user's followers, writing to persistence files and logging.
    *   
    */
    Status Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {

        Message message;
        Client* user = nullptr;
        bool initialized = false;
        std::string timeline_username;

        while (stream->Read(&message)) {

            std::string username = message.username();

            if (!initialized) {
                initialized = true;
                timeline_username = message.username();

                user = getClient(username);
                if (user == nullptr) {
                    log(ERROR, "Timeline: User not found: " + username);
                    return Status(grpc::StatusCode::NOT_FOUND, "User not found");
                }

                user->stream = stream;
                user->connected = true;

                // send last 20 messages
                std::string inbox_file = username + "_following.txt";
                std::vector<Message> last_messages =
                    getLastNMessages(inbox_file, 20);

                for (const Message& m : last_messages) {
                    stream->Write(m);
                }

                log(INFO, "Timeline: " + username + " initialized");

                continue;
            }

            if (user == nullptr) {
                user->last_heartbeat = time(NULL);
                log(ERROR, "Timeline: User null during post");
                continue;
            }

            // write to user's own timeline file
            std::string own_file = user->username + ".txt";
            writeMessageToFile(own_file, message);

            log(INFO, user->username + " posted: " + message.msg());

            // deliver message to followers
            for (Client* follower : user->client_followers) {

                if (follower == nullptr) continue;

                // Write to follower inbox file
                std::string inbox_file =
                    follower->username + "_following.txt";

                writeMessageToFile(inbox_file, message);

                if (follower->connected &&
                    follower->stream != nullptr) {

                    follower->stream->Write(message);
                }
            }
        }

        if (user == nullptr && !timeline_username.empty()) {
            user = getClient(timeline_username);
        }
        if (user != nullptr) {
            user->stream = nullptr;
            user->connected = false;
            user->last_disconnect_time = time(NULL);
            log(INFO, user->username + " disconnected");
        }

        return Status::OK;
    }

    /*
    * Heartbeat(): Heartbeat now sends the client it's last heartbeat time. This is used to enforce the 1 second reconnection condition.
    *
    * The function sets a lock giard, and sets the last heartbeat of the client to whenever it's called.
    */
    Status Heartbeat(ServerContext*, const Request* request, Reply* reply) override {
      std::lock_guard<std::mutex> lk(client_db_mu);

      Client* client = getClient(request->username());
      if (!client) return Status(grpc::StatusCode::NOT_FOUND, "Client not found");

      client->last_heartbeat = time(NULL);
      client->connected = true;

      reply->set_msg("Heartbeat acknowledged");
      return Status::OK;
    }
};
 
void RunServer(std::string port_no) {
    std::string server_address = "0.0.0.0:"+port_no;
    SNSServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    log(INFO, "Server listening on: "+server_address);

    std::thread([]() {
    const time_t HEARTBEAT_TIMEOUT = 2;

    while (true) {
        sleep(1);
        time_t now = time(NULL);

        std::lock_guard<std::mutex> lk(client_db_mu);
        for (Client* c : client_db) {
          if (!c) continue;

          if (c->connected && (now - c->last_heartbeat) >= HEARTBEAT_TIMEOUT) {
            c->connected = false;
            c->stream = nullptr;
            // best estimate of when client died is their last heartbeat time
            c->last_disconnect_time = c->last_heartbeat;
            log(INFO, "Liveness: " + c->username + " marked disconnected");
          }
        }
      }
    }).detach();

    server->Wait();
}

int main(int argc, char** argv) {
    if(argc != 2) {
        std::cerr << "Invalid Command Line Arguments: Usage .\\tsd <config.ini>\n";
        exit(0);
    }

    mINI::INIFile file(argv[1]);
    mINI::INIStructure ini;

    // read the config file
    file.read(ini);

    // read configuration values
    std::string&  port = ini["Server"]["PORT"];
    std::string&  logLevel = ini["Glog"]["MIN_LOG_LEVEL"];
    std::string&  logDir = ini["Glog"]["LOG_DIR"];

    // Disable glog buffering for immediate output
    FLAGS_logbuflevel = -1;
    FLAGS_timestamp_in_logfile_name = 0;
    
    FLAGS_minloglevel = std::stoi(logLevel); 
    FLAGS_log_dir = logDir;

    std::string log_file_name = std::string("server-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting ...");

    RunServer(port);

    return 0;
}
