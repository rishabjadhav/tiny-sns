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

#include <sys/stat.h>

#define THREADED
#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper_log.h>

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

// global zk handle kept alive for the duration of the server process, persists through session lifespan
static zhandle_t* g_zh = nullptr;
 
void zk_watcher(zhandle_t* zh, int type, int state,
                const char* path, void* ctx) {
    if (state == ZOO_EXPIRED_SESSION_STATE) {
        log(ERROR, "ZooKeeper: session expired - ephemeral zNode has been removed");
    } else if (state == ZOO_CONNECTED_STATE && type == ZOO_SESSION_EVENT) {
        log(INFO, "ZooKeeper: session (re)connected");
    }
    (void)zh; (void)path; (void)ctx;
}
 
// register this server with ZooKeeper by creating an ephemeral zNode
void registerWithZooKeeper(const std::string& zk_addr,
                           const std::string& cluster_id,
                           const std::string& server_id,
                           const std::string& host,
                           const std::string& port) {
    g_zh = zookeeper_init(zk_addr.c_str(), zk_watcher,
                          /*recv_timeout_ms=*/5000,
                          /*clientid=*/0, /*ctx=*/nullptr, /*flags=*/0);
    if (!g_zh) {
        log(ERROR, "ZooKeeper: failed to create handle - server will NOT be "
            "visible to clients");
        return;
    }
 
    int waited = 0;
    while (zoo_state(g_zh) != ZOO_CONNECTED_STATE && waited < 5000) {
        usleep(100000);
        waited += 100;
    }
    if (zoo_state(g_zh) != ZOO_CONNECTED_STATE) {
        log(ERROR, "ZooKeeper: timed out connecting to " + zk_addr);
        zookeeper_close(g_zh);
        g_zh = nullptr;
        return;
    }
    log(INFO, "ZooKeeper: connected to " + zk_addr);
 
    // Ensure parent path /services exists (persistent)
    int rc = zoo_create(g_zh, "/services", nullptr, -1,
                        &ZOO_OPEN_ACL_UNSAFE, 0, nullptr, 0);
    if (rc != ZOK && rc != ZNODEEXISTS) {
        log(ERROR, "ZooKeeper: could not create /services rc=" + std::to_string(rc));
    }
 
    // Ensure /services/TinySNS-service exists (persistent)
    rc = zoo_create(g_zh, "/services/TinySNS-service", nullptr, -1,
                    &ZOO_OPEN_ACL_UNSAFE, 0, nullptr, 0);
    if (rc != ZOK && rc != ZNODEEXISTS) {
        log(ERROR, "ZooKeeper: could not create /services/TinySNS-service rc=" +
            std::to_string(rc));
    }
 
    // Create the ephemeral zNode for this server instance.
    // Node name: cluster_id + "_" + server_id  (e.g. "1_1", "2_1", "3_1")
    std::string node_path = "/services/TinySNS-service/" +
                            cluster_id + "_" + server_id;
 
    // Data the client will read: "host:port"
    std::string node_data = host + ":" + port;
 
    // ZOO_EPHEMERAL ensures automatic deletion when the session closes.
    char created_path[256];
    rc = zoo_create(g_zh, node_path.c_str(),
                    node_data.c_str(), (int)node_data.size(),
                    &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL,
                    created_path, sizeof(created_path));
 
    if (rc == ZOK) {
        log(INFO, "ZooKeeper: registered ephemeral zNode at " +
            std::string(created_path) + " data=" + node_data);
    } else if (rc == ZNODEEXISTS) {
        // A previous run may have left a stale node if the session didn't
        // expire cleanly. Overwrite its data.
        rc = zoo_set(g_zh, node_path.c_str(),
                     node_data.c_str(), (int)node_data.size(), -1);
        if (rc == ZOK) {
            log(INFO, "ZooKeeper: updated existing zNode at " + node_path +
                " data=" + node_data);
        } else {
            log(ERROR, "ZooKeeper: zoo_set failed rc=" + std::to_string(rc));
        }
    } else {
        log(ERROR, "ZooKeeper: zoo_create failed for " + node_path +
            " rc=" + std::to_string(rc));
    }
}

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


// vector that stores every client that has been created
std::vector<Client*> client_db;

// mutex added for heartbeat Ctrl-C logic
std::mutex client_db_mu;


// returns the directory name for this server's persistent storage, holding all timeline/relation files
static std::string g_server_dir;
 
std::string serverFile(const std::string& filename) {
    return g_server_dir + "/" + filename;
}

class SNSServiceImpl final : public SNSService::Service {
 
    Client* getClient(const std::string& username) {
        for (Client* c : client_db) {
            if (c->username == username) return c;
        }
        return nullptr;
    }
 
    std::vector<Message> getLastNMessages(const std::string& filename, int n) {
        std::vector<Message> messages;
        std::ifstream file(filename);
        if (!file.is_open()) return messages;
 
        std::vector<std::string> all_lines;
        std::string line;
        while (std::getline(file, line)) all_lines.push_back(line);
        file.close();
 
        std::vector<Message> all_messages;
        for (size_t i = 0; i < all_lines.size(); ) {
            if (i < all_lines.size() &&
                all_lines[i].length() > 0 && all_lines[i][0] == 'T' &&
                i + 2 < all_lines.size()) {
 
                Message msg;
                std::string timestamp_str = all_lines[i].substr(2);
                struct tm tm = {};
                strptime(timestamp_str.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
                time_t t = mktime(&tm);
                google::protobuf::Timestamp* ts = new google::protobuf::Timestamp();
                ts->set_seconds(t);
                ts->set_nanos(0);
                msg.set_allocated_timestamp(ts);
 
                if (i + 1 < all_lines.size() && all_lines[i+1].length() > 0 &&
                    all_lines[i+1][0] == 'U')
                    msg.set_username(all_lines[i+1].substr(2));
 
                if (i + 2 < all_lines.size() && all_lines[i+2].length() > 0 &&
                    all_lines[i+2][0] == 'W')
                    msg.set_msg(all_lines[i+2].substr(2));
 
                all_messages.push_back(msg);
                i += 4;
            } else {
                i++;
            }
        }
 
        int start_idx = std::max(0, (int)all_messages.size() - n);
        for (size_t i = start_idx; i < all_messages.size(); i++)
            messages.push_back(all_messages[i]);
 
        return messages;
    }
 
    void writeMessageToFile(const std::string& filename, const Message& msg) {
        std::ofstream file(filename, std::ios::app);
        if (!file.is_open()) {
            log(ERROR, "Failed to open file: " + filename);
            return;
        }
        time_t time = msg.timestamp().seconds();
        struct tm* tm = localtime(&time);
        char time_buf[64];
        strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm);
        file << "T " << time_buf << "\n";
        file << "U " << msg.username() << "\n";
        file << "W " << msg.msg() << "\n";
        file << "\n";
        file.close();
    }
 
    void saveFollowingToFile(Client* user) {
        std::string filename = serverFile(user->username + "_relations.txt");
        std::ofstream file(filename, std::ios::trunc);
        if (!file.is_open()) {
            log(ERROR, "Failed to open relations file for writing");
            return;
        }
        for (Client* following : user->client_following)
            file << following->username << "\n";
        file.close();
    }
 
    void loadFollowingFromFile(Client* user) {
        std::string filename = serverFile(user->username + "_relations.txt");
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
 
    Status List(ServerContext* context, const Request* request,
                ListReply* list_reply) override {
        Client* user = nullptr;
        for (Client* c : client_db) {
            if (c->username == request->username()) { user = c; break; }
        }
        for (Client* c : client_db)
            list_reply->add_all_users(c->username);
        if (user != nullptr)
            for (Client* follower : user->client_followers)
                list_reply->add_followers(follower->username);
        log(INFO, "List called by " + request->username());
        return Status::OK;
    }
 
    Status Follow(ServerContext* context, const Request* request,
                  Reply* reply) override {
        std::string u1 = request->username();
        std::string u2 = request->arguments(0);
        Client* c1 = nullptr;
        Client* c2 = nullptr;
        for (Client* c : client_db) {
            if (c->username == u1) c1 = c;
            if (c->username == u2) c2 = c;
        }
        if (u1 == u2) { reply->set_msg("Follow failed -- Can't follow yourself"); return Status::OK; }
        if (c1 == nullptr) { reply->set_msg("Follow Failed -- Requesting user not found"); return Status::OK; }
        if (c2 == nullptr) { reply->set_msg("Follow Failed -- Invalid Username"); return Status::OK; }
        for (Client* following : c1->client_following) {
            if (following->username == u2) {
                reply->set_msg("you have already joined");
                return Status::OK;
            }
        }
        c1->client_following.push_back(c2);
        c2->client_followers.push_back(c1);
        saveFollowingToFile(c1);
        log(INFO, u1 + " followed " + u2);
        reply->set_msg("Follow Successful");
        return Status::OK;
    }
 
    Status UnFollow(ServerContext* context, const Request* request,
                    Reply* reply) override {
        std::string u1 = request->username();
        std::string u2 = request->arguments(0);
        Client* c1 = nullptr;
        Client* c2 = nullptr;
        for (Client* c : client_db) {
            if (c->username == u1) c1 = c;
            if (c->username == u2) c2 = c;
        }
        if (c1 == nullptr) { reply->set_msg("Follow Failed -- Requesting user not found"); return Status::OK; }
        if (c2 == nullptr) { reply->set_msg("UnFollow Failed -- Invalid Username"); return Status::OK; }
        auto it1 = std::find(c1->client_following.begin(), c1->client_following.end(), c2);
        if (it1 == c1->client_following.end()) {
            reply->set_msg("You are not a follower");
            return Status::OK;
        }
        c1->client_following.erase(it1);
        auto it2 = std::find(c2->client_followers.begin(), c2->client_followers.end(), c1);
        if (it2 != c2->client_followers.end()) c2->client_followers.erase(it2);
        saveFollowingToFile(c1);
        log(INFO, u1 + " unfollowed " + u2);
        reply->set_msg("UnFollow Successful");
        return Status::OK;
    }
 
    Status Connect(ServerContext* context, const Request* request,
                   Reply* reply) override {
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
            log(INFO, "New client connected: " + request->username());
            reply->set_msg("Connection Successful");
            return Status::OK;
        }
 
        bool session_alive = c->connected && ((now - c->last_heartbeat) < HEARTBEAT_TIMEOUT);
        if (session_alive) {
            reply->set_msg("you are already connected");
            return Status(grpc::StatusCode::ALREADY_EXISTS, "Already connected");
        }
 
        if (c->last_disconnect_time != 0 && now < c->last_disconnect_time + GRACE_PERIOD)
            return Status(grpc::StatusCode::UNAVAILABLE, "Grace period not elapsed");
 
        c->connected = true;
        c->last_heartbeat = now;
        c->last_disconnect_time = 0;
        log(INFO, "Client reconnected: " + request->username());
        reply->set_msg("Connection Successful");
        return Status::OK;
    }
 
    Status Timeline(ServerContext* context,
                    ServerReaderWriter<Message, Message>* stream) override {
        Message message;
        Client* user = nullptr;
        bool initialized = false;
        std::string timeline_username;
 
        while (stream->Read(&message)) {
            std::string username = message.username();
 
            if (!initialized) {
                initialized = true;
                timeline_username = username;
                user = getClient(username);
                if (user == nullptr) {
                    log(ERROR, "Timeline: user not found: " + username);
                    return Status(grpc::StatusCode::NOT_FOUND, "User not found");
                }
                user->stream = stream;
                user->connected = true;
 
                // Send last 20 messages from the user's inbox (following file)
                std::string inbox_file = serverFile(username + "_following.txt");
                std::vector<Message> last_messages = getLastNMessages(inbox_file, 20);
                for (const Message& m : last_messages) stream->Write(m);
 
                log(INFO, "Timeline initialized for " + username);
                continue;
            }
 
            if (user == nullptr) {
                log(ERROR, "Timeline: user null during post");
                continue;
            }
 
            // Write to user's own timeline file
            std::string own_file = serverFile(user->username + ".txt");
            writeMessageToFile(own_file, message);
            log(INFO, user->username + " posted: " + message.msg());
 
            // Deliver to followers
            for (Client* follower : user->client_followers) {
                if (follower == nullptr) continue;
                std::string inbox_file = serverFile(follower->username + "_following.txt");
                writeMessageToFile(inbox_file, message);
                if (follower->connected && follower->stream != nullptr)
                    follower->stream->Write(message);
            }
        }
 
        if (user == nullptr && !timeline_username.empty())
            user = getClient(timeline_username);
        if (user != nullptr) {
            user->stream = nullptr;
            user->connected = false;
            user->last_disconnect_time = time(NULL);
            log(INFO, user->username + " disconnected from timeline");
        }
        return Status::OK;
    }
 
    Status Heartbeat(ServerContext*, const Request* request,
                     Reply* reply) override {
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
    if (argc != 2) {
        std::cerr << "Usage: ./tsd <config.ini>\n";
        return 1;
    }
 
    mINI::INIFile file(argv[1]);
    mINI::INIStructure ini;
    file.read(ini);
 
    std::string& port       = ini["Server"]["PORT"];
    std::string& cluster_id = ini["Server"]["CLUSTER_ID"];
    std::string& server_id  = ini["Server"]["SERVER_ID"];
    std::string& host       = ini["Server"]["HOST"];
    std::string& zk_addr    = ini["ZooKeeper"]["ADDRESS"];
    std::string& logLevel   = ini["Glog"]["MIN_LOG_LEVEL"];
    std::string& logDir     = ini["Glog"]["LOG_DIR"];
 
    // Set up glog
    FLAGS_logbuflevel = -1;
    FLAGS_timestamp_in_logfile_name = 0;
    FLAGS_minloglevel = std::stoi(logLevel);
    FLAGS_log_dir = logDir;
 
    std::string log_file_name = "server-" + cluster_id + "-" + server_id;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging initialized. Server cluster=" + cluster_id +
        " server=" + server_id + " port=" + port);
 
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
 
    g_server_dir = "server_" + cluster_id + "_" + server_id;
    mkdir(g_server_dir.c_str(), 0755);
    log(INFO, "Server storage directory: " + g_server_dir);
 
    registerWithZooKeeper(zk_addr, cluster_id, server_id, host, port);
 
    RunServer(port);
 
    if (g_zh) zookeeper_close(g_zh);
 
    return 0;
}
