using namespace std;

#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <bitset>
#include <chrono>
#include <csignal>
#include <iostream>
#include <regex>
#include <stdexcept>
#include <string>
#include <vector>
#include <map>

#define INITAL_CAPACITY -1

class ServerInfo {
    public:
        int server_capacity;
        int queue_wait_time;
};

// --------------------------------------------------------------------------------------------------------
// global variables
int has_found_all_server_capacities = 0;
map<string, ServerInfo> server_info_map; // Map<ServerName, ServerInfo>
map<string, vector<string>> server_queues; // Map<ServerName, ServerQueue>
map<string, time_t> request_start_time_map; // Map<RequestFileName, StartTime>
// --------------------------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------------------------------------------------
// our methods

void initServerCapacitiesMap(vector<string> server_names) {
    for (int i = 0; i < server_names.size(); i++) {
        string servername = server_names[i];
        ServerInfo si;
        si.queue_wait_time = 0;
        si.server_capacity = INITAL_CAPACITY;
        server_info_map[servername] = si;
    }
}

int isValidRequestSize(int request_size) {
    return request_size != -1;
}

// returns empty string if there's no server with unknown capacity
string getFirstUnknownCapacityServer(vector<string> server_names) {
    for (int i = 0; i < server_names.size(); i++) {
        string server_name = server_names[i];
        ServerInfo server_info = server_info_map[server_name];
        if (server_info.server_capacity == INITAL_CAPACITY) {
            // found unknown server capacity
            return server_name;
        }
    }
    // unknown server
    return "";
}

string handleValidRequestSizeAllocation(vector<string> server_names, string request_name) {
    if (has_found_all_server_capacities) {

    } else {
        // there exists a server capacity we do not know of
        string server_name = getFirstUnknownCapacityServer(server_names);

        if (server_name.empty()) {
            throw "There's no server with unknown capacity!!";
        }

        // use current request to gauge server's processing capacity
        request_start_time_map[request_name] = time(0); // set request's start time
    }
}

string handleInvalidRequestSizeAllocation(vector<string> server_names, string request_name, int request_size) {
    // TODO
    return server_names[0];
}

string allocateToServer(vector<string> server_names, string request_name, int request_size) {
    if (isValidRequestSize(request_size)) {
        return handleValidRequestSizeAllocation(server_names, request_name);
    } else {
        // placeholder first
        return handleInvalidRequestSizeAllocation(server_names, request_name, request_size);
    }
}

// ------------------------------------------------------------------------------------------------------------------------------

// KeyboardInterrupt handler
void signalHandler(int signum) {
    cout << "Interrupt signal (" << signum << ") received.\n";
    exit(signum);
}

// send trigger to printAll at servers
void sendPrintAll(const int& serverSocket) {
    string message = "printAll\n";
    send(serverSocket, message.c_str(), strlen(message.c_str()), 0);
}

// For example, "1\n2\n3\n4\n5\n" -> "1","2","3","4","5"
// Be careful that "1\n2\n3" -> "1,2" without 3.
vector<string> parseWithDelimiter(string target, string delimiter) {
    string element;
    vector<string> ret;
    size_t pos = 0;
    while ((pos = target.find(delimiter)) != string::npos) {
        element = target.substr(0, pos);
        ret.push_back(element);
        target.erase(0, pos + delimiter.length());
    }
    return ret;
}

// Parse available severnames
vector<string> parseServernames(char* buffer, int len) {
    printf("Servernames: %s\n", buffer);
    string servernames(buffer);

    // parse with delimiter ","
    vector<string> ret = parseWithDelimiter(servernames, ",");
    return ret;
}

// get the completed file's name, what you want to do?
void getCompletedFilename(string filename) {
    /****************************************************
     *                       TODO                       *
     * You should use the information on the completed  *
     * job to update some statistics to drive your      *
     * scheduling policy. For example, check timestamp, *
     * or track the number of concurrent files for each *
     * server?                                          *
     ****************************************************/
     

    /* In this example. just print message */
    printf("[JobScheduler] Filename %s is finished.\n", filename.c_str());

    
    /**************************************************/
}

string parser_filename(string request) {
    vector<string> parsed = parseWithDelimiter(request + ",", ",");
    string filename = parsed[0];
    return filename;
}

// parser of request to 2-tuple
int parser_jobsize(string request) {
    vector<string> parsed = parseWithDelimiter(request + ",", ",");
    int jobsize = stoi(parsed[1]);
    return jobsize; // it can be -1 (i.e., unknown)
}

// formatting: to assign server to the request
string scheduleJobToServer(string servername, string request) {
    return servername + string(",") + request + string("\n");
}

// main part you need to do
string assignServerToRequest(vector<string> servernames, string request) {
    /****************************************************
     *                       TODO                       *
     * Given the list of servers, which server you want *
     * to assign this request? You can make decision.   *
     * You can use a global variables or add more       *
     * arguments.                                       */

    string request_name = parser_filename(request);
    int request_size = parser_jobsize(request);

    if (server_info_map.empty()) {
        // init server
        initServerCapacitiesMap(servernames);
    }

    string server_to_send = allocateToServer(servernames, request_name, request_size);
    string scheduled_request = scheduleJobToServer(server_to_send, request);
    return scheduled_request;
}

void parseThenSendRequest(char* buffer, int len, const int& serverSocket, vector<string> servernames) {
    // print received requests
    printf("[JobScheduler] Received string messages:\n%s\n", buffer);
    printf("[JobScheduler] --------------------\n");
    string sendToServers;

    // parsing to "filename, jobsize" pairs
    vector<string> request_pairs = parseWithDelimiter(string(buffer), "\n");
    for (const auto& request : request_pairs) {
        if (request.find("F") != string::npos) {
            // if completed filenames, print them
            string completed_filename = regex_replace(request, regex("F"), "");
            getCompletedFilename(completed_filename);
        } else {
            // if requests, add "servername" front of the request pair
            sendToServers = sendToServers + assignServerToRequest(servernames, request);
        }
    }
    if (sendToServers.size() > 0) {
        send(serverSocket, sendToServers.c_str(), strlen(sendToServers.c_str()), 0);
    }
}

int main(int argc, char const* argv[]) {
    signal(SIGINT, signalHandler);

    if (argc != 2) {
        throw invalid_argument("must type port number");
        return -1;
    }
    uint32_t portNumber = stoi(string(argv[1]));

    int serverSocket = 0;
    struct sockaddr_in serv_addr;
    if ((serverSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("Socket creation error !");
        return -1;
    }
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(portNumber);
    // Converting IPv4 and IPv6 addresses from text to binary form
    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        printf("\nInvalid address ! This IP Address is not supported !\n");
        return -1;
    }
    if (connect(serverSocket, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        printf("Connection Failed : Can't establish a connection over this socket !");
        return -1;
    }

    struct timeval timeout;
    timeout.tv_sec = 0;
    timeout.tv_usec = 100;

    // set timeout
    if (setsockopt(serverSocket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout) < 0) {
        printf("setsockopt failed\n");
        return -1;
    }
    if (setsockopt(serverSocket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout) < 0) {
        printf("setsockopt failed\n");
        return -1;
    }

    char buffer[4096] = {0};
    int len;

    len = read(serverSocket, buffer, 4096);
    vector<string> servernames = parseServernames(buffer, len);

    int currSeconds = -1;
    auto now = chrono::system_clock::now();
    while (true) {
        try {
            len = read(serverSocket, buffer, 4096);
            if (len > 0) {
                parseThenSendRequest(buffer, len, serverSocket, servernames);
                memset(buffer, 0, 4096);
            }
            sleep(0.00001);  // sufficient for 50ms granualrity
        } catch (const exception& e) {
            cerr << e.what() << '\n';
        }

        // // Example printAll API : let servers print status in every seconds
        // if (chrono::duration_cast<chrono::seconds>(chrono::system_clock::now() - now).count() > currSeconds) {
        //     currSeconds = currSeconds + 1;
        //     sendPrintAll(serverSocket);
        // }
    }
    return 0;
}