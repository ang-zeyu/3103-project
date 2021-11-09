using namespace std;

#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include <bitset>
#include <chrono>
#include <csignal>
#include <iostream>
#include <regex>
#include <stdexcept>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <random>
#include <queue>

#define DEBUG 1
#define INITAL_CAPACITY -1
#define NO_SEND ""

class ServerInfo {
    public:
        string server_name;
        double server_capacity;
        double queue_total_wait_time;
};

struct CompareServerCapacity {
    bool operator()(ServerInfo const &s1, ServerInfo const& s2) {
        return s1.server_capacity < s2.server_capacity;
    }
};

// --------------------------------------------------------------------------------------------------------
// global variables
map<string, ServerInfo> server_info_map; // Map<ServerName, ServerInfo>
map<string, time_t> request_start_time_map; // Map<FileName, StartTime>
map<string, int> job_size_map; // Map<FileName, requestSize>
map<string, string> job_to_server_allocation_map; // Map<FileName, ServerName>
map<string, double> job_to_process_time; // Map<FileName, ProcessTimeRequired>
priority_queue<ServerInfo, vector<ServerInfo>, CompareServerCapacity> server_info_pq; // Max PQ, meaning top() will return server with biggest capacity
set<string> queried_servers; // Set<ServerName>
set<string> capacity_query_packets; // Set<FileName>
queue<string> accumulated_jobs; // Queue<Request>
set<string> accumulated_jobs_set; // Set<Request>

size_t SERVER_COUNT = 0;

size_t fifo_index = 0;

#ifdef DEBUG
size_t num_files_sent = 0;
size_t num_files_received = 0;
#endif
// --------------------------------------------------------------------------------------------------------

// --------------------------------------------------------------------------------------------------------
// function headers
string assignServerToRequest(vector<string> servernames, string request);
void accumulatedJobsAllocation(vector<string> server_names);
// --------------------------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------------------------------------------------
// helper methods

time_t getNowInMilliseconds() {
    struct timeval time_now{};
    gettimeofday(&time_now, nullptr);
    return (time_now.tv_sec * 1000) + (time_now.tv_usec / 1000);
}

void printAllServerInfo() {
    for (auto const& p : server_info_map) {
        cout << "Server name: " << p.first << " | Wait time: " << p.second.queue_total_wait_time << " | Capacity: " << p.second.server_capacity << endl;
    }
}

void initalizeServerInfo(vector<string> server_names) {
    SERVER_COUNT = server_names.size();
    for (size_t i = 0; i < server_names.size(); i++) {
        string server_name = server_names[i];

        // ----------------------------------------------------
        // server_info_map
        ServerInfo si;
        si.server_name = server_name;
        si.queue_total_wait_time = 0;
        si.server_capacity = INITAL_CAPACITY;
        server_info_map[server_name] = si;
        // ----------------------------------------------------
    }
}

int hasBeenInitialized() {
    return !server_info_map.empty();
}

int isValidRequestSize(int request_size) {
    return request_size != -1;
}

int hasSentCapacityQueryPacket(string server_name) {
    return queried_servers.count(server_name);
}

int hasAllServerCapacities() {
    return server_info_pq.size() == SERVER_COUNT;
}

int hasAServerCapacity() {
    return server_info_pq.size() > 0;
}

int hasMetadata(string file_name) {
    return job_to_server_allocation_map.count(file_name) > 0 && request_start_time_map.count(file_name) > 0 && job_size_map.count(file_name) > 0;
}

int wasAccumulated(string request) {
    return accumulated_jobs_set.count(request);
}

// accumulates requests (not file_name), does not send request
string accumulateJob(string request) {
    accumulated_jobs.push(request);
    accumulated_jobs_set.insert(request);

    #ifdef DEBUG
    cout << "ACCUMULATED: " << request << endl;
    #endif

    return NO_SEND;
}

void updateServerInfo(string file_name) {
    #ifdef DEBUG
    num_files_received++;
    #endif

    if (!hasMetadata(file_name)) {
        return;
    }

    string assigned_server = job_to_server_allocation_map[file_name];
    time_t duration = getNowInMilliseconds() - request_start_time_map[file_name];

    int size = job_size_map[file_name];
    
    double duration_in_seconds = ((double) duration) / 1000;
    double capacity = size / duration_in_seconds;

    int is_capacity_query_packet = capacity_query_packets.count(file_name);

    if (is_capacity_query_packet) {
        // if server capacity has not been found yet
        server_info_map[assigned_server].server_capacity = capacity; // update server capacity
        server_info_pq.push(server_info_map[assigned_server]); // got all the info we need
    } else {
        // already have server's capacity
        double process_time = job_to_process_time[file_name];
        server_info_map[assigned_server].queue_total_wait_time -= process_time;
    }

    #ifdef DEBUG
    printAllServerInfo();
    cout << "NUM FILES SENT: " << num_files_sent << " | NUM FILES RECEIVED: " << num_files_received << endl;
    cout << "IS ACCUMULATED JOB EMPTY: " << accumulated_jobs.empty() << endl;
    cout << "FRONT ACCUMULATED JOB: " << accumulated_jobs.front() << endl;
    #endif
}

// returns empty string if there's no server unqueried
string getFirstUnqueriedCapacityServer(vector<string> server_names, string file_name) {
    for (size_t i = 0; i < server_names.size(); i++) {
        string server_name = server_names[i];
        if (!hasSentCapacityQueryPacket(server_name)) {
            // found unknown server capacity
            // as it has not been queried yet
            queried_servers.insert(server_name);
            capacity_query_packets.insert(file_name);
            return server_name;
        }
    }
    // unknown server
    return NO_SEND;
}

void insertMetadataBeforeSend(string server_name, string file_name, int request_size) {
    request_start_time_map[file_name] = getNowInMilliseconds(); // set request's start time
    job_size_map[file_name] = request_size;
    job_to_server_allocation_map[file_name] = server_name;
}

string fifoAllocation(vector<string> server_names) {
    string server_name = server_names[fifo_index];
    fifo_index++;
    fifo_index = fifo_index % SERVER_COUNT;

    #ifdef DEBUG
    cout << "FIFO ALLOCATED: " << server_name << endl;
    #endif

    return server_name;
}

string randomAllocation(vector<string> server_names) {
    const int range_from = 0;
    const int range_to = server_names.size() - 1;

    std::random_device rd;
    std::default_random_engine eng(rd());
    std::uniform_int_distribution<int> distr(range_from, range_to);

    int randomIndex = distr(eng);

    #ifdef DEBUG
    cout << "RANDOMLY ALLOCATED: " << server_names[randomIndex] << endl;
    #endif

    return server_names[randomIndex];
}

// allocates to top capacity server, if unknown will default to random allocation
string topKnownServerCapacityAllocation(vector<string> server_names, string request) {
    if (hasAServerCapacity()) {
        #ifdef DEBUG
        cout << "ALLOCATED TO TOP SERVER: " << server_info_pq.top().server_name << " | JOB: " << request << endl;
        #endif

        return server_info_pq.top().server_name;
    } else {
        // return randomAllocation(server_names);
        // return accumulateJob(request);
        return NO_SEND;
    }
}

// this function tries to find the server with the minimum effective response time
// Effective response time is a sum of:
// 1) total wait time from jobs in queue
// 2) how much time the server takes to process "this" job
// However if we cannot find a valid server, in the event where we
// do not know of any server's capacity, it'll default to FIFO
string getMinimumResponseTimeServer(vector<string> server_names, string file_name, string request, int request_size) {
    string min_response_time_server_name = "";
    double min_response_time = __DBL_MAX__;
    double process_time = 0;
    for (size_t i = 0; i < server_names.size(); i++) {
        string server_name = server_names[i];
        ServerInfo si = server_info_map[server_name];
        int is_valid_server = si.server_capacity != INITAL_CAPACITY;
        if (is_valid_server) {
            double server_capacity = si.server_capacity;

            double queue_total_wait_time = si.queue_total_wait_time;
            double process_time_needed = request_size / server_capacity;

            double response_time = queue_total_wait_time + process_time_needed;
            if (response_time < min_response_time) {
                min_response_time_server_name = server_name;
                min_response_time = response_time;
                process_time = process_time_needed;
            }
        }
    }

    if (!min_response_time_server_name.empty()) {
        // update stats
        job_to_process_time[file_name] = process_time; // insert process time so we can subtract from queue wait time when recv
        server_info_map[min_response_time_server_name].queue_total_wait_time += process_time; // update queue wait time

        insertMetadataBeforeSend(min_response_time_server_name, file_name, request_size);
        return min_response_time_server_name;
    } else {
        // that means we don't know any server's capacity

        // return fifoAllocation(server_names);
        // return randomAllocation(server_names);
        // return topKnownServerCapacityAllocation(server_names);
        // return accumulateJob(request);
        return NO_SEND;
    }
}

string handleValidRequestSizeAllocation(vector<string> server_names, string file_name, string request, int request_size) {
    string server_name = getFirstUnqueriedCapacityServer(server_names, file_name);
    if (!server_name.empty()) {
        // use current request to gauge server's processing capacity
        insertMetadataBeforeSend(server_name, file_name, request_size);

        #ifdef DEBUG
        cout << "QUERYING SERVER: " << server_name << endl;
        #endif

        return server_name;
    } else {
        return getMinimumResponseTimeServer(server_names, file_name, request, request_size);
    }
}

string handleInvalidRequestSizeAllocation(vector<string> server_names, string request) {
    // TODO
    // return fifoAllocation(server_names);
    // return randomAllocation(server_names);
    return topKnownServerCapacityAllocation(server_names, request);
}

string allocateToServer(vector<string> server_names, string file_name, string request, int request_size) {
    if (isValidRequestSize(request_size)) {
        return handleValidRequestSizeAllocation(server_names, file_name, request, request_size);
    } else {
        return handleInvalidRequestSizeAllocation(server_names, request);
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
    updateServerInfo(filename);
    
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

// recursive function for dealing with accumulated jobs
void accumulatedJobsAllocation(vector<string> server_names) {
    if (accumulated_jobs.size() == 0) {
        return;
    }

    string request = accumulated_jobs.front();
    accumulated_jobs.pop();
    
    accumulatedJobsAllocation(server_names);
    assignServerToRequest(server_names, request);
}

// main part you need to do
string assignServerToRequest(vector<string> servernames, string request) {
    /****************************************************
     *                       TODO                       *
     * Given the list of servers, which server you want *
     * to assign this request? You can make decision.   *
     * You can use a global variables or add more       *
     * arguments.                                       */

    string file_name = parser_filename(request);
    int request_size = parser_jobsize(request);

    if (!hasBeenInitialized()) {
        // init server
        initalizeServerInfo(servernames);
    }

    if (hasAServerCapacity()) {
        // if found a server capacity, then assign accumulated requests
        accumulatedJobsAllocation(servernames);
    }

    string server_to_send = allocateToServer(servernames, file_name, request, request_size);

    if (!server_to_send.empty()) {
        #ifdef DEBUG
        num_files_sent++;
        cout << "SENT PACKET: " << request << " TO SERVER: " << server_to_send << endl;
        #endif

        return scheduleJobToServer(server_to_send, request);;
    } else {
        // algo decided not to send request, accumulate it
        return accumulateJob(request);
    }
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

        // Example printAll API : let servers print status in every seconds
        if (chrono::duration_cast<chrono::seconds>(chrono::system_clock::now() - now).count() > currSeconds) {
            currSeconds = currSeconds + 1;
            sendPrintAll(serverSocket);
        }
    }
    return 0;
}