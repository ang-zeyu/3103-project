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
#define NO_REQUEST_SIZE -1

class ServerInfo {
    public:
        string server_name;
        double server_capacity;
        double queue_total_wait_time;
        bool is_queried = false;
        set<string> jobs;
};

struct CompareServerCapacity {
    bool operator()(ServerInfo const &s1, ServerInfo const& s2) {
        return s1.server_capacity < s2.server_capacity;
    }
};

time_t getNowInMilliseconds() {
    struct timeval time_now{};
    gettimeofday(&time_now, nullptr);
    return (time_now.tv_sec * 1000) + (time_now.tv_usec / 1000);
}

#define UNINITIALISED_PROCESS_TIME -1.0

class JobMetadata {
    public:
        double size;
        string server;
        time_t start_time;
        double process_time;
        bool is_capacity_query_packet = false;
    
        double getTimeElapsed() {
            return getNowInMilliseconds() - this->start_time;
        }

        double getServerCapacity() {
            double duration_in_seconds = this->getTimeElapsed() / 1000;
            double capacity = this->size / duration_in_seconds;
            return capacity;
        }
};

// --------------------------------------------------------------------------------------------------------
// global variables
map<string, ServerInfo> server_info_map; // Map<ServerName, ServerInfo>
map<string, JobMetadata> job_metadatas; // Map<file_name, JobMetadata>
priority_queue<ServerInfo, vector<ServerInfo>, CompareServerCapacity> server_info_pq; // Max PQ, meaning top() will return server with biggest capacity
queue<string> accumulated_jobs; // Queue<Request>

// array of top capacity servers,
vector<string> top_servers; 
double TOP_SERVERS_PERCENTAGE_SIZE = 0.4;
size_t TOP_SERVERS_LENGTH = 0; // inits with SERVER_COUNT

size_t SERVER_COUNT = 0;

size_t fifo_index = 0;

size_t SUM_OF_KNOWN_JOB_SIZES = 0;
size_t NUM_JOBS_WITH_KNOWN_SIZE = 0;

// ------------------------------------------------
// variables to adjust
size_t MAX_NUM_ACCUMULATED_JOBS = 20;
size_t MAX_ACCUMULATED_TOLERANCE = 3; // if <= 0, will assume --prob=0
// ------------------------------------------------

#ifdef DEBUG
size_t num_files_sent = 0;
size_t num_files_received = 0;
size_t max_size_of_accumulated_jobs_so_far = 0;
#endif
// --------------------------------------------------------------------------------------------------------

// --------------------------------------------------------------------------------------------------------
// function headers
string assignServerToRequest(vector<string> servernames, string request);
string accumulatedJobsAllocation(vector<string> server_names);
string getMinimumResponseTimeServer(vector<string> server_names, string file_name, int request_size);
// --------------------------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------------------------------------------------
// helper methods

void printAllServerInfo() {
    for (auto const& p : server_info_map) {
        cout << "Server name: " << p.first << " | Wait time: " << p.second.queue_total_wait_time << " | Capacity: " << p.second.server_capacity << endl;
    }
}

void initalizeServerInfo(vector<string> server_names) {
    SERVER_COUNT = server_names.size();
    TOP_SERVERS_LENGTH = TOP_SERVERS_PERCENTAGE_SIZE * SERVER_COUNT;
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

int isZeroKnowledgeRun() {
    return MAX_ACCUMULATED_TOLERANCE <= 0;
}

void decreaseReachedMaxAccumulatedTolerance() {
    if (MAX_ACCUMULATED_TOLERANCE > 0) {
        MAX_ACCUMULATED_TOLERANCE--;
    }
    #ifdef DEBUG
    cout << "MAX_ACCUMULATED_TOLERANCE: " << MAX_ACCUMULATED_TOLERANCE << endl;
    #endif
}

int hasBeenInitialized() {
    return !server_info_map.empty();
}

int hasAServerCapacity() {
    return server_info_pq.size() > 0;
}

int hasKnownJobSizeAverage() {
    return NUM_JOBS_WITH_KNOWN_SIZE != 0;
}

// accumulates requests (not file_name), does not send request
string accumulateJob(string request) {
    accumulated_jobs.push(request);

    #ifdef DEBUG
    cout << "ACCUMULATED: " << request << endl;
    #endif

    return NO_SEND;
}

void updateServerInfo(string file_name) {
    #ifdef DEBUG
    num_files_received++;
    cout << "INCOMING FILE_NAME: " << file_name << endl;
    cout << "NUM FILES SENT: " << num_files_sent << " | NUM FILES RECEIVED: " << num_files_received << endl;
    cout << "SIZE OF ACCUMULATED JOBS: " << accumulated_jobs.size() << " | MAX SIZE SO FAR: " << max_size_of_accumulated_jobs_so_far << endl;
    #endif

    JobMetadata job_metadata = job_metadatas.at(file_name);
    string assigned_server = job_metadata.server;
    server_info_map[assigned_server].jobs.erase(file_name); // update which job goes to which server

    if (job_metadata.size == NO_REQUEST_SIZE) {
        return;
    }

    if (job_metadata.is_capacity_query_packet) {
        // if server capacity has not been found yet
        server_info_map[assigned_server].server_capacity = job_metadata.getServerCapacity(); // update server capacity
        server_info_pq.push(server_info_map[assigned_server]); // got all the info we need
    } else {
        // already have server's capacity
        server_info_map[assigned_server].queue_total_wait_time -= job_metadata.process_time;
    }

    #ifdef DEBUG
    printAllServerInfo();
    #endif
}

// returns empty string if there's no server unqueried
string getFirstUnqueriedCapacityServer(vector<string> server_names, string file_name) {
    for (auto &server_and_info : server_info_map) {
        if (!server_and_info.second.is_queried) {
            // found unknown server capacity
            // as it has not been queried yet
            server_and_info.second.is_queried = true;
            job_metadatas[file_name].is_capacity_query_packet = true;
            return server_and_info.first;
        }
    }

    // unknown server
    return NO_SEND;
}

double getAverageKnownJobSize() {
    if (NUM_JOBS_WITH_KNOWN_SIZE == 0) {
        return NO_REQUEST_SIZE;
    }

    return SUM_OF_KNOWN_JOB_SIZES / NUM_JOBS_WITH_KNOWN_SIZE;
}

void insertMetadataBeforeSend(string server_name, string file_name, int request_size) {
    if (request_size != NO_REQUEST_SIZE) {
        NUM_JOBS_WITH_KNOWN_SIZE++;
        SUM_OF_KNOWN_JOB_SIZES += request_size;
    }

    JobMetadata job_metadata = job_metadatas[file_name];
    job_metadata.size = request_size == NO_REQUEST_SIZE ? getAverageKnownJobSize() : request_size;
    job_metadata.server = server_name;
    job_metadata.start_time = getNowInMilliseconds();
    
    server_info_map[server_name].jobs.insert(file_name); // update which job goes to which server
}

string fifoAllocation(vector<string> server_names) {
    if (server_names.size() == 0) {
        throw invalid_argument("fifoAllocation server_names provided empty!");
    }

    string server_name = server_names[fifo_index];
    fifo_index++;
    fifo_index = fifo_index % server_names.size();

    #ifdef DEBUG
    cout << "FIFO ALLOCATED: " << server_name << endl;
    #endif

    return server_name;
}

string randomAllocation(vector<string> server_names) {
    if (server_names.size() == 0) {
        throw invalid_argument("randomAllocation server_names provided empty!");
    }

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

string leastConnectionAllocation(vector<string> server_names) {
    if (server_names.size() == 0) {
        throw invalid_argument("leastConnectionAllocation server_names provided empty!");
    }

    string server_with_least_con = "";
    size_t min_con = SIZE_MAX;
    for (size_t i = 0; i < server_names.size(); i++) {
        string server_name = server_names[i];
        size_t num_con = server_info_map[server_name].jobs.size();
        if (num_con < min_con) {
            min_con = num_con;
            server_with_least_con = server_name;
        }
    }
    return server_with_least_con;
}

void updateTopKnownServers() {
    top_servers.clear(); // clear first
    vector<ServerInfo> buffer; // used to add back to pq and push to top_servers
    
    size_t length = TOP_SERVERS_LENGTH < server_info_pq.size()
        ? TOP_SERVERS_LENGTH
        : server_info_pq.size();

    for (size_t i = 0; i < length; i++) {
        buffer.push_back(server_info_pq.top());
        server_info_pq.pop();
    }

    // add back to pq and push to top_servers
    for (size_t i = 0; i < length; i++) {
        server_info_pq.push(buffer[i]);
        top_servers.push_back(buffer[i].server_name);
    }
}

// allocates to top capacity server
string topKnownServersCapacityAllocation(vector<string> server_names) {
    if (hasAServerCapacity()) {
        updateTopKnownServers();
        // return fifoAllocation(top_servers);
        return leastConnectionAllocation(top_servers);
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
string getMinimumResponseTimeServer(vector<string> server_names, string file_name, int request_size) {
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

        // insert process time so we can subtract from queue wait time when recv
        job_metadatas[file_name].process_time = process_time;

        server_info_map[min_response_time_server_name].queue_total_wait_time += process_time; // update queue wait time
        return min_response_time_server_name;
    } else {
        // that means we don't know any server's capacity

        // return fifoAllocation(server_names);
        // return randomAllocation(server_names);
        // return topKnownServersCapacityAllocation(server_names);
        // return accumulateJob(request);
        return NO_SEND;
    }
}

string handleValidRequestSizeAllocation(vector<string> server_names, string file_name, int request_size) {
    string server_name = getFirstUnqueriedCapacityServer(server_names, file_name);
    if (!server_name.empty()) {
        // use current request to gauge server's processing capacity

        #ifdef DEBUG
        cout << "QUERYING SERVER: " << server_name << endl;
        #endif

        return server_name;
    } else {
        return getMinimumResponseTimeServer(server_names, file_name, request_size);
    }
}

string handleInvalidRequestSizeAllocation(vector<string> server_names, string file_name) {
    if (hasKnownJobSizeAverage()) {
        return getMinimumResponseTimeServer(server_names, file_name, getAverageKnownJobSize());
    } else {
        return topKnownServersCapacityAllocation(server_names);
    }
}

string allocateToServer(vector<string> server_names, string file_name, int request_size) {
    if (request_size != NO_REQUEST_SIZE) {
        return handleValidRequestSizeAllocation(server_names, file_name, request_size);
    } else {
        return handleInvalidRequestSizeAllocation(server_names, file_name);
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
string scheduleJobToServer(string server_name, string request) {
    string file_name = parser_filename(request);
    int request_size = parser_jobsize(request);

    insertMetadataBeforeSend(server_name, file_name, request_size);

    #ifdef DEBUG
    num_files_sent++;
    cout << "SENT PACKET: " << request << " TO SERVER: " << server_name << endl;
    if (accumulated_jobs.size() > max_size_of_accumulated_jobs_so_far) {
        max_size_of_accumulated_jobs_so_far = accumulated_jobs.size();
    }
    #endif

    return server_name + string(",") + request + string("\n");
}

string accumulatedJobsAllocation(vector<string> server_names) {
    if (!hasAServerCapacity() && accumulated_jobs.size() < MAX_NUM_ACCUMULATED_JOBS) {
        // dont send if dk any server's capacity
        return NO_SEND;
    }

    string sendToServers;
    
    int has_reached_max_accumulated_size = accumulated_jobs.size() == MAX_NUM_ACCUMULATED_JOBS;

    if (has_reached_max_accumulated_size) {
        decreaseReachedMaxAccumulatedTolerance();
    }

    while (accumulated_jobs.size() > 0) {
        string request = accumulated_jobs.front();

        sendToServers += has_reached_max_accumulated_size || isZeroKnowledgeRun()
                ? scheduleJobToServer(leastConnectionAllocation(server_names), request) // leastCon allocate if hit MAX or we guess its zero knowledge run
                : assignServerToRequest(server_names, request);
                
        accumulated_jobs.pop();
    }
    return sendToServers;
}

// main part you need to do
string assignServerToRequest(vector<string> server_names, string request) {
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
        initalizeServerInfo(server_names);
    }

    // Initialise to dummy values
    JobMetadata job_metadata = {
        NO_REQUEST_SIZE,
        "",
        0,
        UNINITIALISED_PROCESS_TIME
    };
    job_metadatas[file_name] = job_metadata;

    string server_to_send = allocateToServer(server_names, file_name, request_size);
    // string server_to_send = fifoAllocation(server_names);

    if (!server_to_send.empty()) {
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
            sendToServers += accumulatedJobsAllocation(servernames) + assignServerToRequest(servernames, request) ;
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