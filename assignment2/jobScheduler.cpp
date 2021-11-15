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
        const string server_name;
        double server_capacity = INITAL_CAPACITY;
        double queue_total_wait_time = 0;
        bool is_queried = false;
        set<string> jobs;

        ServerInfo(const string &server_name) : server_name(server_name) {}
};

typedef string AccumulatedJob;

// ------------------------------------------------------------------------------------------------------------------------------
// helper class to abstract a job

#define UNINITIALISED_PROCESS_TIME -1.0

time_t getNowInMilliseconds() {
    struct timeval time_now{};
    gettimeofday(&time_now, nullptr);
    return (time_now.tv_sec * 1000) + (time_now.tv_usec / 1000);
}

class JobMetadata {
    public:
        const string request;
        double size;
        string server = NO_SEND;
        time_t start_time = 0;
        double process_time = UNINITIALISED_PROCESS_TIME;
        bool is_capacity_query_packet = false;

        JobMetadata(const string &request, const int request_size)
            : request(request), size(request_size)
        {}

        double getServerCapacity() {
            double duration_in_seconds = this->getTimeElapsed() / 1000;
            if (duration_in_seconds < 0.5) {
                // too fast
                return INITAL_CAPACITY;
            } else {
                double capacity = this->size / duration_in_seconds;
                return capacity;
            }

        }
    
    private:
        double getTimeElapsed() {
            return getNowInMilliseconds() - this->start_time;
        }
};

// --------------------------------------------------------------------------------------------------------
// global variables
map<string, ServerInfo> server_info_map; // Map<ServerName, ServerInfo>
map<string, JobMetadata> job_metadatas; // Map<file_name, JobMetadata>
queue<AccumulatedJob> accumulated_jobs; // Queue<Request>

// array of top capacity servers,
vector<string> top_servers; 
double TOP_SERVERS_PERCENTAGE_SIZE = 0.4;

size_t fifo_index = 0;

size_t SUM_OF_KNOWN_JOB_SIZES = 0;
size_t NUM_JOBS_WITH_KNOWN_SIZE = 0;

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
string scheduleJobToServer(string server_name, string file_name);
// --------------------------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------------------------------------------------
// helper methods, classes

void printAllServerInfo() {
    for (auto const& p : server_info_map) {
        cout << "Server name: " << p.first
            << " | Wait time: " << p.second.queue_total_wait_time
            << " | Estimated Capacity: " << p.second.server_capacity
            << endl;
    }
}

void initalizeServerInfo(vector<string> server_names) {
    // ----------------------------------------------------
    // server_info_map
    for (string server_name : server_names) {
        ServerInfo info(server_name);
        server_info_map.insert({server_name, info});
    }
    // ----------------------------------------------------
}

int hasBeenInitialized() {
    return !server_info_map.empty();
}

int hasKnownJobSizeAverage() {
    return NUM_JOBS_WITH_KNOWN_SIZE != 0;
}

// ------------------------------------------------------------------------------------------------------------------------------


// ------------------------------------------------------------------------------------------------------------------------------
// Main stuff

// accumulates requests (not file_name), does not send request
string accumulateJob(string file_name) {
    accumulated_jobs.push(file_name);

    #ifdef DEBUG
    cout << "ACCUMULATED: " << job_metadatas.at(file_name).request << endl;
    #endif

    return NO_SEND;
}

void updateServerInfo(string file_name) {
    JobMetadata &job_metadata = job_metadatas.at(file_name);

    #ifdef DEBUG
    num_files_received++;
    cout << "COMPLETED BY: " << job_metadata.server << endl;
    cout << "NUM FILES SENT: " << num_files_sent << " | NUM FILES RECEIVED: " << num_files_received << endl;
    cout << "SIZE OF ACCUMULATED JOBS: " << accumulated_jobs.size() << " | MAX SIZE SO FAR: " << max_size_of_accumulated_jobs_so_far << endl;
    #endif

    string assigned_server = job_metadata.server;
    server_info_map.at(assigned_server).jobs.erase(file_name); // erase job from server list

    if (job_metadata.size == NO_REQUEST_SIZE) {
        // unknown size, can't drive any statistics
        #ifdef DEBUG
        printAllServerInfo();
        #endif
        return;
    }

    ServerInfo &si = server_info_map.at(assigned_server);
    if (job_metadata.is_capacity_query_packet) {
        // We can drive statistics for server's capacity if it is a is_capacity_query_packet
        double server_capacity = job_metadata.getServerCapacity();
        if (server_capacity == INITAL_CAPACITY) {
            // came back too fast, might be inaccurate
            si.is_queried = false;
        } else {
            si.server_capacity = server_capacity; // update server capacity
        }
        // server_info_pq.push(server_info_map.at(assigned_server)); // got all the info we need
    } else if (job_metadata.process_time != UNINITIALISED_PROCESS_TIME) {
        // already have server's capacity
        si.queue_total_wait_time -= job_metadata.process_time;
    }

    #ifdef DEBUG
    printAllServerInfo();
    #endif
}

// returns empty string if there's no server unqueried
string getFirstUnqueriedCapacityServer(string file_name) {
    for (auto &server_and_info : server_info_map) {
        if (
            !server_and_info.second.is_queried         // criteria 1: no server_capacity yet
            && server_and_info.second.jobs.size() == 0 // criteria 2: no jobs allocated (otherwise calculation will be inaccurate)
        ) {
            server_and_info.second.is_queried = true;
            job_metadatas.at(file_name).is_capacity_query_packet = true;
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

void insertMetadataBeforeSend(string server_name, string file_name) {
    JobMetadata &job_metadata = job_metadatas.at(file_name);

    if (job_metadata.size != NO_REQUEST_SIZE) {
        NUM_JOBS_WITH_KNOWN_SIZE++;
        SUM_OF_KNOWN_JOB_SIZES += job_metadata.size;
    } else {
        // Might return NO_REQUEST_SIZE still if it is a 0% run
        job_metadata.size = getAverageKnownJobSize();
    }

    job_metadata.server = server_name;
    job_metadata.start_time = getNowInMilliseconds();
    
    ServerInfo &si = server_info_map.at(server_name);
    si.jobs.insert(file_name); // update which job goes to which server

    if (
        job_metadata.size != NO_REQUEST_SIZE
        && si.server_capacity != INITAL_CAPACITY
    ) {
        job_metadata.process_time = job_metadata.size / si.server_capacity;
        si.queue_total_wait_time += job_metadata.process_time; // update queue wait time

        #ifdef DEBUG
        cout << "INCREASED server " << server_name << " queue_total_wait_time " << si.queue_total_wait_time << " by " << job_metadata.process_time << endl;
        #endif
    }
}

// this function tries to find the server with the minimum effective response time
// Effective response time is a sum of:
// 1) total wait time from jobs in queue
// 2) how much time the server takes to process "this" job
string getMinimumResponseTimeServer(string file_name) {
    JobMetadata &job = job_metadatas.at(file_name);

    string min_response_time_server_name = NO_SEND;
    double min_response_time = __DBL_MAX__;
    for (auto &server_and_info : server_info_map) {
        ServerInfo &si = server_and_info.second;
        int is_valid_server = si.server_capacity != INITAL_CAPACITY;
        if (is_valid_server) {
            double queue_total_wait_time = si.queue_total_wait_time;
            double process_time_needed = job.size / si.server_capacity;

            double response_time = queue_total_wait_time + process_time_needed;
            if (response_time < min_response_time) {
                min_response_time_server_name = server_and_info.first;
                min_response_time = response_time;
            }
        }
    }

    return min_response_time_server_name;
}

string handleValidRequestSizeAllocation(string file_name) {
    #ifdef DEBUG
    cout << "@handleValidRequestSizeAllocation for " << file_name << endl;
    #endif

    // use current request to gauge any server's processing capacity
    string unqueried_server = getFirstUnqueriedCapacityServer(file_name);
    if (unqueried_server != NO_SEND) {
        #ifdef DEBUG
        cout << "QUERYING SERVER: " << unqueried_server << endl;
        #endif

        return scheduleJobToServer(unqueried_server, file_name);
    }

    string bestServer = getMinimumResponseTimeServer(file_name);
    if (bestServer == NO_SEND) {
        // No servers available, we wait.
        return NO_SEND;
    }

    return scheduleJobToServer(bestServer, file_name);
}

string handleInvalidRequestSizeAllocation(string file_name) {
    #ifdef DEBUG
    cout << "@handleInvalidRequestSizeAllocation for " << file_name << endl;
    #endif

    // Choose any **available** server with the highest capacity
    string highest_capacity_server_name = NO_SEND;
    int highest_capacity = INITAL_CAPACITY - 1;
    for (auto &server_and_info : server_info_map) {
        if (!server_and_info.second.jobs.empty()) {
            continue; // server is busy
        }

        if (server_and_info.second.server_capacity > highest_capacity) {
            highest_capacity_server_name = server_and_info.first;
            highest_capacity = server_and_info.second.server_capacity;
        }
    }

    if (highest_capacity_server_name == NO_SEND) {
        // No servers available, we wait.
        #ifdef DEBUG
        cout << "@handleInvalidRequestSizeAllocation: no servers available for " << file_name << endl;
        #endif
        return NO_SEND;
    }

    return scheduleJobToServer(highest_capacity_server_name, file_name);
}

string allocateToServer(string file_name, int request_size) {
    if (request_size != NO_REQUEST_SIZE) {
        return handleValidRequestSizeAllocation(file_name);
    } else {
        return handleInvalidRequestSizeAllocation(file_name);
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
string scheduleJobToServer(string server_name, string file_name) {
    JobMetadata &job = job_metadatas.at(file_name);

    insertMetadataBeforeSend(server_name, file_name);

    #ifdef DEBUG
    num_files_sent++;
    cout << "SENT PACKET: \"" << job.request << "\" TO SERVER: " << job.server << endl;
    if (accumulated_jobs.size() > max_size_of_accumulated_jobs_so_far) {
        max_size_of_accumulated_jobs_so_far = accumulated_jobs.size();
    }
    #endif

    return server_name + string(",") + job.request + string("\n");
}

string accumulatedJobsAllocation() {
    string sendToServers;

    while (accumulated_jobs.size() > 0) {
        AccumulatedJob file_name = accumulated_jobs.front();
        JobMetadata &job = job_metadatas.at(file_name);

        string assignedServer = allocateToServer(file_name, job.size);

        if (assignedServer == NO_SEND) {
            break; // no servers available for now
        }
        
        #ifdef DEBUG
        cout << "ASSIGNED ACCUMULATED JOB \"" << job.request << "\" TO SERVER: " << assignedServer << endl;
        #endif
        accumulated_jobs.pop();
        sendToServers += assignedServer;
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

    // Initialise
    JobMetadata job_metadata(request, request_size);
    job_metadatas.insert({file_name, job_metadata});

    string assignedServer = allocateToServer(file_name, request_size);
    if (assignedServer == NO_SEND) {
        accumulateJob(file_name);
    }
    
    return assignedServer;
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
            // See if we can assign any accumulated jobs now!
            sendToServers += accumulatedJobsAllocation();
        } else {
            // if requests, add "servername" front of the request pair
            sendToServers += assignServerToRequest(servernames, request) ;
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