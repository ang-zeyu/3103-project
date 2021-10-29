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

// KeyboardInterrupt handler
void signalHandler(int signum) {
    std::cout << "Interrupt signal (" << signum << ") received.\n";
    std::exit(signum);
}

// send trigger to printAll at servers
void sendPrintAll(const int& serverSocket) {
    std::string message = "printAll\n";
    send(serverSocket, message.c_str(), strlen(message.c_str()), 0);
}

// For example, "1\n2\n3\n4\n5\n" -> "1","2","3","4","5"
// Be careful that "1\n2\n3" -> "1,2" without 3.
std::vector<std::string> parseWithDelimiter(std::string target, std::string delimiter) {
    std::string element;
    std::vector<std::string> ret;
    size_t pos = 0;
    while ((pos = target.find(delimiter)) != std::string::npos) {
        element = target.substr(0, pos);
        ret.push_back(element);
        target.erase(0, pos + delimiter.length());
    }
    return ret;
}

// Parse available severnames
std::vector<std::string> parseServernames(char* buffer, int len) {
    printf("Servernames: %s\n", buffer);
    std::string servernames(buffer);

    // parse with delimiter ","
    std::vector<std::string> ret = parseWithDelimiter(servernames, ",");
    return ret;
}

// get the completed file's name, what you want to do?
void getCompletedFilename(std::string filename) {
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

std::string parser_filename(std::string request) {
    std::vector<std::string> parsed = parseWithDelimiter(request + ",", ",");
    std::string filename = parsed[0];
    return filename;
}

// parser of request to 2-tuple
int parser_jobsize(std::string request) {
    std::vector<std::string> parsed = parseWithDelimiter(request + ",", ",");
    int jobsize = std::stoi(parsed[1]);
    return jobsize; // it can be -1 (i.e., unknown)
}

// formatting: to assign server to the request
std::string scheduleJobToServer(std::string servername, std::string request) {
    return servername + std::string(",") + request + std::string("\n");
}

// main part you need to do
std::string assignServerToRequest(std::vector<std::string> servernames, std::string request) {
    /****************************************************
     *                       TODO                       *
     * Given the list of servers, which server you want *
     * to assign this request? You can make decision.   *
     * You can use a global variables or add more       *
     * arguments.                                       */

    std::string request_name = parser_filename(request);
    int request_size = parser_jobsize(request);

    /** 
     *  Logic of scheduling using jobsize
     *  if request size is -1, it is unknown.
     */

    /* Example. always assign to the first server */
    std::string server_to_send = servernames[0];

    /**************************************************/

    /* Schedule the job */
    std::string scheduled_request = scheduleJobToServer(server_to_send, request);
    return scheduled_request;
}

void parseThenSendRequest(char* buffer, int len, const int& serverSocket, std::vector<std::string> servernames) {
    // print received requests
    printf("[JobScheduler] Received string messages:\n%s\n", buffer);
    printf("[JobScheduler] --------------------\n");
    std::string sendToServers;

    // parsing to "filename, jobsize" pairs
    std::vector<std::string> request_pairs = parseWithDelimiter(std::string(buffer), "\n");
    for (const auto& request : request_pairs) {
        if (request.find("F") != std::string::npos) {
            // if completed filenames, print them
            std::string completed_filename = std::regex_replace(request, std::regex("F"), "");
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
        throw std::invalid_argument("must type port number");
        return -1;
    }
    uint32_t portNumber = std::stoi(std::string(argv[1]));

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
    std::vector<std::string> servernames = parseServernames(buffer, len);

    int currSeconds = -1;
    auto now = std::chrono::system_clock::now();
    while (true) {
        try {
            len = read(serverSocket, buffer, 4096);
            if (len > 0) {
                parseThenSendRequest(buffer, len, serverSocket, servernames);
                memset(buffer, 0, 4096);
            }
            sleep(0.00001);  // sufficient for 50ms granualrity
        } catch (const std::exception& e) {
            std::cerr << e.what() << '\n';
        }

        // // Example printAll API : let servers print status in every seconds
        // if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - now).count() > currSeconds) {
        //     currSeconds = currSeconds + 1;
        //     sendPrintAll(serverSocket);
        // }
    }
    return 0;
}