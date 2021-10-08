#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <omp.h>
#include <time.h>
#include <signal.h>
#include <vector>
#include <string>

const int NUM_MAX_QUEUED_CONNECTIONS = 100;
const int NUM_THREADS = 8;

// Main thread multiplexing **refresh** timeout (see use case in main())
int MAIN_MULTIPLEX_FD = 0; // set in main() later

// Thread tcp duplex muiltiplexing
int THREAD_DUPLEX_FDS[8]; // set in main() later

int TELEMETRY_ENABLED = 0; // set in main()

char BAD_REQUEST[] = {
    'H', 'T', 'T', 'P', '/', '1', '.', '0', ' ',
    '4', '0', '0', ' ',
    'B', 'a', 'd', ' ', 'r', 'e', 'q', 'u', 'e', 's', 't',
    '\r', '\n', '\r', '\n'
};

char SSL_GREETING_HEADER[] = {
    'H', 'T', 'T', 'P', '/', '1', '.', '0', ' ',
    '2', '0', '0', ' ',
    'C', 'o', 'n', 'n', 'e', 'c', 't', 'i', 'o', 'n', ' ', 'e', 's', 't', 'a', 'b', 'l', 'i', 's', 'h', 'e', 'd',
    '\r', '\n', '\r', '\n'
};

const int BUFFER_SIZE = 16384;
char buffers[8][16384];

struct StreamInfo {
    timeval start_time;
    long byte_count;
    char* domain;
};
struct StreamInfo* stream_infos[FD_SETSIZE];

unsigned short socket_descriptors[FD_SETSIZE]; // contains the other socket, 0 (stdin dummy) otherwise

// Comment to disable
// #define DEBUG_MESSAGES 1



void print_telemetry(struct StreamInfo* info)
{
    if (TELEMETRY_ENABLED)
    {
        timeval end_time;
        gettimeofday(&end_time, NULL);
        double seconds = end_time.tv_sec - info->start_time.tv_sec;
        double microseconds_in_seconds = (((double)end_time.tv_usec) - ((double)info->start_time.tv_usec)) / 1000000.0;
        double time = seconds + microseconds_in_seconds;
        printf("Hostname: %s, Size: %ld bytes, Time: %0.3lf sec\n", info->domain, info->byte_count, time);
    }
}


// @param client_sock_fd should be a valid fd
void cleanup_client(int client_sock_fd)
{
    close(client_sock_fd);

    if (stream_infos[client_sock_fd] != NULL)
    {
        if (stream_infos[client_sock_fd]->domain != NULL)
        {
            free(stream_infos[client_sock_fd]->domain);
        }
        free(stream_infos[client_sock_fd]);
        stream_infos[client_sock_fd] = NULL;
    }

    int dest_sock = socket_descriptors[client_sock_fd];
    if (dest_sock != 0 /* uninitialised */)
    {
        close(dest_sock);
        socket_descriptors[dest_sock] = 0;
        epoll_ctl(MAIN_MULTIPLEX_FD, EPOLL_CTL_DEL, dest_sock, NULL);
    }

    socket_descriptors[client_sock_fd] = 0;
    epoll_ctl(MAIN_MULTIPLEX_FD, EPOLL_CTL_DEL, client_sock_fd, NULL);
}


void handle_errno()
{
    int thread_num = omp_get_thread_num();
    if (errno)
    {
        printf("(This might be expected) Thread %d error code %d\n%s\n", thread_num, errno, strerror(errno));
    }
#ifdef DEBUG_MESSAGES
    else
    {
        printf("Thread %d errno = 0 (no error)\n", thread_num);
    }
#endif
}


void cleanup_client_error(int client_sock_fd)
{
    handle_errno();
    write(client_sock_fd, BAD_REQUEST, sizeof(BAD_REQUEST));
    cleanup_client(client_sock_fd);
}


void cleanup_client_completed(int client_sock_fd)
{
#ifdef DEBUG_MESSAGES
    printf("Fds %d %d connection completed successfully, closing\n", client_sock_fd, socket_descriptors[client_sock_fd]);
#endif
    print_telemetry(stream_infos[client_sock_fd]);
    cleanup_client(client_sock_fd);
}


int parse_http_header(char* recv_buf, int num_bytes_read, int* port_num, char** domain)
{
#ifdef DEBUG_MESSAGES
    printf("Thread %d read %d bytes\n", omp_get_thread_num(), num_bytes_read);
    printf("-----------------------\n");
    printf("%.*s", num_bytes_read, recv_buf);
    printf("-----------------------\n");
#endif

    char* first_space = strchr(recv_buf, ' ');
    if (first_space == NULL)
    {
        return -1;
    }

    char* url_start = first_space + 1;

    *port_num = 443;

    int domain_len;

    for (int i = 0; i < num_bytes_read; i++)
    {
        if (url_start[i] == ' ' || url_start[i] == '/')
        {
            domain_len = i;
            break;
        }
        else if (url_start[i] == ':')
        {
            domain_len = i;
            *port_num = atoi(url_start + i + 1);
            break;
        }
    }

    *domain = (char *)malloc((domain_len + 1) * sizeof(char));
    memcpy(*domain, url_start, domain_len);
    (*domain)[domain_len] = '\0';
    return 0;
}


int forward(int fd_from, int fd_to, int client_fd, int thread_num)
{
#ifdef DEBUG_MESSAGES
    printf("Fds %d %d received fd event\n", fd_from, fd_to);
#endif

    int bytes_read = read(fd_from, buffers[thread_num], BUFFER_SIZE);
    if (bytes_read == -1)
    {
        printf("Thread %d Fds %d %d could not read data, closing\n", thread_num, fd_from, fd_to);
        cleanup_client_error(client_fd);
        return 1;
    }
    else if (bytes_read == 0)
    {
        cleanup_client_completed(client_fd);
        return 1;
    }

    stream_infos[client_fd]->byte_count += bytes_read;

#ifdef DEBUG_MESSAGES
    printf("Thread %d Fds %d %d read client fd event\n", thread_num, fd_from, fd_to);
#endif

    int write_result = write(fd_to, buffers[thread_num], bytes_read);
    if (write_result == -1)
    {
        printf("Thread %d Fds %d %d failed to forward data\n", thread_num, fd_from, fd_to);
        cleanup_client_error(client_fd);
        return 1;
    }

#ifdef DEBUG_MESSAGES
    printf("Thread %d Fds %d %d completed fd event %d %d bytes\n", thread_num, fd_from, fd_to, bytes_read, write_result);
#endif

    return 0;
}


// Forward and persist data between this pair of client / destination **for a while**
// Why not completely multiplex? -- context switch is expensive
// @param fd_one one of the client OR socket fds which received a fd event from select() multiplex
// @param whether to assume there is an even available for fd_one right away
void forward_socket_pair(int fd_one, int fd_two, int client_fd, int thread_num)
{
#ifdef DEBUG_MESSAGES
    // assert
    if (stream_infos[fd_one] == NULL && stream_infos[fd_two] == NULL)
    {
        printf("No stream infos\n");
        exit(1);
    }
    printf("Processing %d %d flow\n", fd_one, fd_two);
#endif

    struct epoll_event fd_one_epoll_ev;
    fd_one_epoll_ev.data.fd = fd_one;
    fd_one_epoll_ev.events = EPOLLIN;

    struct epoll_event fd_two_epoll_ev;
    fd_two_epoll_ev.data.fd = fd_two;
    fd_two_epoll_ev.events = EPOLLIN;
    if (epoll_ctl(THREAD_DUPLEX_FDS[thread_num], EPOLL_CTL_ADD, fd_one, &fd_one_epoll_ev) == -1
        ||  epoll_ctl(THREAD_DUPLEX_FDS[thread_num], EPOLL_CTL_ADD, fd_two, &fd_two_epoll_ev) == -1)
    {
        printf("Thread %d Fds %d %d failed to add fds to duplex e_fd\n", thread_num, fd_one, fd_two);
        cleanup_client_error(client_fd);
        return;
    }

    struct epoll_event waited_events[2];

    while (1)
    {
        int epoll_result = epoll_wait(THREAD_DUPLEX_FDS[thread_num], waited_events, 2, 300);
        if (epoll_result == -1)
        {
            printf("Thread %d Fds %d %d failed to multiplex\n", thread_num, fd_one, fd_two);
            handle_errno();
            break;
        }
        else if (epoll_result == 0)
        {
#ifdef DEBUG_MESSAGES
            printf("Thread %d Fds %d %d no data for now\n", thread_num, fd_one, fd_two);
#endif
            break;
        }


#ifdef DEBUG_MESSAGES
        printf("Thread %d Fds %d %d received %d multiplex event(s)\n", thread_num, fd_one, fd_two, epoll_result);
#endif

        for (int i = 0; i < epoll_result; i++)
        {
            if (waited_events[i].data.fd == fd_one)
            {
                // fd_one to fd_two
                if (forward(fd_one, fd_two, client_fd, thread_num))
                {
                    return;
                }
            }
            else if (waited_events[i].data.fd == fd_two)
            {
                // fd_two to fd_one
                if (forward(fd_two, fd_one, client_fd, thread_num))
                {
                    return;
                }
            }
        }
    }

    // Add back for listening, as the connection wasn't closed yet
    if (epoll_ctl(THREAD_DUPLEX_FDS[thread_num], EPOLL_CTL_DEL, fd_one, NULL) == -1
        || epoll_ctl(THREAD_DUPLEX_FDS[thread_num], EPOLL_CTL_DEL, fd_two, NULL) == -1)
    {
        printf("Thread %d Fds %d %d failed to delete fds from duplex e_fd\n", thread_num, fd_one, fd_two);
    }

    if (epoll_ctl(MAIN_MULTIPLEX_FD, EPOLL_CTL_ADD, fd_one, &fd_one_epoll_ev) == -1
        || epoll_ctl(MAIN_MULTIPLEX_FD, EPOLL_CTL_ADD, fd_two, &fd_two_epoll_ev) == -1)
    {
        printf("Thread %d Fds %d %d failed to add fds back\n", thread_num, fd_one, fd_two);
        return;
    }

#ifdef DEBUG_MESSAGES
    printf("Thread %d Fds %d %d session completed %s\n", thread_num, fd_one, fd_two, stream_infos[client_fd]->domain);
#endif
}


int in_blacklist(std::vector<std::string> blacklist, char* domain) {
    for (int i = 0; i < blacklist.size(); i++) {
        const char* blacklist_domain = blacklist.at(i).c_str();
        if(strstr(domain, blacklist_domain) != NULL) {
            // found, means its in blacklist
            return 1;
        }
    }
    return 0;
}


void handle_new_client(int client_sock_fd, std::vector<std::string> blacklist)
{
    struct StreamInfo* info = (StreamInfo *)malloc(sizeof(struct StreamInfo));
    stream_infos[client_sock_fd] = info;
    gettimeofday(&info->start_time, NULL);
    info->domain = NULL;
    info->byte_count = 0;

    int thread_num = omp_get_thread_num();

#ifdef DEBUG_MESSAGES
    printf("Thread %d Fds %d handling new request\n", thread_num, client_sock_fd);
#endif

    int num_bytes_read = read(client_sock_fd, buffers[thread_num], BUFFER_SIZE);
    if (num_bytes_read == -1)
    {
        printf("Thread %d Fds %d failed to read new request\n", thread_num, client_sock_fd);
        cleanup_client_error(client_sock_fd);
        return;
    }
    else if (num_bytes_read == 0)
    {
        printf("Thread %d Fds %d No bytes after accepting (likely connection terminated by client)\n", thread_num, client_sock_fd);
        cleanup_client_error(client_sock_fd);
        return;
    }

    info->byte_count += num_bytes_read;

    // ----------------------------------------------------
    // Parse the HTTP header

    int port_num;
    if (parse_http_header(buffers[thread_num], num_bytes_read, &port_num, &info->domain) == -1)
    {
        printf("Thread %d Fds %d Error parsing http header\n", thread_num, client_sock_fd);
        cleanup_client_error(client_sock_fd);
        return;
    }
    // ----------------------------------------------------

    // ----------------------------------------------------
    // Check Blacklist
    if (in_blacklist(blacklist, info->domain) == 1) {
        printf("Thread %d Fds %d blocked from blacklisted domain %s\n", thread_num, client_sock_fd, info->domain);
        cleanup_client_error(client_sock_fd);
        return;
    }
    // ----------------------------------------------------
    // "DNS query"

    // https://man7.org/linux/man-pages/man3/getaddrinfo.3.html
    struct addrinfo hints;
    bzero(&hints, sizeof(hints)); // erase everything
    hints.ai_family = AF_INET; // TO ASK: do we need IPV6? (AF_UNSPEC if so)
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

#ifdef DEBUG_MESSAGES
    printf("Thread %d Fds %d attempting domain %s port %d\n", thread_num, client_sock_fd, info->domain, port_num);
#endif

    struct addrinfo *dest_sock_result = NULL;
    int addr_info_result = getaddrinfo(info->domain, NULL, &hints, &dest_sock_result);
    if (addr_info_result == -1)
    {
        printf("Thread %d Fds %d failed to resolve %s\n", thread_num, client_sock_fd, info->domain);
        cleanup_client_error(client_sock_fd);
        return;
    }
    else if (dest_sock_result == NULL)
    {
        // no addrinfo fits the hints
        printf("Thread %d Fds %d failed to resolve hints (might be expected, try dns-ing it) %s\n", thread_num, client_sock_fd, info->domain);
        cleanup_client_error(client_sock_fd);
        return;
    }

#ifdef DEBUG_MESSAGES
    printf("Thread %d Fds %d attempting ai_family %d\n", thread_num, client_sock_fd, dest_sock_result->ai_family);
    char ip[(dest_sock_result->ai_family == AF_INET) ? INET_ADDRSTRLEN : INET6_ADDRSTRLEN];
    inet_ntop(
        dest_sock_result->ai_family,
        (dest_sock_result->ai_family == AF_INET)
            ? &(((struct sockaddr_in*)dest_sock_result->ai_addr)->sin_addr)
            : &(((struct sockaddr_in6*)dest_sock_result->ai_addr)->sin6_addr),
        ip, (dest_sock_result->ai_family == AF_INET) ? INET_ADDRSTRLEN : INET6_ADDRSTRLEN
    );
    printf("Thread %d Fds %d attempting ip %s\n", thread_num, client_sock_fd, ip);
#endif

    // Add on the port
    if ((dest_sock_result -> ai_family) == AF_INET) // ipv4
    {
        ((struct sockaddr_in*)dest_sock_result->ai_addr)->sin_port = htons((unsigned short)port_num);
    }
    else // ipv6
    {
        ((struct sockaddr_in6*)dest_sock_result->ai_addr)->sin6_port = htons((unsigned short)port_num);
    }

    // ----------------------------------------------------
    // Create "Destination socket" (proxy - webserver)

    int dest_sock_fd = socket(dest_sock_result->ai_family, SOCK_STREAM, 0);
    if (dest_sock_fd == -1)
    {
        printf("Thread %d Fds %d failed to open dest socket\n", thread_num, client_sock_fd);
        cleanup_client_error(client_sock_fd);
        return;
    }

    socket_descriptors[client_sock_fd] = dest_sock_fd;
    socket_descriptors[dest_sock_fd] = client_sock_fd;

    int connect_result = connect(dest_sock_fd, dest_sock_result->ai_addr, dest_sock_result->ai_addrlen);
    if (connect_result != 0)
    {
        printf("Thread %d Fds %d %d failed to open tcp connection to destination %s\n", thread_num, client_sock_fd, dest_sock_fd, info->domain);
        cleanup_client_error(client_sock_fd);
        return;
    }

    // freeaddrinfo(dest_sock_result);

    // ----------------------------------------------------
    // SSL greeting to client

#ifdef DEBUG_MESSAGES
    printf("Thread %d Fds %d %d connected ip %s\n", thread_num, client_sock_fd, dest_sock_fd, ip);
#endif

    if (write(client_sock_fd, SSL_GREETING_HEADER, sizeof(SSL_GREETING_HEADER)) == -1)
    {
        printf("Thread %d Fds %d %d failed to send SSL greeting to client\n", thread_num, client_sock_fd, dest_sock_fd);
        cleanup_client_error(client_sock_fd);
        return;
    }

#ifdef DEBUG_MESSAGES
    printf("Thread %d Fds %d %d sent SSL greeting to client\n", thread_num, client_sock_fd, dest_sock_fd);
#endif

    // ----------------------------------------------------

    forward_socket_pair(client_sock_fd, dest_sock_fd, client_sock_fd, thread_num);
}


std::vector<std::string> init_blacklist(FILE* blacklist_fd) {
    std::vector<std::string> blacklist;
    char buffer[10000];
    if (blacklist_fd != NULL) {
        while (fgets(buffer, sizeof(buffer), blacklist_fd)) {
#ifdef DEBUG_MESSAGES
                printf("Blacklisted: %s\n", buffer);
#endif
            blacklist.push_back(buffer);
        }
    }
    return blacklist;
}


int main(int argc, char *argv[])
{
    if (argc == 0)
    {
        printf("No port number specified!\n");
        return -1;
    }
    
    int port_number = atoi(argv[1]);

#ifdef DEBUG_MESSAGES
    printf("Port number %d\n", port_number);
#endif

    TELEMETRY_ENABLED = atoi(argv[2]);

    FILE* blacklist_fd = fopen(argv[3], "r");
    std::vector<std::string> blacklist = init_blacklist(blacklist_fd);

#ifdef DEBUG_MESSAGES
    printf("Size of blacklist %ld\n", blacklist.size());
#endif

    // Listen socket
    int listen_socket_fd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in listen_socket_addr;
    bzero(&listen_socket_addr, sizeof(listen_socket_addr));
    listen_socket_addr.sin_family = AF_INET;
    listen_socket_addr.sin_port = htons((unsigned short)port_number);
    listen_socket_addr.sin_addr.s_addr = INADDR_ANY;

    bind(listen_socket_fd, (struct sockaddr*)&listen_socket_addr, sizeof(listen_socket_addr));

    if (listen(listen_socket_fd, NUM_MAX_QUEUED_CONNECTIONS) != 0)
    {
        printf("Failed to listen on port %d\n", port_number);
        exit(1);
    }

    // Ignore SIGPIPE that can occur when trying to write 400 Bad request response,
    // As a broken pipe may (e.g. if the client pre-emptively closed the connection) be an expected part of our error handling
    signal(SIGPIPE, SIG_IGN);

    // OpenMP has a thread pool, this sets the number of threads there.
    omp_set_num_threads(NUM_THREADS);
    omp_set_dynamic(0); // prevent dynamic adjustment of the amount of threads (OpenMP reduces it sometimes)


    for (int i = 0; i < FD_SETSIZE; i++)
    {
        socket_descriptors[i] = 0;
        stream_infos[i] = NULL;
    }

    MAIN_MULTIPLEX_FD = epoll_create(1); // 1 means nothing, obsolete parameter - see https://man7.org/linux/man-pages/man2/epoll_create.2.html
    if (MAIN_MULTIPLEX_FD == -1)
    {
        printf("Failed to create MAIN_MULTIPLEX_FD e_fd\n");
        exit(1);
    }

    for (int i = 0; i < NUM_THREADS; i++)
    {
        THREAD_DUPLEX_FDS[i] = epoll_create(1);
        if (THREAD_DUPLEX_FDS[i] == -1)
        {
            printf("Failed to create %d thread e_fd\n", i);
            exit(1);
        }
    }

    struct epoll_event epoll_ev;
    epoll_ev.data.fd = listen_socket_fd;
    epoll_ev.events = EPOLLIN;
    epoll_ctl(MAIN_MULTIPLEX_FD, EPOLL_CTL_ADD, listen_socket_fd, &epoll_ev);

    struct epoll_event waited_events[NUM_THREADS];

    // This directive defines a **parallel region** in which other OpenMP directives may be placed
    // May be ignored.
    #pragma omp parallel
    {
        // The while loop is executed by the master thread
        #pragma omp master
        while (1)
        {
            // -------------------------------------

            int epoll_result = epoll_wait(MAIN_MULTIPLEX_FD, waited_events, NUM_THREADS, 1000);
            if (epoll_result == -1)
            {
                printf("Main loop failed to multiplex\n");
                break;
            }
            else if (epoll_result == 0)
            {
                continue; // continue refreshing fd set
            }
            // -------------------------------------

            for (int ev_idx = 0; ev_idx < epoll_result; ev_idx++)
            {
                // -------------------------------------
                // New connection
                if (waited_events[ev_idx].data.fd == listen_socket_fd)
                {

    #ifdef DEBUG_MESSAGES
                    printf("Main thread accepting new connection\n");
    #endif

                    struct sockaddr_in client_sock_addr;
                    socklen_t len = sizeof(client_sock_addr);
                    bzero(&client_sock_addr, len);
                    int client_sock_fd = accept(listen_socket_fd, (struct sockaddr*)&client_sock_addr, &len);
                    if (client_sock_fd == -1)
                    {
                        printf("Main thread error-ed in @accept call\n");
                        handle_errno();
                        break;
                    }

    #ifdef DEBUG_MESSAGES
                    char client_ip[INET_ADDRSTRLEN];
                    inet_ntop(client_sock_addr.sin_family, &client_sock_addr.sin_addr.s_addr, client_ip, INET_ADDRSTRLEN);
                    printf("Main thread accepted new connection from %s\n", client_ip);
    #endif

                    // This function / task goes to the OpenMP threadpool
                    #pragma omp task
                    handle_new_client(client_sock_fd, blacklist);
                }
                // -------------------------------------
                // -------------------------------------
                // Existing connections, new data
                else
                {
                    int event_fd = waited_events[ev_idx].data.fd;

                    // Clear both sockets
                    if (epoll_ctl(MAIN_MULTIPLEX_FD, EPOLL_CTL_DEL, event_fd, NULL) == -1
                        // Clear the other socket as it will be handled in @forward_socket_pair if needed
                        || epoll_ctl(MAIN_MULTIPLEX_FD, EPOLL_CTL_DEL, socket_descriptors[event_fd], NULL) == -1)
                    {
                        printf("Main thread failed to clear fds %d %d\n", event_fd, socket_descriptors[event_fd]);
                        continue; // retry
                    }

                    // This block goes to the OpenMP threadpool
                    #pragma omp task
                    {
                        int client_fd = stream_infos[event_fd] == NULL
                            ? socket_descriptors[event_fd]
                            : event_fd;
                        int thread_num = omp_get_thread_num();
                        if (forward(event_fd, socket_descriptors[event_fd], client_fd, thread_num) == 0)
                        {
                            forward_socket_pair(event_fd, socket_descriptors[event_fd], client_fd, thread_num);
                        }
                    }
                }
                // -------------------------------------
            }
        }
    }
}
