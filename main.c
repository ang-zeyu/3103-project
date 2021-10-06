#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <omp.h>
#include <time.h>

const int NUM_MAX_QUEUED_CONNECTIONS = 100;
const int NUM_THREADS = 8;

// Main thread multiplexing **refresh** timeout (see use case in main())
struct timespec MAIN_MULTIPLEX_TIMEOUT = {
    tv_sec: 1,
    tv_nsec: 0,
};

// Not TCP connection timeout!
// See use in forward_socket_pair
struct timespec SOCKET_PAIR_TIMEOUT = {
    tv_sec: 0,
    tv_nsec: 300000000, // 0.3s
};


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

char* BLACKLIST = NULL;

struct StreamInfo {
    clock_t start_time;
    long byte_count;
    char* domain;
};
struct StreamInfo* stream_infos[FD_SETSIZE];

unsigned short socket_descriptors[FD_SETSIZE]; // contains the other socket, 0 (stdin dummy) otherwise
unsigned char is_socket_processing[FD_SETSIZE]; // 0 - nothing, 1 - a thread is processing it

// Comment to disable
// #define DEBUG_MESSAGES 1



void print_telemetry(struct StreamInfo* info)
{
    if (TELEMETRY_ENABLED)
    {
        float time = ((float)(clock() - info->start_time)) / CLOCKS_PER_SEC;
        printf("Hostname: %s, Size: %ld bytes, Time: %0.3f sec\n", info->domain, info->byte_count, time);
    }
}


// @param client_sock_fd should be a valid fd
void cleanup_client(int client_sock_fd)
{
    close(client_sock_fd);

    if (stream_infos[client_sock_fd] != NULL)
    {
        free(stream_infos[client_sock_fd]->domain);
        free(stream_infos[client_sock_fd]);
        stream_infos[client_sock_fd] = NULL;
    }

    int dest_sock = socket_descriptors[client_sock_fd];
    if (dest_sock != 0 /* uninitialised */)
    {
        close(dest_sock);
        socket_descriptors[dest_sock] = 0;
        is_socket_processing[dest_sock] = 0;
    }

    socket_descriptors[client_sock_fd] = 0;
    is_socket_processing[client_sock_fd] = 0;
}


void handle_errno()
{
    int thread_num = omp_get_thread_num();
    printf("Thread %d error code %d\n", thread_num, errno);
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

    *domain = malloc((domain_len + 1) * sizeof(char));
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

    fd_set both_sockets;
    while (1)
    {
        FD_ZERO(&both_sockets);
        FD_SET(fd_one, &both_sockets);
        FD_SET(fd_two, &both_sockets);

        int select_result = pselect(FD_SETSIZE, &both_sockets, NULL, NULL, &SOCKET_PAIR_TIMEOUT, NULL);
        if (select_result == -1)
        {
            printf("Thread %d Fds %d %d failed to multiplex\n", thread_num, fd_one, fd_two);
            handle_errno();
            break;
        }
        else if (select_result == 0)
        {
#ifdef DEBUG_MESSAGES
            printf("Thread %d Fds %d %d no data for now\n", hread_num, fd_one, fd_two);
#endif
            break;
        }


#ifdef DEBUG_MESSAGES
        printf("Thread %d Fds %d %d received %d multiplex event(s)\n", thread_num, fd_one, fd_two, select_result);
#endif

        // fd_one -> fd_two
        if (FD_ISSET(fd_one, &both_sockets))
        {
            if (forward(fd_one, fd_two, client_fd, thread_num))
            {
                return;
            }
        }

        // fd_two -> fd_one
        if (FD_ISSET(fd_two, &both_sockets))
        {
            if (forward(fd_two, fd_one, client_fd, thread_num))
            {
                return;
            }
        }
    }

    is_socket_processing[fd_one] = 0;
    is_socket_processing[fd_two] = 0;

#ifdef DEBUG_MESSAGES
    printf("Thread %d Fds %d %d session completed %s\n", thread_num, fd_one, fd_two, stream_infos[client_fd]->domain);
#endif
}


void handle_new_client(int client_sock_fd)
{
    struct StreamInfo* info = malloc(sizeof(struct StreamInfo));
    stream_infos[client_sock_fd] = info;

    info->start_time = clock();
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

    // These 4 writes must be in this order: is_socket_processing then socket_descriptors
    is_socket_processing[client_sock_fd] = 1;
    is_socket_processing[dest_sock_fd] = 1;
    asm volatile("" ::: "memory"); // sequential consistency without atomics / cs
    socket_descriptors[client_sock_fd] = dest_sock_fd;
    socket_descriptors[dest_sock_fd] = client_sock_fd;

    int connect_result = connect(dest_sock_fd, dest_sock_result->ai_addr, dest_sock_result->ai_addrlen);
    if (connect_result != 0)
    {
        printf("Thread %d Fds %d %d failed to open tcp connection to destination\n", thread_num, client_sock_fd, dest_sock_fd);
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
    // TODO

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


    // OpenMP has a thread pool, this sets the number of threads there.
    omp_set_num_threads(NUM_THREADS);


    for (int i = 0; i < FD_SETSIZE; i++)
    {
        socket_descriptors[i] = 0;
        is_socket_processing[i] = 0;
        stream_infos[i] = NULL;
    }
    fd_set all_sockets;

    // This directive defines a **parallel region** in which other OpenMP directives may be placed
    // May be ignored.
    #pragma omp parallel
    {
        // The while loop is executed by the master thread
        #pragma omp master
        while (1)
        {
            // -------------------------------------
            // Refresh fd set
            FD_ZERO(&all_sockets);
            FD_SET(listen_socket_fd, &all_sockets);
            for (int i = 0; i < FD_SETSIZE; i++)
            {
                if (socket_descriptors[i] && is_socket_processing[i] == 0)
                {
                    FD_SET(i, &all_sockets);
                }
            }
            // -------------------------------------


            // -------------------------------------
            int select_result = pselect(FD_SETSIZE, &all_sockets, NULL, NULL, &MAIN_MULTIPLEX_TIMEOUT, NULL);
            if (select_result == -1)
            {
                printf("main loop failed to multiplex\n");
                break;
            }
            else if (select_result == 0)
            {
                continue; // continue refreshing fd set
            }
            // -------------------------------------


            // -------------------------------------
            // New connection
            if (FD_ISSET(listen_socket_fd, &all_sockets))
            {

#ifdef DEBUG_MESSAGES
                printf("Main thread accepting new request\n");
#endif

                struct sockaddr_in client_sock_addr;
                int len = sizeof(client_sock_addr);
                bzero(&client_sock_addr, len);
                int client_sock_fd = accept(listen_socket_fd, (struct sockaddr*)&client_sock_addr, &len);
                if (client_sock_fd == -1)
                {
                    printf("Main thread error-ed in @accept call\n");
                    handle_errno();
                    break;
                }

#ifdef DEBUG_MESSAGES
                char client_ip[20];
                inet_ntop(client_sock_addr.sin_family, &client_sock_addr.sin_addr.s_addr, client_ip, 20);
                printf("Main thread accepted new request from %s\n", client_ip);
#endif

                // This function / task goes to the OpenMP threadpool
                #pragma omp task
                handle_new_client(client_sock_fd);
            }
            // -------------------------------------


            // -------------------------------------
            // Existing connections, new data
            for (int i = 0; i < FD_SETSIZE; i++)
            {
                if (i != listen_socket_fd && FD_ISSET(i, &all_sockets))
                {
                    // clear the other socket, will be handled in @forward_socket_pair if needed
                    FD_CLR(socket_descriptors[i], &all_sockets);

                    is_socket_processing[i] = 1;
                    is_socket_processing[socket_descriptors[i]] = 1;

                    // This block goes to the OpenMP threadpool
                    #pragma omp task
                    {
                        int client_fd = stream_infos[i] == NULL ? socket_descriptors[i] : i;
                        int thread_num = omp_get_thread_num();
                        if (forward(i, socket_descriptors[i], client_fd, thread_num) == 0)
                        {
                            forward_socket_pair(i, socket_descriptors[i], client_fd, thread_num);
                        }
                    }
                }
            }
            // -------------------------------------
        }
    }
}
