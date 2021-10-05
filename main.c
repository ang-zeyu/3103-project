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
const int BUFFER_SIZE = 16384;
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

char* BLACKLIST = NULL;

#define DEBUG_MESSAGES 1


void cleanup(int client_sock_fd)
{
    write(client_sock_fd, BAD_REQUEST, sizeof(BAD_REQUEST));
    close(client_sock_fd);
}

int parse_http_header(
    char* recv_buf, int num_bytes_read,                    // inputs
    char** url_start_outer, int* domain_len, int* port_num // inputs / outputs
)
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

    for (int i = 0; i < num_bytes_read; i++)
    {
        if (url_start[i] == ' ' || url_start[i] == '/')
        {
            *domain_len = i + 1;
            break;
        }
        else if (url_start[i] == ':')
        {
            *domain_len = i;
            *port_num = atoi(url_start + i + 1);
            break;
        }
    }

    *url_start_outer = url_start;

    return 0;
}

void handle_errno(int thread_num)
{
    switch(errno) {
        case ECONNREFUSED:
            printf("ECONNREFUSED %d\n", thread_num);
            break;
        case EHOSTUNREACH:
            printf("EHOSTUNREACH %d\n", thread_num);
            break;
        case ENETUNREACH:
            printf("ENETUNREACH %d\n", thread_num);
            break;
        case EHOSTDOWN:
            printf("EHOSTDOWN %d\n", thread_num);
            break;
        case ENETDOWN:
            printf("ENETDOWN %d\n", thread_num);
            break;
        case ETIMEDOUT:
            printf("ETIMEDOUT %d\n", thread_num);
            break;
        default:
            printf("Thread %d failed to write, unknown error\n", thread_num);
    }
}

void print_telemetry(char* hostname, long size, float time)
{
    printf("Hostname: %s, Size: %ld bytes, Time: %0.3f sec\n", hostname, size, time);
}

void handle_client(int client_sock_fd)
{
    clock_t start = clock();
    long total_bytes = 0;

    int thread_num = omp_get_thread_num();

#ifdef DEBUG_MESSAGES
    printf("Thread %d handling new request\n", thread_num);
#endif

    char recv_buf[BUFFER_SIZE];
    int num_bytes_read = read(client_sock_fd, &recv_buf, sizeof(recv_buf));
    if (num_bytes_read == -1)
    {
        printf("Thread %d failed to read new request\n", thread_num);
        cleanup(client_sock_fd);
        return;
    }
    else if (num_bytes_read == 0)
    {
        cleanup(client_sock_fd);
        return;
    }

    total_bytes += num_bytes_read;

    // ----------------------------------------------------
    // Parse the HTTP header

    int domain_len, port_num;
    char* url_start;
    if (parse_http_header(recv_buf, num_bytes_read, &url_start, &domain_len, &port_num) == -1)
    {
        printf("Error parsing http header %d\n", thread_num);
        cleanup(client_sock_fd);
        return;
    }

    char domain[domain_len + 1];
    strncpy(domain, url_start, domain_len);
    domain[domain_len] = '\0';


    // ----------------------------------------------------
    // "DNS query"

    // https://man7.org/linux/man-pages/man3/getaddrinfo.3.html
    struct addrinfo hints;
    bzero(&hints, sizeof(hints)); // erase everything
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

#ifdef DEBUG_MESSAGES
    printf("Thread %d attempting domain %s port %d\n", thread_num, domain, port_num);
#endif

    struct addrinfo *dest_sock_result;
    int addr_info_result = getaddrinfo(domain, NULL, &hints, &dest_sock_result);
    if (addr_info_result == -1)
    {
        printf("Thread %d failed to resolve %s\n", thread_num, domain);
        cleanup(client_sock_fd);
        return;
    }

#ifdef DEBUG_MESSAGES
    char ip[INET_ADDRSTRLEN];
    inet_ntop(
        dest_sock_result->ai_family,
        &dest_sock_result->ai_addr->sa_data[2],
        ip, INET_ADDRSTRLEN
    );
    printf("Thread %d attempting ip %s\n", thread_num, ip);
#endif

    // ----------------------------------------------------
    // Destination

    ((struct sockaddr_in*)dest_sock_result->ai_addr)->sin_port = htons((unsigned short)port_num);

    /* struct sockaddr_in dest_sock_addr;
    bzero(&dest_sock_addr, sizeof(dest_sock_addr));
    dest_sock_addr.sin_family = AF_INET;
    inet_pton(AF_INET, ip, &dest_sock_addr.sin_addr.s_addr);   // pton converts text to binary, sets it in dst
    dest_sock_addr.sin_port = htons((unsigned short)port_num);
 */
    // ----------------------------------------------------
    // "Client socket" (proxy - webserver)
    int dest_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (dest_sock_fd == -1)
    {
        printf("Thread %d failed to open client socket\n", thread_num);
        cleanup(client_sock_fd);
        return;
    }

    int connect_result = connect(dest_sock_fd, dest_sock_result->ai_addr, sizeof(*dest_sock_result->ai_addr));
    if (connect_result != 0)
    {
        printf("Thread %d failed to open tcp connection to destination\n", thread_num);
        cleanup(client_sock_fd);
        close(dest_sock_fd);
        return;
    }

    freeaddrinfo(dest_sock_result);

    // ----------------------------------------------------
    // SSL greeting to client

#ifdef DEBUG_MESSAGES
    printf("Thread %d connected ip %s\n", thread_num, ip);
#endif

    if (write(client_sock_fd, SSL_GREETING_HEADER, sizeof(SSL_GREETING_HEADER)) == -1)
    {
        cleanup(client_sock_fd);
        return;
    }

#ifdef DEBUG_MESSAGES
    printf("Thread %d sent SSL greeting to client\n", thread_num);
#endif

    // ----------------------------------------------------
    
    /* int first_write_result = write(dest_sock_fd, recv_buf, num_bytes_read);
    if (first_write_result == -1)
    {
        printf("Thread %d failed to write\n", thread_num);
        cleanup(client_sock_fd);
        close(dest_sock_fd);
        return;
    } */

#ifdef DEBUG_MESSAGES
    printf("Thread %d sent first message to dest %s\n", thread_num, ip);
#endif

    // ----------------------------------------------------
    // Main proxy loop
    // Forward stuff back and forth client & server

    // Multiplexing here as tcp is duplex.
    fd_set both_sockets;
    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;

    while (1)
    {
        FD_ZERO(&both_sockets);
        FD_SET(client_sock_fd, &both_sockets);
        FD_SET(dest_sock_fd, &both_sockets);

        int select_result = select(FD_SETSIZE, &both_sockets, NULL, NULL, &timeout);
        if (select_result == -1)
        {
            printf("Thread %d failed to multiplex\n", thread_num);
            break;
        }
        else if (select_result == 0)
        {
            printf("Thread %d timed out\n", thread_num);
            break;
        }


#ifdef DEBUG_MESSAGES
        printf("Thread %d received %d multiplex event(s)\n", thread_num, select_result);
#endif

        if (FD_ISSET(client_sock_fd, &both_sockets))
        {
#ifdef DEBUG_MESSAGES
            printf("Thread %d received client fd event\n", thread_num);
#endif

            int read_result = read(client_sock_fd, recv_buf, sizeof(recv_buf));
            if (read_result == -1)
            {
                printf("Thread %d could not read data from client, closing\n", thread_num);
                break;
            }
            else if (read_result == 0)
            {
                printf("Thread %d connection closing\n", thread_num);
                break;
            }

            total_bytes += read_result;

            //printf("port %d\n", (int)recv_buf[0]);

            int write_result = write(dest_sock_fd, recv_buf, read_result);
            if (write_result == -1)
            {
                printf("Thread %d failed to forward data to dest\n", thread_num);
                break;
            }


#ifdef DEBUG_MESSAGES
            printf("Thread %d completed client fd event %d %d bytes\n", thread_num, read_result, write_result);
#endif
        }

        if (FD_ISSET(dest_sock_fd, &both_sockets))
        {
#ifdef DEBUG_MESSAGES
            printf("Thread %d received dest fd event\n", thread_num);
#endif


            int read_result = read(dest_sock_fd, recv_buf, sizeof(recv_buf));
            if (read_result == -1)
            {
                printf("Thread %d could not read data from dest, closing\n", thread_num);
                break;
            }
            else if (read_result == 0)
            {
                printf("Thread %d connection closing\n", thread_num);
                break;
            }

            total_bytes += read_result;

            int write_result = write(client_sock_fd, recv_buf, read_result);
            if (write_result == -1)
            {
                printf("Thread %d failed to forward data to client\n", thread_num);
                break;
            }

#ifdef DEBUG_MESSAGES
            printf("Thread %d completed dest fd event %d %d bytes\n", thread_num, read_result, write_result);
#endif
        }
    }

#ifdef DEBUG_MESSAGES
    printf("Thread %d completed domain %s port %d\n", thread_num, domain, port_num);
#endif

    if (TELEMETRY_ENABLED)
    {
        float time = ((float)(clock() - start)) / CLOCKS_PER_SEC;
        print_telemetry(domain, total_bytes, time);
    }

    close(client_sock_fd);
    close(dest_sock_fd);
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

    int len;

    // This directive defines a **parallel region** in which other OpenMP directives may be placed
    // May be ignored.
    #pragma omp parallel
    {
        // The while loop is executed by the master thread
        #pragma omp master
        while (1)
        {
#ifdef DEBUG_MESSAGES
            printf("Main thread accepting new request\n");
#endif
            struct sockaddr_in client_sock_addr;
            int client_sock_fd = accept(listen_socket_fd, (struct sockaddr*)&client_sock_addr, &len);
            if (client_sock_fd == -1)
            {
                printf("Main thread error-ed in @accept call\n");
                exit(1);
            }

#ifdef DEBUG_MESSAGES
            char client_ip[20];
            inet_ntop(client_sock_addr.sin_family, &client_sock_addr.sin_addr.s_addr, client_ip, 20);
            printf("Main thread accepted new request from %s\n", client_ip);
#endif

            // This function / task goes to the OpenMP threadpool
            #pragma omp task
            handle_client(client_sock_fd);
        }
    }
}
