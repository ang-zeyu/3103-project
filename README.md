# 3103 Project

## Group Members

-   Ang Ze Yu (A0187094U)
-   Choo Xing Yu (A0202132E)

## Compiling the Proxy

A makefile is provided in our submission.

Simply run the following command:

```
make build
```

This script uses the `g++` command which we have verified to be available on `xcne`.

It also uses the `-O2` optimization flag.

Lastly, `-fopenmp` flag (verified to be working on `xcne` machines as well) is specified to allow us to use the **multithreading library** OpenMP.
OpenMP is a multithreading library which's usage is discussed in our document writeup.

## Running the Proxy

Simply run the command specified in the assignment pdf.

```
./proxy <port> <flag_telemetry> <filename of blacklist>
```

All 3 arguments are required. If testing with no blacklist urls, you may simply use an empty text file for the blacklist file.
