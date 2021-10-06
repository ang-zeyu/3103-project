PORT = 8080
TELEMETRY = 1

build:
	gcc -fopenmp *.c -o main

build-debug:
	gcc -O2 -g -fopenmp *.c -o main
	gdb main

build-opt:
	gcc -O2 -fopenmp *.c -o main

run:
	./main $(PORT) $(TELEMETRY) blacklist.txt
