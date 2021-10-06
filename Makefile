PORT = 8080
TELEMETRY = 1

build:
	gcc -fopenmp *.c -o proxy

build-debug:
	gcc -O2 -g -fopenmp *.c -o proxy
	gdb proxy

build-opt:
	gcc -O2 -fopenmp *.c -o proxy

run:
	./proxy $(PORT) $(TELEMETRY) blacklist.txt
