PORT = 8080
TELEMETRY = 1

build:
	g++ -fopenmp *.cpp -o proxy

build-debug:
	g++ -O2 -g -fopenmp *.cpp -o proxy
	gdb proxy

build-opt:
	g++ -O2 -fopenmp *.cpp -o proxy

run:
	./proxy $(PORT) $(TELEMETRY) blacklist.txt
