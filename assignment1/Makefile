PORT = 8080
TELEMETRY = 1
BLACKLIST = blacklist.txt

build:
	g++ -O2 -fopenmp *.cpp -o proxy

build-debug:
	g++ -O2 -g -fopenmp *.cpp -o proxy
	gdb proxy

run:
	./proxy $(PORT) $(TELEMETRY) $(BLACKLIST)
