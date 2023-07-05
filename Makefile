
VPATH = libnet
CFLAGS += -g -Ilibnet
CORE := net.c util.c hash.c

raft-server: raft-server.c $(CORE)
	gcc $(CFLAGS) $^ -o bin/$@

raft-client: raft-client.c $(CORE)
	gcc $(CFLAGS) $^ -o bin/$@

raft-admin: raft-admin.c $(CORE)
	gcc $(CFLAGS) $^ -o bin/$@

raft-log: raft-log.c $(CORE)
	gcc $(CFLAGS) $^ -o bin/$@

clean:
	rm -f bin/*

.PHONY: clean
