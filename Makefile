
VPATH = libnet src
CFLAGS += -g -Ilibnet
CORE := net.c util.c hash.c

raft-server: raft-server.c raft-log.c $(CORE)
	gcc $(CFLAGS) $^ -o bin/$@

raft-client: raft-cli-client.c $(CORE)
	gcc $(CFLAGS) $^ -o bin/$@

raft-admin: raft-cli-admin.c $(CORE)
	gcc $(CFLAGS) $^ -o bin/$@

raft-log: raft-cli-log.c $(CORE)
	gcc $(CFLAGS) $^ -o bin/$@

raft-snapshot: raft-cli-snapshot.c $(CORE)
	gcc $(CFLAGS) $^ -o bin/$@

clean:
	rm -f bin/*

.PHONY: clean
