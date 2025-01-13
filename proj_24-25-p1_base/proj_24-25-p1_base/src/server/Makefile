CC = gcc

# Para mais informações sobre as flags de warning, consulte a informação adicional no lab_ferramentas
CFLAGS = -g -std=c17 -D_POSIX_C_SOURCE=200809L \
		 -Wall -Werror -Wextra \
		 -Wcast-align -Wfloat-equal -Wformat=2 -Wnull-dereference -Wshadow -Wsign-conversion -Wswitch-enum -Wundef -Wunreachable-code -Wunused\

		 
LDFLAGS = -lpthread

ifneq ($(shell uname -s),Darwin) # if not MacOS
	CFLAGS += -fmax-errors=5
endif

all: kvs

kvs: main.c constants.h operations.o parser.o kvs.o io.o ../common/constants.h ../common/protocol.h
	$(CC) $(CFLAGS) $(SLEEP) -o kvs main.c constants.h operations.o parser.o kvs.o io.o ../common/io.c ../common/constants.h ../common/protocol.h $(LDFLAGS)

%.o: %.c %.h
	$(CC) $(CFLAGS) -c ${@:.o=.c}

run: kvs
	@./kvs

clean:
	rm -f *.o kvs jobs/*.out jobs/*.bck

format:
	@which clang-format >/dev/null 2>&1 || echo "Please install clang-format to run this command"
	clang-format -i *.c *.h
