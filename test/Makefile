LAST_CC=g++
CC=g++
CXX=g++
COMMON_FLAGS=-O3

CC_FLAGS=${COMMON_FLAGS}

LDFLAGS = ${CC_FLAGS}
LDLIBS  =-lm
INCLUDES =

C_SRC=

CPP_SRC=test.cpp

OBJS =
OBJS += $(C_SRC:%.c=%.o)
OBJS += $(CPP_SRC:%.cpp=%.o)

.SUFFIXES: .o

.PHONY: all
all:test

test: $(OBJS)
	$(LAST_CC) -o $@ $(OBJS) $(LDFLAGS) $(LDLIBS)

.c.o:
	$(CC) -c $(CC_FLAGS) $< -o $@  $(INCLUDES)

.cpp.o:
	$(CXX) -c $(CC_FLAGS) $< -o $@  $(INCLUDES)



.PHONY: clean
clean:
	rm -f *.o
	rm -f *~
