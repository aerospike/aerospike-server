HOST :=			$(CXX)
CROSS :=		$(CC)

INCL :=			$(shell $(CROSS) -print-file-name=plugin)/include
FLAGS :=		-std=c++17 -Os -gdwarf-4 -I$(INCL) -Wall -Wextra \
					-fno-strict-aliasing -fno-inline -fno-omit-frame-pointer \
					-fno-rtti -fno-exceptions

all:			tso.so

tso.so:			tso.cc
				$(HOST) $(FLAGS) -fPIC -shared -o tso.so tso.cc

dump:			tso.so test.c
				$(CROSS) -std=c99 -O2 -Wall -Wextra -fdump-tree-all \
					-gdwarf-4 -fno-strict-aliasing \
					-fplugin=./tso.so \
					-fplugin-arg-tso-enable=yes \
					-fplugin-arg-tso-exclude=exclude_ce.txt \
					-fplugin-arg-tso-track-deps=yes \
					-fplugin-arg-tso-fix-asm=yes \
					-fplugin-arg-tso-fix-built-in=yes \
					-fplugin-arg-tso-profiling=no \
					-o test test.c

test:			tso.so test.c
				$(CROSS) -std=c99 -O2 -Wall -Wextra \
					-gdwarf-4 -fno-strict-aliasing \
					-fplugin=./tso.so \
					-fplugin-arg-tso-enable=yes \
					-fplugin-arg-tso-exclude=exclude_ce.txt \
					-fplugin-arg-tso-track-deps=yes \
					-fplugin-arg-tso-fix-asm=yes \
					-fplugin-arg-tso-fix-built-in=yes \
					-fplugin-arg-tso-profiling=no \
					-o test test.c

clean:
				rm -f tso.so test test.c.*
