# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.21

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/local/bin/cmake

# The command to remove a file.
RM = /usr/local/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/luo/rucbase/RUCbase

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/luo/rucbase/RUCbase/deps

# Include any dependencies generated for this target.
include src/record/CMakeFiles/rm_gtest.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include src/record/CMakeFiles/rm_gtest.dir/compiler_depend.make

# Include the progress variables for this target.
include src/record/CMakeFiles/rm_gtest.dir/progress.make

# Include the compile flags for this target's objects.
include src/record/CMakeFiles/rm_gtest.dir/flags.make

src/record/CMakeFiles/rm_gtest.dir/rm_gtest.cpp.o: src/record/CMakeFiles/rm_gtest.dir/flags.make
src/record/CMakeFiles/rm_gtest.dir/rm_gtest.cpp.o: ../src/record/rm_gtest.cpp
src/record/CMakeFiles/rm_gtest.dir/rm_gtest.cpp.o: src/record/CMakeFiles/rm_gtest.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/luo/rucbase/RUCbase/deps/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object src/record/CMakeFiles/rm_gtest.dir/rm_gtest.cpp.o"
	cd /home/luo/rucbase/RUCbase/deps/src/record && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT src/record/CMakeFiles/rm_gtest.dir/rm_gtest.cpp.o -MF CMakeFiles/rm_gtest.dir/rm_gtest.cpp.o.d -o CMakeFiles/rm_gtest.dir/rm_gtest.cpp.o -c /home/luo/rucbase/RUCbase/src/record/rm_gtest.cpp

src/record/CMakeFiles/rm_gtest.dir/rm_gtest.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/rm_gtest.dir/rm_gtest.cpp.i"
	cd /home/luo/rucbase/RUCbase/deps/src/record && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/luo/rucbase/RUCbase/src/record/rm_gtest.cpp > CMakeFiles/rm_gtest.dir/rm_gtest.cpp.i

src/record/CMakeFiles/rm_gtest.dir/rm_gtest.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/rm_gtest.dir/rm_gtest.cpp.s"
	cd /home/luo/rucbase/RUCbase/deps/src/record && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/luo/rucbase/RUCbase/src/record/rm_gtest.cpp -o CMakeFiles/rm_gtest.dir/rm_gtest.cpp.s

# Object files for target rm_gtest
rm_gtest_OBJECTS = \
"CMakeFiles/rm_gtest.dir/rm_gtest.cpp.o"

# External object files for target rm_gtest
rm_gtest_EXTERNAL_OBJECTS =

bin/rm_gtest: src/record/CMakeFiles/rm_gtest.dir/rm_gtest.cpp.o
bin/rm_gtest: src/record/CMakeFiles/rm_gtest.dir/build.make
bin/rm_gtest: lib/librecord.a
bin/rm_gtest: lib/libgtest_main.a
bin/rm_gtest: lib/libsystem.a
bin/rm_gtest: lib/libtransaction.a
bin/rm_gtest: lib/librecovery.a
bin/rm_gtest: lib/librecord.a
bin/rm_gtest: lib/libsystem.a
bin/rm_gtest: lib/libtransaction.a
bin/rm_gtest: lib/librecovery.a
bin/rm_gtest: lib/libindex.a
bin/rm_gtest: lib/libstorage.a
bin/rm_gtest: lib/libgtest.a
bin/rm_gtest: src/record/CMakeFiles/rm_gtest.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/luo/rucbase/RUCbase/deps/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable ../../bin/rm_gtest"
	cd /home/luo/rucbase/RUCbase/deps/src/record && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/rm_gtest.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
src/record/CMakeFiles/rm_gtest.dir/build: bin/rm_gtest
.PHONY : src/record/CMakeFiles/rm_gtest.dir/build

src/record/CMakeFiles/rm_gtest.dir/clean:
	cd /home/luo/rucbase/RUCbase/deps/src/record && $(CMAKE_COMMAND) -P CMakeFiles/rm_gtest.dir/cmake_clean.cmake
.PHONY : src/record/CMakeFiles/rm_gtest.dir/clean

src/record/CMakeFiles/rm_gtest.dir/depend:
	cd /home/luo/rucbase/RUCbase/deps && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/luo/rucbase/RUCbase /home/luo/rucbase/RUCbase/src/record /home/luo/rucbase/RUCbase/deps /home/luo/rucbase/RUCbase/deps/src/record /home/luo/rucbase/RUCbase/deps/src/record/CMakeFiles/rm_gtest.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : src/record/CMakeFiles/rm_gtest.dir/depend

