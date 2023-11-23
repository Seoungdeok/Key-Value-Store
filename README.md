
# MP2 README - Fault-Tolerant Key-Value Store

## About the MP1Node.h and MP1Node.cpp files in MP2

Please note, this MP makes use of the MP1Node.h and MP1Node.cpp files that
are like the ones from the previous MP. You can (and should) copy your own
MP1Node.h and MP1Node.cpp files from MP1 to this directory, and then make
use of them in local testing.

However, if you were not completely successful on MP1, then you can still
choose to have your MP2 graded against a reference solution copy of MP1 on the
server. You cannot see the MP1 solution source code, but it will be used to
compile and link your MP2 files. When you use the submit.py script in this
directory, you will be asked to choose whether to submit your own MP1 files
or to rely on the solution files on the server.

## Basic tips

Read the specification document thoroughly on Coursera.

Create a high level design covering all scenarios / test cases before you start coding.

## How to submit for grading

To submit your work for autograding on the server, you need to generate a fresh submission token from the Coursera assignment web page. It's a text string you can copy and paste. Then, you need to run the submission script from your workspace directory:

```
python3 ./submit.py
```

It will ask you to enter your Coursera email address and the submission token.

## Local testing

### How do I test if my code passes all the test cases ?

Run the grader. Check the run procedure in `KVStoreTester.sh`. You can run this test suite locally like this:

```
$ chmod +x KVStoreTester.sh
$ ./KVStoreTester.sh
```

The `chmod` part only needs to be done once to allow the script to be executed. It's okay if the `chmod` command gives an error message. In that case you can do this instead:

```
$ bash ./KVStoreTester.sh
```

### How do I run the CRUD tests separately?

First, compile your project:

```
$ make clean
$ make
```

Then use one of these invocations:

```
$ ./Application ./testcases/create.conf
$ ./Application ./testcases/delete.conf
$ ./Application ./testcases/read.conf
$ ./Application ./testcases/update.conf
```

You may need to do `make clean && make` in between tests to make sure you have a clean run.

### How can I get detailed debugging logs?

The `KVStoreTester.sh` script has additional command-line options `-v` (verbose) and `-g` (show GDB debugger output). This is useful for showing where a crash might be happening. You may want to run the script several times in case you have an intermittent crash bug.

If you use the `-g` option, keep these things in mind:

- You may see an message about "No frame selected" in the verbose output from GDB. This is normal when it finishes running without detecting a crash.

- An error message about `raise.c` not being found is not a system error. It means the system library source code can't be inspected but the system libraries themselves are installed. Look at your backtrace to see where the error really occurred.

- When you look at the backtrace, look for the part where the execution leaves your code and enters the system library code. The bug is in your code, not in the system library. The notation `someCodeFile:123` means line 123 of `someCodeFile`.

- When using `-g`, The `KVStoreTester.sh` script will first check that `gdb` is working correctly on the server. Sometimes, a server initialization error can prevent `gdb` from working properly. If the script detects this situation, you can try rebooting the lab: please click the "Help" button on the Coursera Labs interface and then the "Reboot" button to restart your lab. After the workspace reloads, then try the tester script again. You may need to try this more than once. If the issue persists, please contact support. You can still run the tester without the `gdb` feature by omitting the `-g` flag when you run the script from the terminal.
