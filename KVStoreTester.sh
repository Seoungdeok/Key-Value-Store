#!/bin/bash

#################################################
# FILE NAME: KVStoreTester.sh
#
# DESCRIPTION: Tester for MP2 (with GDB option)
#
# RUN PROCEDURE:
# $ chmod +x KVStoreTester.sh
# $ ./KVStoreTester.sh
#
# The chmod part only needs to be done once to allow the script to be
# executed. It's okay if the chmod command gives an error message. In that
# case you can do this instead:
# $ bash ./KVStoreTester.sh
#
# You can use these options if you want:
# -v   Verbose output
# -g   Use gdb debugger to show crashes (implies -v)
#################################################

function contains () {
    local e
    for e in "${@:2}"
    do
        if [ "$e" == "$1" ]; then
            echo 1
            return 1;
        fi
    done
    echo 0
}

####
# Main function
####

verbose=$(contains "-v" "$@")
use_gdb=$(contains "-g" "$@")

if [ $use_gdb -eq 1 ]; then
    verbose=1
fi

# Make sure gdb is working with ptrace correctly on the server.
assert_ptrace_allowed () {
    HELP_MSG="Please click the \"Help\" button on the Coursera Labs interface and try
the \"Get latest version\" or \"Reboot\" buttons to restart your workspace,
then try this script again after the workspace restarts. If the issue
persists, please contact support. You can still run this tester without
the gdb feature (remove the -g flag)."
    failmsg () {
        echo ""
        echo "Error: [$@]"
        echo "$HELP_MSG"
        exit 1
    }
    echo ""
    echo "Checking that gdb is working with ptrace correctly..."
    (
        PTRACE_CHECK_DIR=$(realpath "/tmp/ptrace_check")
        (
            set -eu
            rm -rf "$PTRACE_CHECK_DIR"
            mkdir -p "$PTRACE_CHECK_DIR"
        ) || failmsg "Could not create temp directory"
        cd "$PTRACE_CHECK_DIR" || failmsg "Could not enter temp directory"
        (
            set +e
            echo 'int main() { return 0; }' > check.cpp
            g++ -std=c++11 -g check.cpp -o check
            gdb -batch -ex "set disable-randomization off" -ex "r" ./check &> check.log
        )
        echo "" >> check.log
        # if "not permitted" in log, ptrace is not allowed right now
        if grep -i "not permitted" check.log &>/dev/null ; then
            failmsg "It looks like the server won't allow gdb to work right now."
        fi
        # if "exited normally" not in log, something else broke
        if ! grep -i "exited normally" check.log &>/dev/null ; then
            failmsg "The gdb test did not exit normally."
        fi
    ) || { echo "Quitting." ; exit 1 ; }
    echo "Check OK."
}
if [ $use_gdb -eq 1 ]; then
    assert_ptrace_allowed
fi

###
# Global variables
###
SUCCESS=0
FAILURE=-1
RF=3
RFPLUSONE=4
CREATE_OPERATION="CREATE OPERATION"
CREATE_SUCCESS="create success"
GRADE=0
DELETE_OPERATION="DELETE OPERATION"
DELETE_SUCCESS="delete success"
DELETE_FAILURE="delete fail"
INVALID_KEY="invalidKey"
READ_OPERATION="READ OPERATION"
READ_SUCCESS="read success"
READ_FAILURE="read fail"
QUORUM=2
QUORUMPLUSONE=3
UPDATE_OPERATION="UPDATE OPERATION"
UPDATE_SUCCESS="update success"
UPDATE_FAILURE="update fail"

CRASH_LOG=$(realpath "crash_backtrace.log")

build_and_run () {
    set -u

    if [ $# -lt 1 ]; then
        echo "The testcase conf should be the first argument."
        exit 1
    fi
    if [ ! -f "$1" ]; then
        echo "File not found: $1"
        echo "The testcase conf should be the first argument."
        exit 1
    fi

    local CONF_FILE
    CONF_FILE="$1"

    TEST_CRASHED=0
    EXIT_CODE=0
    rm -f dbg.log /tmp/stderr.txt "$CRASH_LOG"

    if [ "${verbose}" -eq 0 ]; then
        make clean > /dev/null 2>&1
        make > /dev/null 2>&1
        if [ $? -ne "${SUCCESS}" ]; then
            echo "COMPILATION ERROR"
            exit
        fi
        rm -f dbg.log /tmp/stderr.txt "$CRASH_LOG"
        ./Application "$CONF_FILE" > /dev/null 2>&1
        EXIT_CODE=$?
        if [ $EXIT_CODE -gt 128 ]; then
            TEST_CRASHED=1
        fi
    else
        if [ $use_gdb -eq 1 ]; then
            make clean
            make
            if [ $? -ne "${SUCCESS}" ]; then
                echo "COMPILATION ERROR"
                exit
            fi
            echo ""
            echo "Running with GDB. This will be verbose."
            rm -f dbg.log /tmp/stderr.txt "$CRASH_LOG"
            gdb --batch --command=gdb.conf --args ./Application "$CONF_FILE" 2>/tmp/stderr.txt
            echo ""
            echo "Cleaning up GDB output:"
            echo "" >> /tmp/stderr.txt
            if [ "$(cat /tmp/stderr.txt)" ]; then
                echo ""
                echo "This was found in the standard error output stream:"
                cat /tmp/stderr.txt
            fi
            NO_FRAME=$(grep -i "No frame selected" "$CRASH_LOG" || echo "")
            if [ "$NO_FRAME" ]; then
                echo ""
                echo "GDB did not detect a crash, but you should still check that"
                echo "your program exited normally."
                TEST_CRASHED=0
                rm -f "$CRASH_LOG"
            else
                echo ""
                echo "GDB detected a crash. Log:"
                cat "$CRASH_LOG"
                echo ""
                TEST_CRASHED=1
                rm -f "$CRASH_LOG"
            fi
        else
            make clean
            make
            if [ $? -ne "${SUCCESS}" ]; then
                echo "COMPILATION ERROR"
                exit
            fi
            rm -f dbg.log /tmp/stderr.txt "$CRASH_LOG"
            ./Application "$CONF_FILE"
            EXIT_CODE=$?
            if [ $EXIT_CODE -gt 128 ]; then
                TEST_CRASHED=1
            fi
        fi
    fi

    if [ ! -f dbg.log ]; then
        TEST_CRASHED=1
        echo ""
        echo "Error: Could not find file dbg.log after running your program."
        echo ""
    fi

    if [ $EXIT_CODE -ne 0 ] || [ $TEST_CRASHED -eq 1 ]; then
        echo ""
        echo "Your program did not exit normally. Please look for logs above."
        echo "If you don't see any logs, rerun this script with the options"
        echo "-v or -g for more info."
        echo ""
    elif [ $use_gdb -eq 1 ]; then
        echo ""
        echo "Please look for logs above and check carefully to see if your code"
        echo "exited with any error messages besides what GDB might have caught."
        echo ""
    fi

    set +u
}

echo ""
echo "############################"
echo " CREATE TEST"
echo "############################"
echo ""

CREATE_TEST_STATUS="${SUCCESS}"
CREATE_TEST_SCORE=0

build_and_run "./testcases/create.conf"

if [ $TEST_CRASHED -eq 0 ]; then

    echo "TEST 1: Create 3 replicas of every key"

    create_count=`grep -i "${CREATE_OPERATION}" dbg.log | wc -l`
    create_success_count=`grep -i "${CREATE_SUCCESS}" dbg.log | wc -l`
    expected_count=$(( ${create_count} * ${RFPLUSONE} ))

    if [ ${create_success_count} -ne ${expected_count} ]
    then
        CREATE_TEST_STATUS="${FAILURE}"
    else
        keys=`grep -i "${CREATE_OPERATION}" dbg.log | cut -d" " -f7`
        for key in ${keys}
        do
            key_create_success_count=`grep -i "${CREATE_SUCCESS}" dbg.log | grep "${key}" | wc -l`
            if [ "${key_create_success_count}" -ne "${RFPLUSONE}" ]
            then
                CREATE_TEST_STATUS="${FAILURE}"
                break
            fi
        done
    fi

    if [ "${CREATE_TEST_STATUS}" -eq "${SUCCESS}" ]
    then
        CREATE_TEST_SCORE=3
    fi
fi

# Display score
echo "TEST 1 SCORE..................: ${CREATE_TEST_SCORE} / 3"
# Add to grade
GRADE=$(( ${GRADE} + ${CREATE_TEST_SCORE} ))

#echo ""
#echo "############################"
#echo " CREATE TEST ENDS"
#echo "############################"
#echo ""

echo ""
echo "############################"
echo " DELETE TEST"
echo "############################"
echo ""

DELETE_TEST1_STATUS="${SUCCESS}"
DELETE_TEST2_STATUS="${SUCCESS}"
DELETE_TEST1_SCORE=0
DELETE_TEST2_SCORE=0

build_and_run "./testcases/delete.conf"

if [ $TEST_CRASHED -eq 0 ]; then
    echo "TEST 1: Delete 3 replicas of every key"

    delete_count=`grep -i "${DELETE_OPERATION}" dbg.log | wc -l`
    valid_delete_count=$(( ${delete_count} - 1 ))
    expected_count=$(( ${valid_delete_count} * ${RFPLUSONE} ))
    delete_success_count=`grep -i "${DELETE_SUCCESS}" dbg.log | wc -l`

    if [ "${delete_success_count}" -ne "${expected_count}" ]
    then
        DELETE_TEST1_STATUS="${FAILURE}"
    else
        keys=""
        keys=`grep -i "${DELETE_OPERATION}" dbg.log | cut -d" " -f7`
        for key in ${keys}
        do
            if [ $key != "${INVALID_KEY}" ]
            then
                key_delete_success_count=`grep -i "${DELETE_SUCCESS}" dbg.log | grep "${key}" | wc -l`
                if [ "${key_delete_success_count}" -ne "${RFPLUSONE}" ]
                then
                    DELETE_TEST1_STATUS="${FAILURE}"
                    break
                fi
            fi
        done
    fi

    echo "TEST 2: Attempt delete of an invalid key"

    delete_fail_count=`grep -i "${DELETE_FAILURE}" dbg.log | grep "${INVALID_KEY}" | wc -l`
    if [ "${delete_fail_count}" -ne 4 ]
    then
        DELETE_TEST2_STATUS="${FAILURE}"
    fi

    if [ "${DELETE_TEST1_STATUS}" -eq "${SUCCESS}" ]
    then
        DELETE_TEST1_SCORE=3
    fi

    if [ "${DELETE_TEST2_STATUS}" -eq "${SUCCESS}" ]
    then
        DELETE_TEST2_SCORE=4
    fi
fi

# Display score
echo "TEST 1 SCORE..................: ${DELETE_TEST1_SCORE} / 3"
echo "TEST 2 SCORE..................: ${DELETE_TEST2_SCORE} / 4"
# Add to grade
GRADE=$(( ${GRADE} + ${DELETE_TEST1_SCORE} ))
GRADE=$(( ${GRADE} + ${DELETE_TEST2_SCORE} ))

#echo ""
#echo "############################"
#echo " DELETE TEST ENDS"
#echo "############################"
#echo ""

echo ""
echo "############################"
echo " READ TEST"
echo "############################"
echo ""

READ_TEST1_STATUS="${FAILURE}"
READ_TEST1_SCORE=0
READ_TEST2_STATUS="${FAILURE}"
READ_TEST2_SCORE=0
READ_TEST3_PART1_STATUS="${FAILURE}"
READ_TEST3_PART1_SCORE=0
READ_TEST3_PART2_STATUS="${FAILURE}"
READ_TEST3_PART2_SCORE=0
READ_TEST4_STATUS="${FAILURE}"
READ_TEST4_SCORE=0
READ_TEST5_STATUS="${FAILURE}"
READ_TEST5_SCORE=0

build_and_run "./testcases/read.conf"

if [ $TEST_CRASHED -eq 0 ]; then

    read_operations=`grep -i "${READ_OPERATION}" dbg.log  | cut -d" " -f3 | tr -s ']' ' '  | tr -s '[' ' ' | sort`

    cnt=1
    for time in ${read_operations}
    do
        if [ ${cnt} -eq 1 ]
        then
            echo "TEST 1: Read a key. Check for correct value being read at least in quorum of replicas"
            read_op_test1_time="${time}"
            read_op_test1_key=`grep -i "${READ_OPERATION}" dbg.log | grep "${read_op_test1_time}" | cut -d" " -f7`
            read_op_test1_value=`grep -i "${READ_OPERATION}" dbg.log | grep "${read_op_test1_time}" | cut -d" " -f9`
        elif [ ${cnt} -eq 2 ]
        then
            echo "TEST 2: Read a key after failing a replica. Check for correct value being read at least in quorum of replicas"
            read_op_test2_time="${time}"
            read_op_test2_key=`grep -i "${READ_OPERATION}" dbg.log | grep "${read_op_test2_time}" | cut -d" " -f7`
            read_op_test2_value=`grep -i "${READ_OPERATION}" dbg.log | grep "${read_op_test2_time}" | cut -d" " -f9`
        elif [ ${cnt} -eq 3 ]
        then
            echo "TEST 3 PART 1: Read a key after failing two replicas. Read should fail"
            read_op_test3_part1_time="${time}"
            read_op_test3_part1_key=`grep -i "${READ_OPERATION}" dbg.log | grep "${read_op_test3_part1_time}" | cut -d" " -f7`
            read_op_test3_part1_value=`grep -i "${READ_OPERATION}" dbg.log | grep "${read_op_test3_part1_time}" | cut -d" " -f9`
        elif [ ${cnt} -eq 4 ]
        then
            echo "TEST 3 PART 2: Read the key after allowing stabilization protocol to kick in. Check for correct value being read at least in quorum of replicas"
            read_op_test3_part2_time="${time}"
            read_op_test3_part2_key=`grep -i "${READ_OPERATION}" dbg.log | grep "${read_op_test3_part2_time}" | cut -d" " -f7`
            read_op_test3_part2_value=`grep -i "${READ_OPERATION}" dbg.log | grep "${read_op_test3_part2_time}" | cut -d" " -f9`
        elif [ ${cnt} -eq 5 ]
        then
            echo "TEST 4: Read a key after failing a non-replica. Check for correct value being read at least in quorum of replicas"
            read_op_test4_time="${time}"
            read_op_test4_key="${read_op_test1_key}"
            read_op_test4_value="${read_op_test1_value}"
        elif [ ${cnt} -eq 6 ]
        then
            echo "TEST 5: Attempt read of an invalid key"
            read_op_test5_time="${time}"
        fi
        cnt=$(( ${cnt} + 1 ))
    done

    read_test1_success_count=0
    read_test2_success_count=0
    read_test3_part2_success_count=0
    read_test4_success_count=0

    read_successes=`grep -i "${READ_SUCCESS}" dbg.log | grep ${read_op_test1_key} | grep ${read_op_test1_value} 2>/dev/null`
    if [ "${read_successes}" ]
    then
        while read success
        do
            time_of_this_success=`echo "${success}" | cut -d" " -f2 | tr -s '[' ' ' | tr -s ']' ' '`
            if [ "${time_of_this_success}" -ge "${read_op_test1_time}" -a "${time_of_this_success}" -lt "${read_op_test2_time}" ]
            then
                read_test1_success_count=`expr ${read_test1_success_count} + 1`
            elif [ "${time_of_this_success}" -ge "${read_op_test2_time}" -a "${time_of_this_success}" -lt "${read_op_test3_part1_time}" ]
            then
                read_test2_success_count=`expr ${read_test2_success_count} + 1`
            elif [ "${time_of_this_success}" -ge "${read_op_test3_part2_time}" -a "${time_of_this_success}" -lt "${read_op_test4_time}" ]
            then
                read_test3_part2_success_count=`expr ${read_test3_part2_success_count} + 1`
            elif [ "${time_of_this_success}" -ge "${read_op_test4_time}" ]
            then
                read_test4_success_count=`expr ${read_test4_success_count} + 1`
            fi
        done <<<"${read_successes}"
    fi

    read_test3_part1_fail_count=0
    read_test5_fail_count=0

    read_fails=`grep -i "${READ_FAILURE}" dbg.log 2>/dev/null`
    if [ "${read_fails}" ]
    then
        while read fail
        do
            time_of_this_fail=`echo "${fail}" | cut -d" " -f2 | tr -s '[' ' ' | tr -s ']' ' '`
            if [ "${time_of_this_fail}" -ge "${read_op_test3_part1_time}" -a "${time_of_this_fail}" -lt "${read_op_test3_part2_time}" ]
            then
                actual_key=`echo "${fail}" | grep "${read_op_test3_part1_key}" | wc -l`
                if [ "${actual_key}"  -eq 1 ]
                then
                    read_test3_part1_fail_count=`expr ${read_test3_part1_fail_count} + 1`
                fi
            elif [ "${time_of_this_fail}" -ge "${read_op_test5_time}" ]
            then
                actual_key=`echo "${fail}" | grep "${INVALID_KEY}" | wc -l`
                if [ "${actual_key}" -eq 1 ]
                then
                    read_test5_fail_count=`expr ${read_test5_fail_count} + 1`
                fi
            fi
        done <<<"${read_fails}"
    fi

    if [ "${read_test1_success_count}" -eq "${QUORUMPLUSONE}" -o "${read_test1_success_count}" -eq "${RFPLUSONE}" ]
    then
        READ_TEST1_STATUS="${SUCCESS}"
    fi
    if [ "${read_test2_success_count}" -eq "${QUORUMPLUSONE}" ]
    then
        READ_TEST2_STATUS="${SUCCESS}"
    fi
    if [ "${read_test3_part1_fail_count}" -eq 1 ]
    then
        READ_TEST3_PART1_STATUS="${SUCCESS}"
    fi
    if [ "${read_test3_part2_success_count}" -eq "${QUORUMPLUSONE}" -o "${read_test3_part2_success_count}" -eq "${RFPLUSONE}" ]
    then
        READ_TEST3_PART2_STATUS="${SUCCESS}"
    fi
    if [ "${read_test4_success_count}" -eq "${QUORUMPLUSONE}" -o "${read_test4_success_count}" -eq "${RFPLUSONE}" ]
    then
        READ_TEST4_STATUS="${SUCCESS}"
    fi
    if [ "${read_test5_fail_count}" -eq "${QUORUMPLUSONE}" -o "${read_test5_fail_count}" -eq "${RFPLUSONE}" ]
    then
        READ_TEST5_STATUS="${SUCCESS}"
    fi

    if [ "${READ_TEST1_STATUS}" -eq "${SUCCESS}" ]
    then
        READ_TEST1_SCORE=3
    fi
    if [ "${READ_TEST2_STATUS}" -eq "${SUCCESS}" ]
    then
        READ_TEST2_SCORE=9
    fi
    if [ "${READ_TEST3_PART1_STATUS}" -eq "${SUCCESS}" ]
    then
        READ_TEST3_PART1_SCORE=9
    fi
    if [ "${READ_TEST3_PART2_STATUS}" -eq "${SUCCESS}" ]
    then
        READ_TEST3_PART2_SCORE=10
    fi
    if [ "${READ_TEST4_STATUS}" -eq "${SUCCESS}" ]
    then
        READ_TEST4_SCORE=6
    fi
    if [ "${READ_TEST5_STATUS}" -eq "${SUCCESS}" ]
    then
        READ_TEST5_SCORE=3
    fi

fi

# Display score
echo "TEST 1 SCORE..................: ${READ_TEST1_SCORE} / 3"
echo "TEST 2 SCORE..................: ${READ_TEST2_SCORE} / 9"
echo "TEST 3 PART 1 SCORE..................: ${READ_TEST3_PART1_SCORE} / 9"
echo "TEST 3 PART 2 SCORE..................: ${READ_TEST3_PART2_SCORE} / 10"
echo "TEST 4 SCORE..................: ${READ_TEST4_SCORE} / 6"
echo "TEST 5 SCORE..................: ${READ_TEST5_SCORE} / 3"
# Add to grade
GRADE=`expr ${GRADE} + ${READ_TEST1_SCORE}`
GRADE=`expr ${GRADE} + ${READ_TEST2_SCORE}`
GRADE=`echo ${GRADE} ${READ_TEST3_PART1_SCORE} | awk '{print $1 + $2}'`
GRADE=`echo ${GRADE} ${READ_TEST3_PART2_SCORE} | awk '{print $1 + $2}'`
GRADE=`echo ${GRADE} ${READ_TEST4_SCORE} | awk '{print $1 + $2}'`
GRADE=`echo ${GRADE} ${READ_TEST5_SCORE} | awk '{print $1 + $2}'`

#echo ""
#echo "############################"
#echo " READ TEST ENDS"
#echo "############################"
#echo ""

echo ""
echo "############################"
echo " UPDATE TEST"
echo "############################"
echo ""

UPDATE_TEST1_STATUS="${FAILURE}"
UPDATE_TEST1_SCORE=0
UPDATE_TEST2_STATUS="${FAILURE}"
UPDATE_TEST2_SCORE=0
UPDATE_TEST3_PART1_STATUS="${FAILURE}"
UPDATE_TEST3_PART1_SCORE=0
UPDATE_TEST3_PART2_STATUS="${FAILURE}"
UPDATE_TEST3_PART2_SCORE=0
UPDATE_TEST4_STATUS="${FAILURE}"
UPDATE_TEST4_SCORE=0
UPDATE_TEST5_STATUS="${FAILURE}"
UPDATE_TEST5_SCORE=0

build_and_run "./testcases/update.conf"

if [ $TEST_CRASHED -eq 0 ]; then

    update_operations=`grep -i "${UPDATE_OPERATION}" dbg.log  | cut -d" " -f3 | tr -s ']' ' '  | tr -s '[' ' ' | sort`

    cnt=1
    for time in ${update_operations}
    do
        if [ ${cnt} -eq 1 ]
        then
            echo "TEST 1: Update a key. Check for correct value being updated at least in quorum of replicas"
            update_op_test1_time="${time}"
            update_op_test1_key=`grep -i "${UPDATE_OPERATION}" dbg.log | grep "${update_op_test1_time}" | cut -d" " -f7`
            update_op_test1_value=`grep -i "${UPDATE_OPERATION}" dbg.log | grep "${update_op_test1_time}" | cut -d" " -f9`
        elif [ ${cnt} -eq 2 ]
        then
            echo "TEST 2: Update a key after failing a replica. Check for correct value being updated at least in quorum of replicas"
            update_op_test2_time="${time}"
            update_op_test2_key=`grep -i "${UPDATE_OPERATION}" dbg.log | grep "${update_op_test2_time}" | cut -d" " -f7`
            update_op_test2_value=`grep -i "${UPDATE_OPERATION}" dbg.log | grep "${update_op_test2_time}" | cut -d" " -f9`
        elif [ ${cnt} -eq 3 ]
        then
            echo "TEST 3 PART 1: Update a key after failing two replicas. Update should fail"
            update_op_test3_part1_time="${time}"
            update_op_test3_part1_key=`grep -i "${UPDATE_OPERATION}" dbg.log | grep "${update_op_test3_part1_time}" | cut -d" " -f7`
            update_op_test3_part1_value=`grep -i "${UPDATE_OPERATION}" dbg.log | grep "${update_op_test3_part1_time}" | cut -d" " -f9`
        elif [ ${cnt} -eq 4 ]
        then
            echo "TEST 3 PART 2: Update the key after allowing stabilization protocol to kick in. Check for correct value being updated at least in quorum of replicas"
            update_op_test3_part2_time="${time}"
            update_op_test3_part2_key=`grep -i "${UPDATE_OPERATION}" dbg.log | grep "${update_op_test3_part2_time}" | cut -d" " -f7`
            update_op_test3_part2_value=`grep -i "${UPDATE_OPERATION}" dbg.log | grep "${update_op_test3_part2_time}" | cut -d" " -f9`
        elif [ ${cnt} -eq 5 ]
        then
            echo "TEST 4: Update a key after failing a non-replica. Check for correct value being updated at least in quorum of replicas"
            update_op_test4_time="${time}"
            update_op_test4_key="${update_op_test1_key}"
            update_op_test4_value="${update_op_test1_value}"
        elif [ ${cnt} -eq 6 ]
        then
            echo "TEST 5: Attempt update of an invalid key"
            update_op_test5_time="${time}"
        fi
        cnt=$(( ${cnt} + 1 ))
    done

    update_test1_success_count=0
    update_test2_success_count=0
    update_test3_part2_success_count=0
    update_test4_success_count=0

    update_successes=`grep -i "${UPDATE_SUCCESS}" dbg.log | grep ${update_op_test1_key} | grep ${update_op_test1_value} 2>/dev/null`
    if [ "${update_successes}" ]
    then
        while read success
        do
            time_of_this_success=`echo "${success}" | cut -d" " -f2 | tr -s '[' ' ' | tr -s ']' ' '`
            if [ "${time_of_this_success}" -ge "${update_op_test1_time}" -a "${time_of_this_success}" -lt "${update_op_test2_time}" ]
            then
                update_test1_success_count=`expr ${update_test1_success_count} + 1`
            elif [ "${time_of_this_success}" -ge "${update_op_test2_time}" -a "${time_of_this_success}" -lt "${update_op_test3_part1_time}" ]
            then
                update_test2_success_count=`expr ${update_test2_success_count} + 1`
            elif [ "${time_of_this_success}" -ge "${update_op_test3_part2_time}" -a "${time_of_this_success}" -lt "${update_op_test4_time}" ]
            then
                update_test3_part2_success_count=`expr ${update_test3_part2_success_count} + 1`
            elif [ "${time_of_this_success}" -ge "${update_op_test4_time}" ]
            then
                update_test4_success_count=`expr ${update_test4_success_count} + 1`
            fi
        done <<<"${update_successes}"
    fi

    update_test3_part1_fail_count=0
    update_test5_fail_count=0

    update_fails=`grep -i "${UPDATE_FAILURE}" dbg.log 2>/dev/null`
    if [ "${update_fails}" ]
    then
        while read fail
        do
            time_of_this_fail=`echo "${fail}" | cut -d" " -f2 | tr -s '[' ' ' | tr -s ']' ' '`
            if [ "${time_of_this_fail}" -ge "${update_op_test3_part1_time}" -a "${time_of_this_fail}" -lt "${update_op_test3_part2_time}" ]
            then
                actual_key=`echo "${fail}" | grep "${update_op_test3_part1_key}" | wc -l`
                if [ "${actual_key}"  -eq 1 ]
                then
                    update_test3_part1_fail_count=`expr ${update_test3_part1_fail_count} + 1`
                fi
            elif [ "${time_of_this_fail}" -ge "${update_op_test5_time}" ]
            then
                actual_key=`echo "${fail}" | grep "${INVALID_KEY}" | wc -l`
                if [ "${actual_key}" -eq 1 ]
                then
                    update_test5_fail_count=`expr ${update_test5_fail_count} + 1`
                fi
            fi
        done <<<"${update_fails}"
    fi

    if [ "${update_test1_success_count}" -eq "${QUORUMPLUSONE}" -o "${update_test1_success_count}" -eq "${RFPLUSONE}" ]
    then
        UPDATE_TEST1_STATUS="${SUCCESS}"
    fi
    if [ "${update_test2_success_count}" -eq "${QUORUMPLUSONE}" ]
    then
        UPDATE_TEST2_STATUS="${SUCCESS}"
    fi
    if [ "${update_test3_part1_fail_count}" -eq 1 ]
    then
        UPDATE_TEST3_PART1_STATUS="${SUCCESS}"
    fi
    if [ "${update_test3_part2_success_count}" -eq "${QUORUMPLUSONE}" -o "${update_test3_part2_success_count}" -eq "${RFPLUSONE}" ]
    then
        UPDATE_TEST3_PART2_STATUS="${SUCCESS}"
    fi
    if [ "${update_test4_success_count}" -eq "${QUORUMPLUSONE}" -o "${update_test4_success_count}" -eq "${RFPLUSONE}" ]
    then
        UPDATE_TEST4_STATUS="${SUCCESS}"
    fi
    if [ "${update_test5_fail_count}" -eq "${QUORUMPLUSONE}" -o "${update_test5_fail_count}" -eq "${RFPLUSONE}" ]
    then
        UPDATE_TEST5_STATUS="${SUCCESS}"
    fi

    if [ "${UPDATE_TEST1_STATUS}" -eq "${SUCCESS}" ]
    then
        UPDATE_TEST1_SCORE=3
    fi
    if [ "${UPDATE_TEST2_STATUS}" -eq "${SUCCESS}" ]
    then
        UPDATE_TEST2_SCORE=9
    fi
    if [ "${UPDATE_TEST3_PART1_STATUS}" -eq "${SUCCESS}" ]
    then
        UPDATE_TEST3_PART1_SCORE=9
    fi
    if [ "${UPDATE_TEST3_PART2_STATUS}" -eq "${SUCCESS}" ]
    then
        UPDATE_TEST3_PART2_SCORE=10
    fi
    if [ "${UPDATE_TEST4_STATUS}" -eq "${SUCCESS}" ]
    then
        UPDATE_TEST4_SCORE=6
    fi
    if [ "${UPDATE_TEST5_STATUS}" -eq "${SUCCESS}" ]
    then
        UPDATE_TEST5_SCORE=3
    fi

fi

# Display score
echo "TEST 1 SCORE..................: ${UPDATE_TEST1_SCORE} / 3"
echo "TEST 2 SCORE..................: ${UPDATE_TEST2_SCORE} / 9"
echo "TEST 3 PART 1 SCORE..................: ${UPDATE_TEST3_PART1_SCORE} / 9"
echo "TEST 3 PART 2 SCORE..................: ${UPDATE_TEST3_PART2_SCORE} / 10"
echo "TEST 4 SCORE..................: ${UPDATE_TEST4_SCORE} / 6"
echo "TEST 5 SCORE..................: ${UPDATE_TEST5_SCORE} / 3"
# Add to grade
GRADE=`echo ${GRADE} ${UPDATE_TEST1_SCORE} | awk '{print $1 + $2}'`
GRADE=`echo ${GRADE} ${UPDATE_TEST2_SCORE} | awk '{print $1 + $2}'`
GRADE=`echo ${GRADE} ${UPDATE_TEST3_PART1_SCORE} | awk '{print $1 + $2}'`
GRADE=`echo ${GRADE} ${UPDATE_TEST3_PART2_SCORE} | awk '{print $1 + $2}'`
GRADE=`echo ${GRADE} ${UPDATE_TEST4_SCORE} | awk '{print $1 + $2}'`
GRADE=`echo ${GRADE} ${UPDATE_TEST5_SCORE} | awk '{print $1 + $2}'`

#echo ""
#echo "############################"
#echo " UPDATE TEST ENDS"
#echo "############################"
#echo ""

echo ""
echo "ESTIMATED OVERALL GRADE: ${GRADE} / 90"
echo ""
echo "Please run the tester several times, as some kinds of coding mistakes"
echo "will cause intermittent crashes. The autograder on the server will run"
echo "your code several times."

if [ $use_gdb -eq 0 ]; then
    echo ""
    echo "Remember that with this tester, you can use -v for verbose"
    echo "output or -g to see GDB (debugger) crash reports."
else
    echo ""
    echo "You ran with the -g option for GDB reports, so please check the output"
    echo "above carefully. Keep these things in mind:"
    echo ""
    echo "- You may see an message about \"No frame selected\" in the verbose"
    echo "  output from GDB. This is normal when it finishes running without"
    echo "  detecting a crash."
    echo ""
    echo "- An error message about raise.c not being found is not a system"
    echo "  error. It means the system library source code can't be inspected"
    echo "  but the system libraries themselves are installed. Look at your"
    echo "  backtrace to see where the error really occurred."
    echo ""
    echo "- When you look at the backtrace, look for the part where the"
    echo "  execution leaves your code and enters the system library code."
    echo "  The bug is in your code, not in the system library. The notation"
    echo "  someCodeFile:123 means line 123 of someCodeFile."
fi

echo ""
