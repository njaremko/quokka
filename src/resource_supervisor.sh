set -u

quokka_action_cgroup=
quokka_logical_cgroup=
quokka_supervisor_cgroup=
quokka_outer_cgroup=
quokka_supervisor_return_cgroup=
quokka_action_oom_before=
quokka_action_oom_kill_before=
quokka_child_pid=
quokka_watchdog_pid=
quokka_timer_pid=
quokka_timeout_marker=
quokka_timer_pid_handshake=
quokka_cleanup_running=0

quokka_fail() {
    echo "quokka resource $quokka_resource_marker_token cgroup setup failure: $*" >&2
    exit 2
}

quokka_remote_linux_worker() {
    [ "$(uname -s)" = "Linux" ] || return 1
    case "$(pwd)" in
        /worker/build/*) return 0 ;;
        *) return 1 ;;
    esac
}

quokka_cgroup_subtree_has_memory() {
    while IFS= read -r enabled_controllers; do
        for enabled_controller in $enabled_controllers; do
            [ "$enabled_controller" = "memory" ] && return 0
        done
    done < "$1/cgroup.subtree_control"
    return 1
}

quokka_required_controllers_ready() {
    available=$(cat "$1/cgroup.controllers") || quokka_fail "cannot read action cgroup controllers: $1"
    enabled=$(cat "$1/cgroup.subtree_control") || quokka_fail "cannot read action cgroup delegation: $1"
    for controller in memory cpu pids; do
        case " $available " in
            *" $controller "*)
                case " $enabled " in
                    *" $controller "*) ;;
                    *) return 1 ;;
                esac
                ;;
        esac
    done
    return 0
}

quokka_enable_controllers() {
    cgroup=$1
    available=$(cat "$cgroup/cgroup.controllers") || quokka_fail "cannot read action cgroup controllers: $cgroup"
    enabled=$(cat "$cgroup/cgroup.subtree_control") || quokka_fail "cannot read action cgroup delegation: $cgroup"
    controllers=
    for controller in memory cpu pids; do
        case " $available " in
            *" $controller "*)
                case " $enabled " in
                    *" $controller "*) ;;
                    *) controllers="$controllers +$controller" ;;
                esac
                ;;
        esac
    done
    [ -n "$controllers" ] || return 0
    printf '%s' "${controllers# }" > "$cgroup/cgroup.subtree_control" || quokka_fail "cannot delegate action cgroup controllers: $cgroup"
}

quokka_find_outer_cgroup() {
    candidate="$1"
    [ -d "$candidate" ] || return 1
    if [ -d "$candidate/bb-runner-self" ] && [ -w "$candidate/bb-runner-self/cgroup.procs" ]; then
        echo "$candidate/bb-runner-self"
        return 0
    fi
    if [ -n "$quokka_cgroup_parent" ] && [ -d "$quokka_cgroup_parent/bb-runner-self" ] && [ -w "$quokka_cgroup_parent/bb-runner-self/cgroup.procs" ]; then
        echo "$quokka_cgroup_parent/bb-runner-self"
        return 0
    fi
    if [ -f "$candidate/cgroup.subtree_control" ] && [ -s "$candidate/cgroup.subtree_control" ]; then
        return_leaf="$candidate/quokka-supervisor-return"
        mkdir -p "$return_leaf" 2>/dev/null
        if [ -w "$return_leaf/cgroup.procs" ]; then
            echo "$return_leaf"
            return 0
        fi
    fi
    [ -w "$candidate/cgroup.procs" ] || return 1
    echo "$candidate"
    return 0
}

quokka_copy_cgroup_value() {
    source_file=$1
    destination_file=$2
    [ -f "$source_file" ] || return 0
    [ -w "$destination_file" ] || return 0
    value=
    while IFS= read -r line; do
        value=$line
    done < "$source_file"
    [ -z "$value" ] || echo "$value" > "$destination_file"
}

quokka_cgroup_event_value() {
    events_file=$1
    event_name=$2
    [ -f "$events_file" ] || return 1
    while IFS=' ' read -r name value; do
        if [ "$name" = "$event_name" ]; then
            echo "$value"
            return 0
        fi
    done < "$events_file"
    return 1
}

quokka_resolve_cgroup_parent() {
    current_cgroup_path=
    while IFS=: read -r hierarchy _controllers path; do
        [ "$hierarchy" = "0" ] && current_cgroup_path=$path
    done < /proc/self/cgroup
    [ -n "$current_cgroup_path" ] || quokka_fail "remote Linux tests require cgroup v2"
    case "$current_cgroup_path" in
        */*) quokka_outer_cgroup="/sys/fs/cgroup${current_cgroup_path%/*}" ;;
        *) quokka_fail "remote Linux tests must run under a nested action cgroup: $current_cgroup_path" ;;
    esac
    quokka_cgroup_parent="/sys/fs/cgroup$current_cgroup_path"
    [ -w "$quokka_cgroup_parent/cgroup.procs" ] || quokka_fail "remote Linux tests require writable action cgroup: $quokka_cgroup_parent"
    quokka_supervisor_return_cgroup=$(quokka_find_outer_cgroup "$quokka_outer_cgroup") || quokka_fail "remote Linux tests require a writable outer cgroup: $quokka_outer_cgroup"
    quokka_supervisor_cgroup=$(mktemp -d "$quokka_cgroup_parent/quokka-supervisor-XXXXXX") || quokka_fail "cannot create action supervisor cgroup"
    echo "$$" > "$quokka_supervisor_cgroup/cgroup.procs" || quokka_fail "cannot move action supervisor into its cgroup"
    if ! quokka_required_controllers_ready "$quokka_cgroup_parent"; then
        quokka_enable_controllers "$quokka_cgroup_parent"
    fi
    quokka_cgroup_subtree_has_memory "$quokka_cgroup_parent" || quokka_fail "remote Linux tests require action cgroup memory delegation: $quokka_cgroup_parent"
}

quokka_create_cgroup() {
    prefix=$1
    quokka_created_cgroup=$(mktemp -d "$quokka_cgroup_parent/$prefix-XXXXXX") || quokka_fail "cannot create a child under $quokka_cgroup_parent"
    quokka_copy_cgroup_value "$quokka_cgroup_parent/cpuset.cpus.effective" "$quokka_created_cgroup/cpuset.cpus" || quokka_fail "cannot copy cpuset.cpus"
    quokka_copy_cgroup_value "$quokka_cgroup_parent/cpuset.mems.effective" "$quokka_created_cgroup/cpuset.mems" || quokka_fail "cannot copy cpuset.mems"
    echo "$quokka_memory_max_value" > "$quokka_created_cgroup/memory.max" || quokka_fail "cannot set memory.max"
    echo "1" > "$quokka_created_cgroup/memory.oom.group" || quokka_fail "cannot set memory.oom.group"
}

quokka_kill_cgroup() {
    cgroup=$1
    [ -d "$cgroup" ] || return 0
    [ -w "$cgroup/cgroup.kill" ] || quokka_fail "remote Linux tests require cgroup.kill: $cgroup"
    echo "1" > "$cgroup/cgroup.kill" || quokka_fail "cannot kill child cgroup $cgroup"
    cleanup_waits=0
    while :; do
        populated=$(quokka_cgroup_event_value "$cgroup/cgroup.events" populated) || quokka_fail "cannot read child cgroup population: $cgroup"
        [ "$populated" = "0" ] && return 0
        cleanup_waits=$((cleanup_waits + 1))
        [ "$cleanup_waits" -lt 1000 ] || quokka_fail "child cgroup did not empty: $cgroup"
        sleep 0.01
    done
}

quokka_remove_cgroup() {
    cgroup=$1
    [ -d "$cgroup" ] || return 0
    quokka_kill_cgroup "$cgroup"
    rmdir "$cgroup" 2>/dev/null || quokka_fail "cannot remove child cgroup $cgroup"
}

quokka_stop_pid() {
    pid=$1
    [ -n "$pid" ] || return 0
    [ "$pid" = "$$" ] && return 0
    kill -TERM "$pid" 2>/dev/null || true
    stop_waits=0
    while kill -0 "$pid" 2>/dev/null; do
        stop_waits=$((stop_waits + 1))
        [ "$stop_waits" -lt 100 ] || break
        sleep 0.01
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || true
}

quokka_cleanup() {
    [ "$quokka_cleanup_running" -eq 0 ] || return 0
    quokka_cleanup_running=1
    if [ -z "$quokka_timer_pid" ] && [ -n "$quokka_timer_pid_handshake" ] && [ -s "$quokka_timer_pid_handshake" ]; then
        quokka_timer_pid=$(cat "$quokka_timer_pid_handshake" 2>/dev/null || true)
    fi
    quokka_stop_pid "$quokka_timer_pid"
    quokka_timer_pid=
    quokka_stop_pid "$quokka_watchdog_pid"
    quokka_watchdog_pid=
    quokka_stop_pid "$quokka_child_pid"
    quokka_child_pid=
    [ -z "$quokka_timeout_marker" ] || rm -f "$quokka_timeout_marker"
    quokka_timeout_marker=
    [ -z "$quokka_timer_pid_handshake" ] || rm -f "$quokka_timer_pid_handshake"
    quokka_timer_pid_handshake=
    [ -z "$quokka_logical_cgroup" ] || quokka_remove_cgroup "$quokka_logical_cgroup"
    quokka_logical_cgroup=
    [ -z "$quokka_action_cgroup" ] || quokka_remove_cgroup "$quokka_action_cgroup"
    quokka_action_cgroup=
    if [ -n "$quokka_supervisor_cgroup" ]; then
        echo "$$" > "$quokka_supervisor_return_cgroup/cgroup.procs" || quokka_fail "cannot leave action supervisor cgroup"
        rmdir "$quokka_supervisor_cgroup" 2>/dev/null || quokka_fail "cannot remove action supervisor cgroup"
        quokka_supervisor_cgroup=
        quokka_supervisor_return_cgroup=
    fi
}

quokka_exit() {
    quokka_exit_status=$?
    trap - EXIT
    quokka_cleanup
    exit "$quokka_exit_status"
}

quokka_signal() {
    quokka_signal_status=$1
    trap - EXIT HUP INT TERM
    quokka_cleanup
    exit "$quokka_signal_status"
}

trap quokka_exit EXIT
trap 'quokka_signal 129' HUP
trap 'quokka_signal 130' INT
trap 'quokka_signal 143' TERM

quokka_begin_action() {
    quokka_remote_linux_worker || return 0
    quokka_resolve_cgroup_parent
    if [ "$quokka_cgroup_granularity" = "action" ]; then
        quokka_create_cgroup quokka-action
        quokka_action_cgroup=$quokka_created_cgroup
        quokka_action_oom_before=$(quokka_cgroup_event_value "$quokka_action_cgroup/memory.events" oom) || quokka_fail "cannot read action oom counter"
        quokka_action_oom_kill_before=$(quokka_cgroup_event_value "$quokka_action_cgroup/memory.events" oom_kill) || quokka_fail "cannot read action oom_kill counter"
    fi
}

quokka_run_child() {
    cgroup=$1
    shift
    if [ -n "$cgroup" ]; then
        /bin/sh -c '
            cgroup_procs=$1
            marker_token=$2
            shift 2
            if ! echo "$$" > "$cgroup_procs"; then
                echo "quokka resource $marker_token cgroup setup failure: cannot join child cgroup ${cgroup_procs%/cgroup.procs}" >&2
                exit 125
            fi
            exec "$@"
        ' quokka-resource-child "$cgroup/cgroup.procs" "$quokka_resource_marker_token" "$@" &
    else
        "$@" &
    fi
    quokka_child_pid=$!
}

quokka_run_logical() {
    logical_index=$1
    shift
    quokka_logical_cgroup=
    oom_before=
    oom_kill_before=
    if quokka_remote_linux_worker; then
        if [ "$quokka_cgroup_granularity" = "logical-test" ]; then
            quokka_create_cgroup quokka-logical-test
            quokka_logical_cgroup=$quokka_created_cgroup
            oom_before=$(quokka_cgroup_event_value "$quokka_logical_cgroup/memory.events" oom) || quokka_fail "cannot read logical-test oom counter"
            oom_kill_before=$(quokka_cgroup_event_value "$quokka_logical_cgroup/memory.events" oom_kill) || quokka_fail "cannot read logical-test oom_kill counter"
        else
            quokka_logical_cgroup=$quokka_action_cgroup
        fi
    fi

    quokka_timeout_marker=$(mktemp "${TMPDIR:-/tmp}/quokka-timeout-XXXXXX") || quokka_fail "cannot create timeout marker"
    timeout_marker=$quokka_timeout_marker
    rm -f "$quokka_timeout_marker"
    quokka_timer_pid_handshake=$(mktemp "${TMPDIR:-/tmp}/quokka-timer-pid-XXXXXX") || quokka_fail "cannot create timer handshake file"
    timer_pid_handshake=$quokka_timer_pid_handshake
    rm -f "$quokka_timer_pid_handshake"
    quokka_run_child "$quokka_logical_cgroup" "$@"
    child_pid=$quokka_child_pid
    quokka_child_pid=$child_pid
    (
        trap - EXIT
        timer_pid=
        trap 'if [ -n "$timer_pid" ]; then kill -TERM "$timer_pid" 2>/dev/null || true; wait "$timer_pid" 2>/dev/null || true; fi; exit 143' HUP INT TERM
        sleep "$quokka_logical_timeout_seconds" &
        timer_pid=$!
        echo "$timer_pid" > "$timer_pid_handshake"
        wait "$timer_pid" 2>/dev/null || true
        if kill -0 "$child_pid" 2>/dev/null; then
            : > "$timeout_marker"
            if [ -n "$quokka_logical_cgroup" ]; then
                quokka_kill_cgroup "$quokka_logical_cgroup"
            else
                kill -KILL "$child_pid" 2>/dev/null || true
            fi
        fi
    ) &
    watchdog_pid=$!
    quokka_watchdog_pid=$watchdog_pid

    set +e
    wait "$child_pid"
    status=$?
    set -e
    quokka_child_pid=

    # The timer is a live grandchild of this supervisor (forked by the
    # subshell below, not by us), so we can never `wait` it directly, and
    # killing only that subshell on early completion orphans the timer for
    # the rest of the deadline. Reap it by the PID the subshell hands back
    # once it has actually started the timer; only signal it early when the
    # marker proves it has not already fired and been reaped by its own
    # parent, which also keeps this from targeting a since-recycled PID in
    # all but a vanishingly narrow window right at the deadline boundary.
    handshake_waits=0
    while [ ! -s "$quokka_timer_pid_handshake" ]; do
        handshake_waits=$((handshake_waits + 1))
        [ "$handshake_waits" -lt 1000 ] || quokka_fail "watchdog timer did not start"
        sleep 0.01
    done
    quokka_last_watchdog_timer_pid=$(cat "$quokka_timer_pid_handshake") || quokka_fail "cannot read watchdog timer handshake"
    quokka_timer_pid=$quokka_last_watchdog_timer_pid
    rm -f "$quokka_timer_pid_handshake"
    quokka_timer_pid_handshake=
    if [ ! -f "$quokka_timeout_marker" ]; then
        quokka_stop_pid "$quokka_timer_pid"
    fi
    wait "$watchdog_pid" 2>/dev/null || true
    quokka_watchdog_pid=
    quokka_timer_pid=

    if [ -f "$quokka_timeout_marker" ]; then
        if [ "$quokka_cgroup_granularity" = "action" ]; then
            echo "quokka resource $quokka_resource_marker_token test action timeout: seconds=$quokka_logical_timeout_seconds" >&2
        else
            echo "quokka resource $quokka_resource_marker_token logical test timeout: index=$logical_index seconds=$quokka_logical_timeout_seconds" >&2
        fi
        status=124
    fi
    rm -f "$quokka_timeout_marker"
    quokka_timeout_marker=

    if [ "$quokka_cgroup_granularity" = "action" ] && [ -n "$quokka_action_cgroup" ]; then
        quokka_kill_cgroup "$quokka_action_cgroup"
    fi

    if [ "$quokka_cgroup_granularity" = "logical-test" ] && [ -n "$quokka_logical_cgroup" ]; then
        oom_after=$(quokka_cgroup_event_value "$quokka_logical_cgroup/memory.events" oom) || quokka_fail "cannot read logical-test oom counter"
        oom_kill_after=$(quokka_cgroup_event_value "$quokka_logical_cgroup/memory.events" oom_kill) || quokka_fail "cannot read logical-test oom_kill counter"
        if [ "$oom_after" -gt "$oom_before" ] || [ "$oom_kill_after" -gt "$oom_kill_before" ]; then
            echo "quokka resource $quokka_resource_marker_token logical test cgroup OOM: index=$logical_index memory.max=$quokka_memory_max_value oom=$oom_before->$oom_after oom_kill=$oom_kill_before->$oom_kill_after" >&2
        fi
        quokka_remove_cgroup "$quokka_logical_cgroup"
        quokka_logical_cgroup=
    fi
    return "$status"
}

quokka_finish_action() {
    status=$1
    if [ -n "$quokka_action_cgroup" ]; then
        oom_after=$(quokka_cgroup_event_value "$quokka_action_cgroup/memory.events" oom) || quokka_fail "cannot read action oom counter"
        oom_kill_after=$(quokka_cgroup_event_value "$quokka_action_cgroup/memory.events" oom_kill) || quokka_fail "cannot read action oom_kill counter"
        if [ "$oom_after" -gt "$quokka_action_oom_before" ] || [ "$oom_kill_after" -gt "$quokka_action_oom_kill_before" ]; then
            echo "quokka resource $quokka_resource_marker_token test action cgroup OOM: memory.max=$quokka_memory_max_value oom=$quokka_action_oom_before->$oom_after oom_kill=$quokka_action_oom_kill_before->$oom_kill_after" >&2
        fi
        quokka_remove_cgroup "$quokka_action_cgroup"
        quokka_action_cgroup=
    fi
    quokka_cleanup
    exit "$status"
}
