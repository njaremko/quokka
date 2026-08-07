//! Regression test for the logical-test watchdog's timer process lifecycle.
//!
//! Runs the real `resource_supervisor.sh` through `/bin/sh`, outside any
//! cgroup. The child synchronizes on the watchdog's PID handshake instead of
//! elapsed time, and timer liveness is checked with the shell's `kill -0`
//! builtin so the Nix build sandbox needs no process-inspection tool.

use std::process::{Command, Output};
use std::thread;
use std::time::Duration;

use quokka::execution::{ResourceFailure, ResourceMarkerToken, parse_resource_failures};

fn marker_token() -> ResourceMarkerToken {
    ResourceMarkerToken::from_stable_fields("watchdog-lifecycle-test", [])
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn run_supervisor(
    logical_timeout_seconds: &str,
    marker: &ResourceMarkerToken,
    child_script: &str,
) -> Output {
    let mut script = format!(
        "{}\nquokka_cgroup_granularity=action\nquokka_logical_timeout_seconds={}\nquokka_memory_max_value=max\nquokka_resource_marker_token={}\nquokka_action_cgroup=\nquokka_run_child() {{\n    _cgroup=$1\n    shift\n    sh -c {} quokka-watchdog-child \"$timer_pid_handshake\" &\n    quokka_child_pid=$!\n}}\nstatus=0\nquokka_run_logical 0 ignored",
        include_str!("../src/resource_supervisor.sh"),
        logical_timeout_seconds,
        shell_quote(marker.as_str()),
        shell_quote(child_script),
    );
    script.push_str(
        " || status=$?\n\
         if kill -0 \"$quokka_last_watchdog_timer_pid\" 2>/dev/null; then\n\
         \x20   echo \"RESULT status=$status timer_alive=yes\"\n\
         else\n\
         \x20   echo \"RESULT status=$status timer_alive=no\"\n\
         fi\n",
    );
    Command::new("sh")
        .arg("-c")
        .arg(&script)
        .output()
        .expect("resource_supervisor.sh executes under /bin/sh")
}

fn result_line(output: &Output) -> String {
    String::from_utf8(output.stdout.clone())
        .expect("script stdout is UTF-8")
        .lines()
        .find(|line| line.starts_with("RESULT "))
        .map(str::to_owned)
        .unwrap_or_else(|| {
            panic!(
                "driver script prints exactly one RESULT line; stdout={:?}; stderr={:?}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            )
        })
}

#[test]
fn normal_completion_reaps_the_watchdog_timer_without_orphaning_it() {
    let token = marker_token();
    let output = run_supervisor("300", &token, "while [ ! -s \"$1\" ]; do :; done");

    assert_eq!(result_line(&output), "RESULT status=0 timer_alive=no");
    assert_eq!(
        parse_resource_failures(&output.stderr, &token),
        Vec::<ResourceFailure>::new()
    );
}

#[test]
fn timeout_still_fires_and_leaves_no_timer_behind() {
    let token = marker_token();
    let output = run_supervisor("1", &token, "kill -STOP $$");

    assert_eq!(result_line(&output), "RESULT status=124 timer_alive=no");
    assert_eq!(
        parse_resource_failures(&output.stderr, &token),
        vec![ResourceFailure::ActionTimeout {
            details: "quokka test action timeout: seconds=1".to_owned(),
        }]
    );
}

fn process_is_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn run_signal_supervisor(signal: &str) -> (Output, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "quokka-supervisor-signal-{}-{signal}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("signal test directory");
    let child_pid_file = root.join("child.pid");
    let marker = marker_token();
    let script = format!(
        "{}\nquokka_cgroup_granularity=action\nquokka_logical_timeout_seconds=300\nquokka_memory_max_value=max\nquokka_resource_marker_token={}\nquokka_run_child() {{\n    _cgroup=$1\n    shift\n    sh -c 'echo $$ > \"$1\"; while :; do :; done' quokka-watchdog-child {} &\n    quokka_child_pid=$!\n}}\nquokka_run_logical 0 ignored",
        include_str!("../src/resource_supervisor.sh"),
        shell_quote(marker.as_str()),
        shell_quote(&child_pid_file.to_string_lossy()),
    );
    let mut process = Command::new("sh")
        .arg("-c")
        .arg(script)
        .env("TMPDIR", &root)
        .spawn()
        .expect("signal supervisor starts");
    for _ in 0..100 {
        if child_pid_file.is_file() {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    let kill_status = Command::new("kill")
        .args([format!("-{signal}"), process.id().to_string()])
        .status()
        .expect("send supervisor signal");
    assert!(kill_status.success());
    let output = process.wait_with_output().expect("signal supervisor exits");
    (output, root)
}

#[test]
fn signals_exit_and_clean_all_owned_processes_and_files() {
    for signal in ["HUP", "INT", "TERM"] {
        let (output, root) = run_signal_supervisor(signal);
        assert_eq!(
            output.status.code(),
            Some(match signal {
                "HUP" => 129,
                "INT" => 130,
                "TERM" => 143,
                _ => unreachable!(),
            }),
            "signal={signal} stderr={:?}",
            String::from_utf8_lossy(&output.stderr),
        );
        let child_pid = std::fs::read_to_string(root.join("child.pid"))
            .expect("child pid")
            .trim()
            .parse::<u32>()
            .expect("numeric child pid");
        assert!(!process_is_alive(child_pid), "child survived {signal}");
        std::fs::remove_file(root.join("child.pid")).expect("remove child pid evidence");
        assert!(
            std::fs::read_dir(&root)
                .expect("signal test directory")
                .next()
                .is_none(),
            "owned temporary state survived {signal}"
        );
        std::fs::remove_dir_all(root).expect("remove signal test directory");
    }
}

#[test]
fn final_counter_read_failure_runs_cleanup_before_exit() {
    let root = std::env::temp_dir().join(format!(
        "quokka-supervisor-counter-failure-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("counter test directory");
    let child_pid_file = root.join("child.pid");
    let script = format!(
        "{}\nquokka_resource_marker_token=counter-failure\nquokka_action_cgroup=/missing-action-cgroup\nquokka_cgroup_event_value() {{ return 1; }}\nsleep 60 &\nquokka_child_pid=$!\necho \"$quokka_child_pid\" > {}\nquokka_timeout_marker=$(mktemp \"$TMPDIR/quokka-timeout-XXXXXX\")\nquokka_timer_pid_handshake=$(mktemp \"$TMPDIR/quokka-handshake-XXXXXX\")\nquokka_finish_action 0",
        include_str!("../src/resource_supervisor.sh"),
        shell_quote(&child_pid_file.to_string_lossy()),
    );
    let output = Command::new("sh")
        .arg("-c")
        .arg(script)
        .env("TMPDIR", &root)
        .output()
        .expect("counter failure supervisor starts");
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("cgroup setup failure"));
    let child_pid = std::fs::read_to_string(&child_pid_file)
        .expect("child pid")
        .trim()
        .parse::<u32>()
        .expect("numeric child pid");
    assert!(
        !process_is_alive(child_pid),
        "child survived final counter failure"
    );
    std::fs::remove_file(&child_pid_file).expect("remove child pid evidence");
    assert!(
        std::fs::read_dir(&root)
            .expect("counter test directory")
            .next()
            .is_none(),
        "temporary state survived final counter failure"
    );
    std::fs::remove_dir_all(root).expect("remove counter test directory");
}
