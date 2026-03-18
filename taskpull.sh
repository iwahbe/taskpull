#!/usr/bin/env bash
set -euo pipefail

# taskpull — pull-based multi-repo Claude Code task runner
# Continuously assigns standing tasks to Claude Code sessions,
# one per repo at a time, with Remote Control visibility.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TASKS_DIR="${SCRIPT_DIR}/tasks"
STATE_FILE="${SCRIPT_DIR}/state.json"
CONFIG_FILE="${SCRIPT_DIR}/config.toml"

# --- Config ---

poll_interval=300 # default, overridden by config.toml

load_config() {
    if [[ -f "$CONFIG_FILE" ]]; then
        local val
        val=$(grep -E '^\s*poll_interval\s*=' "$CONFIG_FILE" | head -1 | sed 's/.*=\s*//' | tr -d ' ')
        if [[ -n "$val" ]]; then
            poll_interval="$val"
        fi
    fi
}

# --- State helpers ---

ensure_state() {
    if [[ ! -f "$STATE_FILE" ]]; then
        echo '{}' > "$STATE_FILE"
    fi
}

state_get() {
    local task_id="$1" field="$2"
    jq -r --arg id "$task_id" --arg f "$field" '.[$id][$f] // empty' "$STATE_FILE"
}

state_set() {
    local task_id="$1"
    shift
    # Accepts pairs: field1 value1 field2 value2 ...
    local tmp
    tmp=$(mktemp)
    local expr=".[\$id]"
    local args=( --arg id "$task_id" )
    while [[ $# -ge 2 ]]; do
        local field="$1" value="$2"
        shift 2
        # Use --argjson for JSON literals, --arg for strings
        case "$value" in
            true|false|null)
                args+=( --argjson "v_${field}" "$value" )
                ;;
            *)
                if [[ "$value" =~ ^[0-9]+$ ]]; then
                    args+=( --argjson "v_${field}" "$value" )
                else
                    args+=( --arg "v_${field}" "$value" )
                fi
                ;;
        esac
        expr="${expr} | .${field} = \$v_${field}"
    done
    jq "${args[@]}" "$expr" "$STATE_FILE" > "$tmp" && mv "$tmp" "$STATE_FILE"
}

state_init() {
    local task_id="$1"
    local tmp
    tmp=$(mktemp)
    jq --arg id "$task_id" '.[$id] //= {}' "$STATE_FILE" > "$tmp" && mv "$tmp" "$STATE_FILE"
}

# --- Task file parsing ---
# Parses YAML front matter delimited by --- lines.
# Body (prompt) is everything after the second ---.

task_field() {
    local file="$1" field="$2"
    sed -n '/^---$/,/^---$/{ /^---$/d; p; }' "$file" | grep -E "^${field}:" | head -1 | sed "s/^${field}:\s*//"
}

task_prompt() {
    local file="$1"
    # Everything after the second ---
    awk 'BEGIN{n=0} /^---$/{n++; next} n>=2{print}' "$file"
}

task_id_from_file() {
    basename "$1" .md
}

# --- Worktree helpers ---

worktree_path() {
    local repo="$1" branch="$2"
    # Place worktrees in a .taskpull-worktrees dir next to the repo
    local repo_dir
    repo_dir="$(cd "$repo" && pwd)"
    local repo_name
    repo_name="$(basename "$repo_dir")"
    echo "${repo_dir}/../.taskpull-worktrees/${repo_name}/${branch}"
}

cleanup_worktree() {
    local repo="$1" branch="$2"
    local wt
    wt="$(worktree_path "$repo" "$branch")"
    if [[ -d "$wt" ]]; then
        git -C "$repo" worktree remove --force "$wt" 2>/dev/null || rm -rf "$wt"
    fi
    # Delete the local branch if it exists
    git -C "$repo" branch -D "$branch" 2>/dev/null || true
}

cleanup_tmux() {
    local session="$1"
    tmux kill-session -t "$session" 2>/dev/null || true
}

# --- Resolve repo path (expand ~) ---

resolve_repo() {
    echo "${1/#\~/$HOME}"
}

# --- Default branch detection ---

default_branch() {
    local repo="$1"
    local ref
    ref=$(git -C "$repo" symbolic-ref refs/remotes/origin/HEAD 2>/dev/null) || { echo "main"; return; }
    echo "${ref#refs/remotes/origin/}"
}

# --- Core logic ---

phase1_check_prs() {
    log "Phase 1: Checking PRs"
    local task_ids
    task_ids=$(jq -r 'to_entries[] | select(.value.status == "pr-open") | .key' "$STATE_FILE")

    for task_id in $task_ids; do
        local repo pr_number branch
        repo=$(resolve_repo "$(state_get "$task_id" repo)")
        pr_number=$(state_get "$task_id" pr_number)
        branch=$(state_get "$task_id" branch)

        local pr_state
        pr_state=$(gh pr view "$pr_number" --repo "$(git -C "$repo" remote get-url origin)" --json state -q '.state' 2>/dev/null || echo "UNKNOWN")

        if [[ "$pr_state" == "MERGED" ]]; then
            log "  $task_id: PR #$pr_number merged"
            cleanup_worktree "$repo" "$branch"
            cleanup_tmux "taskpull-${task_id}"

            local task_file="${TASKS_DIR}/${task_id}.md"
            local repeat
            repeat=$(task_field "$task_file" "repeat")
            local exhausted
            exhausted=$(state_get "$task_id" exhausted)

            if [[ "$repeat" == "true" && "$exhausted" != "true" ]]; then
                state_set "$task_id" status "idle" pr_number null branch ""
            else
                state_set "$task_id" status "done"
            fi

        elif [[ "$pr_state" == "CLOSED" ]]; then
            log "  $task_id: PR #$pr_number closed without merge"
            cleanup_worktree "$repo" "$branch"
            cleanup_tmux "taskpull-${task_id}"
            state_set "$task_id" status "idle" pr_number null branch ""
        fi
    done
}

phase2_check_sessions() {
    log "Phase 2: Checking active sessions"
    local task_ids
    task_ids=$(jq -r 'to_entries[] | select(.value.status == "active") | .key' "$STATE_FILE")

    for task_id in $task_ids; do
        local tmux_session="taskpull-${task_id}"

        if ! tmux has-session -t "$tmux_session" 2>/dev/null; then
            log "  $task_id: session ended"

            local repo branch
            repo=$(resolve_repo "$(state_get "$task_id" repo)")
            branch=$(state_get "$task_id" branch)
            local wt
            wt="$(worktree_path "$repo" "$branch")"

            # Capture output to check for TASKPULL_DONE
            local pane_output=""
            # tmux session is dead, try to capture from the pane buffer file if we saved it
            local output_file="${SCRIPT_DIR}/.session-output-${task_id}"
            if [[ -f "$output_file" ]]; then
                pane_output=$(cat "$output_file")
                rm -f "$output_file"
            fi

            if echo "$pane_output" | grep -q "TASKPULL_DONE"; then
                log "  $task_id: agent signaled TASKPULL_DONE"
                state_set "$task_id" exhausted true status "idle" branch "" pr_number null
                cleanup_worktree "$repo" "$branch"

            elif [[ -d "$wt" ]] && git -C "$wt" log "origin/$(default_branch "$repo")..HEAD" --oneline 2>/dev/null | grep -q .; then
                # Branch has commits — create PR
                log "  $task_id: has commits, creating PR"
                git -C "$wt" push -u origin "$branch" 2>/dev/null || true

                local run_count
                run_count=$(state_get "$task_id" run_count)
                local remote_url
                remote_url=$(git -C "$repo" remote get-url origin)

                local pr_number
                pr_number=$(gh pr create \
                    --repo "$remote_url" \
                    --head "$branch" \
                    --title "${task_id} (run ${run_count})" \
                    --body "Generated by taskpull, task: ${task_id}" \
                    --json number -q '.number' \
                    2>/dev/null) || true

                if [[ -n "$pr_number" ]]; then
                    state_set "$task_id" status "pr-open" pr_number "$pr_number"
                    log "  $task_id: created PR #$pr_number"
                else
                    log "  $task_id: failed to create PR, resetting"
                    cleanup_worktree "$repo" "$branch"
                    state_set "$task_id" status "idle" branch "" pr_number null
                fi
            else
                log "  $task_id: no commits, resetting"
                cleanup_worktree "$repo" "$branch"
                state_set "$task_id" status "idle" branch "" pr_number null
            fi
        fi
    done
}

phase3_launch() {
    log "Phase 3: Launching new work"

    # Collect repos that are busy (active or pr-open)
    local busy_repos
    busy_repos=$(jq -r 'to_entries[] | select(.value.status == "active" or .value.status == "pr-open") | .value.repo' "$STATE_FILE")

    for task_file in "${TASKS_DIR}"/*.md; do
        [[ -f "$task_file" ]] || continue

        local task_id
        task_id=$(task_id_from_file "$task_file")
        local repo_raw branch_prefix repeat
        repo_raw=$(task_field "$task_file" "repo")
        branch_prefix=$(task_field "$task_file" "branch_prefix")
        repeat=$(task_field "$task_file" "repeat")
        local repo
        repo=$(resolve_repo "$repo_raw")

        # Initialize state if new
        state_init "$task_id"

        local status exhausted
        status=$(state_get "$task_id" status)
        exhausted=$(state_get "$task_id" exhausted)

        # Skip if already busy, done, or exhausted
        if [[ "$status" == "active" || "$status" == "pr-open" || "$status" == "done" ]]; then
            continue
        fi
        if [[ "$exhausted" == "true" ]]; then
            continue
        fi

        # Skip if this repo already has a busy task
        if echo "$busy_repos" | grep -qxF "$repo_raw"; then
            continue
        fi

        # --- Launch ---
        local run_count
        run_count=$(state_get "$task_id" run_count)
        if [[ -z "$run_count" || "$run_count" == "null" ]]; then
            run_count=0
        fi
        run_count=$((run_count + 1))

        local branch="${branch_prefix}-${run_count}"
        local default_br
        default_br=$(default_branch "$repo")

        log "  $task_id: launching run $run_count on $repo (branch: $branch)"

        # Fetch latest
        git -C "$repo" fetch origin 2>/dev/null

        # Create worktree from latest origin/default
        local wt
        wt="$(worktree_path "$repo" "$branch")"
        mkdir -p "$(dirname "$wt")"
        git -C "$repo" worktree add "$wt" -b "$branch" "origin/${default_br}" 2>/dev/null

        # Read prompt
        local prompt
        prompt=$(task_prompt "$task_file")

        if [[ "$repeat" == "true" ]]; then
            prompt="${prompt}

If there is nothing left to do, exit with the message: TASKPULL_DONE"
        fi

        # Output capture file
        local output_file="${SCRIPT_DIR}/.session-output-${task_id}"

        # Write prompt to a file for stdin redirection
        local prompt_file="${SCRIPT_DIR}/.prompt-${task_id}"
        printf '%s' "$prompt" > "$prompt_file"

        # Launch Claude in tmux with Remote Control
        # Pipe prompt via stdin so claude exits on EOF while --remote-control stays active
        local tmux_session="taskpull-${task_id}"
        tmux new-session -d -s "$tmux_session" \
            "cd '$wt' && claude \
                --remote-control \
                --name '${task_id} (run ${run_count})' \
                --allowedTools 'Bash,Read,Write,Edit' \
                < '$prompt_file' \
                2>&1 | tee '$output_file'; \
             rm -f '$prompt_file'; \
             sleep 5"

        # Update state
        state_set "$task_id" \
            status "active" \
            repo "$repo_raw" \
            branch "$branch" \
            pr_number null \
            run_count "$run_count" \
            exhausted false \
            tmux_session "$tmux_session"

        # Add to busy repos so we don't double-schedule
        busy_repos="${busy_repos}
${repo_raw}"
    done
}

# --- Logging ---

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# --- Main ---

main() {
    load_config
    ensure_state

    log "taskpull starting (poll_interval=${poll_interval}s)"
    log "Tasks dir: $TASKS_DIR"
    log "State file: $STATE_FILE"

    while true; do
        log "--- Poll cycle ---"
        phase1_check_prs
        phase2_check_sessions
        phase3_launch
        log "Sleeping ${poll_interval}s"
        sleep "$poll_interval"
    done
}

# Allow running a single cycle for testing
if [[ "${1:-}" == "--once" ]]; then
    load_config
    ensure_state
    log "taskpull: single cycle"
    phase1_check_prs
    phase2_check_sessions
    phase3_launch
    log "Done"
else
    main
fi
