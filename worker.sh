#!/bin/bash

set -euo pipefail
PATH="/usr/local/bin:/usr/bin:${PATH}"

log() {
  local level=$1
  local message=$2
  echo "[$(date -Is)] [$level] $message" >&2
}

usage() {
  cat <<EOF
Usage:
  $0 serial_time <job_dir> <temp_dir> <algo_orig> <iva_value> <idx>
  $0 serial_memory <job_dir> <temp_dir> <algo_orig> <iva_value> <idx>
  $0 thmgr <job_dir> <temp_dir> <repo_name> <core_value> <iva_data> <thmgr_api> <idx>
  $0 parallel_time <job_dir> <temp_dir> <algo> <core_value> <iva_data> <idx>
  $0 parallel_memory <job_dir> <temp_dir> <algo> <core_value> <iva_data> <idx>
  $0 curve_fit <job_dir> <fit_script> <analysis_type>
EOF
  exit 1
}

require_file() {
  local path=$1
  if [[ ! -f "$path" ]]; then
    log "ERROR" "Required file missing: $path"
    exit 2
  fi
}

write_measurement() {
  local path=$1
  local idx=$2
  local kind=$3
  local value=$4
  mkdir -p "$(dirname "$path")"
  printf '%s:%s:%s\n' "$idx" "$kind" "$value" > "$path"
}

convert_peak_to_mb() {
  local raw=$1
  if [[ -z "$raw" ]]; then
    echo "0"
    return
  fi

  local trimmed=${raw// /}
  local number
  number=$(echo "$trimmed" | sed 's/[^0-9.].*$//')
  local suffix=${trimmed#$number}
  local upper_suffix=${suffix^^}

  if [[ -z "$number" ]]; then
    echo "0"
    return
  fi

  case "$upper_suffix" in
    "K"|"KB"|"KIB")
      printf '%.6f\n' "$(echo "$number * 0.001" | bc -l)"
      ;;
    "M"|"MB"|"MIB"|"")
      printf '%.6f\n' "$number"
      ;;
    "B")
      printf '%.6f\n' "$(echo "$number / 1000000" | bc -l)"
      ;;
    *)
      printf '%.6f\n' "$number"
      ;;
  esac
}

run_serial_time() {
  local job_dir=$1
  local temp_dir=$2
  local algo_orig=$3
  local iva_value=$4
  local idx=$5

  local serial_core_count=${SERIAL_CORE_COUNT:-1}
  
  require_file "$job_dir/$algo_orig"
  pushd "$job_dir" >/dev/null

  local start end exec_time
  IFS=',' read -ra iva_args <<< "$iva_value"
  start=$(date +%s.%N)
  "./$algo_orig" "${iva_args[@]}" >/dev/null
  end=$(date +%s.%N)

  exec_time=$(printf '%.8f' "$(echo "$end - $start" | bc -l)")
  popd >/dev/null

  write_measurement "$temp_dir/serial_time_$idx.tmp" "$idx" "time" "$exec_time"
  log "INFO" "Serial time idx=$idx iva=$iva_value time=$exec_time"
}

run_serial_memory() {
  local job_dir=$1
  local temp_dir=$2
  local algo_orig=$3
  local iva_value=$4
  local idx=$5
  local serial_core_count=${SERIAL_CORE_COUNT:-1}

  if ! command -v heaptrack >/dev/null 2>&1; then
    log "ERROR" "heaptrack executable not found on PATH (${PATH})"
    exit 9
  fi

  require_file "$job_dir/$algo_orig"
  pushd "$job_dir" >/dev/null


  local heap_prefix="$temp_dir/serial_heap_$idx"
  IFS=',' read -ra iva_args <<< "$iva_value"
  heaptrack -o "$heap_prefix" "./$algo_orig" "${iva_args[@]}" >/dev/null
  local peak
  peak=$(heaptrack --analyze "${heap_prefix}.zst" | grep "peak heap memory consumption" | awk '{print $5}')
  rm -f "${heap_prefix}.zst"

  popd >/dev/null

  if [[ -z "$peak" ]]; then
    log "ERROR" "Failed to capture serial memory idx=$idx iva=$iva_value"
    exit 3
  fi

  local peak_mb
  peak_mb=$(convert_peak_to_mb "$peak")
  write_measurement "$temp_dir/serial_space_$idx.tmp" "$idx" "space" "$peak_mb"
  log "INFO" "Serial memory idx=$idx iva=$iva_value peak=${peak_mb}MB (raw $peak)"
}

run_thmgr() {
  local job_dir=$1
  local temp_dir=$2
  local repo_name=$3
  local core_value=$4
  local iva_data=$5
  local thmgr_api=$6
  local idx=$7

  local payload response job_id attempt retries interval http_status body status duration

  IFS=',' read -ra iva_args <<< "$iva_data"
  local argv_json='"main"'
  for arg in "${iva_args[@]}"; do
    argv_json+=", \"$arg\""
  done
  argv_json+=", \"$core_value\""

  payload=$(cat <<EOF
{
  "repo": "$repo_name",
  "core": $core_value,
  "argv": [$argv_json]
}
EOF
)

  response=$(curl -s -X POST -H "Content-Type: application/json" -d "$payload" "$thmgr_api/run")
  sleep 3
  job_id=$(echo "$response" | jq -r '.id // empty')

  if [[ -z "$job_id" ]]; then
    log "ERROR" "THMGR submission failed idx=$idx core=$core_value response=$response"
    write_measurement "$temp_dir/parallel_time_$idx.tmp" "$idx" "time" "0"
    exit 4
  fi

  attempt=0
  retries=3600
  interval=1

  while [[ $attempt -lt $retries ]]; do
    response=$(curl -s -w "\nHTTP_STATUS:%{http_code}" "$thmgr_api/job/$job_id" 2>/dev/null)
    http_status=$(echo "$response" | tail -n1 | sed 's/HTTP_STATUS://')
    body=$(echo "$response" | sed '$d')

    if [[ "$http_status" != "200" ]]; then
      log "WARN" "THMGR status http=$http_status job=$job_id attempt=$attempt"
      attempt=$((attempt + 1))
      sleep $interval
      continue
    fi

    if ! echo "$body" | jq empty >/dev/null 2>&1; then
      log "WARN" "Invalid JSON from THMGR job=$job_id attempt=$attempt"
      attempt=$((attempt + 1))
      sleep $interval
      continue
    fi

    status=$(echo "$body" | jq -r '.data.attributes.status // .status // empty')
    if [[ -z "$status" ]]; then
      log "WARN" "THMGR missing status job=$job_id attempt=$attempt"
      attempt=$((attempt + 1))
      sleep $interval
      continue
    fi

    if [[ "$status" == "complete" || "$status" == "completed" ]]; then
      duration=$(echo "$body" | jq -r '.data.attributes.duration // .duration // empty')
      if [[ -n "$duration" && "$duration" != "0" && "$duration" != "0.0" ]]; then
        write_measurement "$temp_dir/parallel_time_$idx.tmp" "$idx" "time" "$duration"
        log "INFO" "THMGR job=$job_id idx=$idx core=$core_value duration=$duration"
        return
      fi

      local start_time end_time start_epoch end_epoch
      start_time=$(echo "$body" | jq -r '.data.attributes.start_time // .start_time // empty')
      end_time=$(echo "$body" | jq -r '.data.attributes.end_time // .end_time // empty')

      if [[ -n "$start_time" && -n "$end_time" ]]; then
        start_epoch=$(date -d "$start_time" +%s.%N 2>/dev/null || echo "")
        end_epoch=$(date -d "$end_time" +%s.%N 2>/dev/null || echo "")
        if [[ -n "$start_epoch" && -n "$end_epoch" ]]; then
          duration=$(printf '%.8f' "$(echo "$end_epoch - $start_epoch" | bc -l)")
          write_measurement "$temp_dir/parallel_time_$idx.tmp" "$idx" "time" "$duration"
          log "INFO" "THMGR job=$job_id idx=$idx computed_duration=$duration"
          return
        fi
      fi
    elif [[ "$status" == "failed" || "$status" == "error" ]]; then
      log "ERROR" "THMGR job=$job_id failed status=$status"
      write_measurement "$temp_dir/parallel_time_$idx.tmp" "$idx" "time" "0"
      exit 5
    fi

    attempt=$((attempt + 1))
    sleep $interval
  done

  log "ERROR" "THMGR job=$job_id timed out after $retries attempts"
  write_measurement "$temp_dir/parallel_time_$idx.tmp" "$idx" "time" "0"
  exit 6
}

run_parallel_time() {
  local job_dir=$1
  local temp_dir=$2
  local algo=$3
  local core_value=$4
  local iva_data=$5
  local idx=$6

  require_file "$job_dir/$algo"
  pushd "$job_dir" >/dev/null

  local start end exec_time
  IFS=',' read -ra iva_args <<< "$iva_data"
  start=$(date +%s.%N)
  "./$algo" "${iva_args[@]}" "$core_value" >/dev/null
  end=$(date +%s.%N)
  exec_time=$(printf '%.8f' "$(echo "$end - $start" | bc -l)")

  popd >/dev/null

  write_measurement "$temp_dir/parallel_time_slow_$idx.tmp" "$idx" "time_slow" "$exec_time"
  log "INFO" "Parallel time idx=$idx core=$core_value time=$exec_time"
}

run_parallel_memory() {
  local job_dir=$1
  local temp_dir=$2
  local algo=$3
  local core_value=$4
  local iva_data=$5
  local idx=$6

  if ! command -v heaptrack >/dev/null 2>&1; then
    log "ERROR" "heaptrack executable not found on PATH (${PATH})"
    exit 9
  fi

  require_file "$job_dir/$algo"
  pushd "$job_dir" >/dev/null

  local heap_prefix="$temp_dir/parallel_heap_$idx"
  IFS=',' read -ra iva_args <<< "$iva_data"
  heaptrack -o "$heap_prefix" "./$algo" "${iva_args[@]}" "$core_value" >/dev/null
  local peak
  peak=$(heaptrack --analyze "${heap_prefix}.zst" | grep "peak heap memory consumption" | awk '{print $5}')
  rm -f "${heap_prefix}.zst"

  popd >/dev/null

  if [[ -z "$peak" ]]; then
    log "ERROR" "Failed to capture parallel memory idx=$idx core=$core_value"
    exit 7
  fi

  local peak_mb
  peak_mb=$(convert_peak_to_mb "$peak")
  write_measurement "$temp_dir/parallel_space_$idx.tmp" "$idx" "space" "$peak_mb"
  log "INFO" "Parallel memory idx=$idx core=$core_value peak=${peak_mb}MB (raw $peak)"
}

run_curve_fit() {
  local job_dir=$1
  local fit_script=$2
  local analysis_type=$3

  require_file "$job_dir/$analysis_type.json"
  local fit_path=$fit_script
  if [[ "$fit_script" != /* ]]; then
    fit_path="$job_dir/$fit_script"
  fi
  require_file "$fit_path"

  pushd "$job_dir" >/dev/null
  python3 "$fit_path" --in-file "$analysis_type.json" --out-file "$analysis_type-fitted.json"
  popd >/dev/null
  log "INFO" "Curve fitting complete for $analysis_type"
}

if [[ $# -lt 1 ]]; then
  usage
fi

task=$1
shift

case "$task" in
  serial_time)
    [[ $# -eq 5 ]] || usage
    run_serial_time "$@"
    ;;
  serial_memory)
    [[ $# -eq 5 ]] || usage
    run_serial_memory "$@"
    ;;
  thmgr)
    [[ $# -eq 7 ]] || usage
    run_thmgr "$@"
    ;;
  parallel_time)
    [[ $# -eq 6 ]] || usage
    run_parallel_time "$@"
    ;;
  parallel_memory)
    [[ $# -eq 6 ]] || usage
    run_parallel_memory "$@"
    ;;
  curve_fit)
    [[ $# -eq 3 ]] || usage
    run_curve_fit "$@"
    ;;
  *)
    log "ERROR" "Unknown task: $task"
    usage
    ;;
esac
