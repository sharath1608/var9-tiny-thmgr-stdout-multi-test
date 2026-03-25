#!/bin/bash
set -o pipefail

usage()
{
  echo "Usage: $0 <algorithm> <iva> <iva data> <iva data file> <core count file> <power profile file> <time serial analytics file> <time parallel analytics file> <time parallel slow analytics file> <space serial analytics file> <space parallel analytics file> <power serial analytics file> <power parallel analytics file> <energy serial analytics file> <energy parallel analytics file> <speedup analytics file> <freeup analytics file> <powerup analytics file> <energyup analytics file> <id> <repo> <repo name> <start time> <progress> <thmgr api> <thmgr lib dir>"
  exit 1
}

check_abort() {
  local repo_path=$1
  local abort_file="$repo_path/do.abort"

  echo "[$$] Checking for abort signal in $abort_file"

  if [[ ! -f "$abort_file" ]]; then
    return 1
  fi

  local flag=$(cat "$abort_file" 2>/dev/null | tr -d '[:space]' | tr '[:upper]' '[:lower]')
  echo "[$$] Abort signal found: $flag"

  if [[ "$flag" == "1" || "$flag" == "true" || "$flag" == "abort" ]]; then
    echo "[$$] abort signal detected, terminating.."

    # if [[ -d "$repo_path" ]]; then
    #   rm -rf "$repo_path"
    #   echo "[$$] Repository directory cleaned up : $repo_path"
    # fi

    return 0
  fi

  return 1
}

call_fit() {
  local in_file=$1
  local out_file=$2
  local progress=$3
  local progress_bandwidth=$4
  local fit_count=$5
  local id=$6
  local repo=$7
  local repo_name=$8
  local start_time=$9
  local analysis_file=${10}

  fit-multivar.py --data "${1}" --model "${1%.*}.pkl" --visualization "${1%.*}.png"
  predict.py --model "${1%.*}.pkl" --data "${1}" --format json --output "${2}" --output-header

  progress=`echo "scale=1; p=$progress; bw=$progress_bandwidth; l=$fit_count; p + (bw/l)" | bc -l`

  echo "{\"id\":\"$id\",\"repo\":\"$repo\",\"repoName\":\"$repo_name\",\"startTime\":\"$start_time\",\
  \"endTime\":\"\",\"status\":\"In progress\",\"progress\":{\"currentStep\":\"Predictive Model Generation\",\
  \"nextStep\":\"None\",\"percent\":$progress},\
  \"result\":{\"errorCode\":0,\"message\":\"\",\"repo\":\"\"}}" > $analysis_file
}

read_summary_array() {
  local dest_name=$1
  local jq_expr=$2
  local -n dest_ref=$dest_name

  dest_ref=()
  if [[ -z "${MEASUREMENT_SUMMARY:-}" || ! -f "$MEASUREMENT_SUMMARY" ]]; then
    return
  fi

  mapfile -t dest_ref < <(jq -r "(${jq_expr} // []) | .[]" "$MEASUREMENT_SUMMARY")
}

value_or_default() {
  local array_name=$1
  local index=$2
  local default_value=$3
  local -n array_ref=$array_name

  if (( index < ${#array_ref[@]} )); then
    echo "${array_ref[$index]}"
  else
    echo "$default_value"
  fi
}

power_for_core() {
  local core_value=$1
  local index=$((core_value - 1))
  if (( index < 0 || index >= ${#power_profile[@]} )); then
    echo "${power_profile[0]}"
  else
    echo "${power_profile[$index]}"
  fi
}

if [ "$#" -ne 32 ]; then
    echo "Invalid number of parameters. Expected:32 Passed:$#"
    usage
fi

algo=$1
main_file=$2
target_fn=$3
target_fn_iva_name=$4
target_fn_iva_start=$5
target_fn_iva_end=$6
argc=$7
iva_name=$8
iva_data=$9
iva_data_file=${10}
core_count_file=${11}
power_profile_file=${12}
time_serial_analytics_file=${13}
time_parallel_analytics_file=${14}
time_parallel_slow_analytics_file=${15}
space_serial_analytics_file=${16}
space_parallel_analytics_file=${17}
power_serial_analytics_file=${18}
power_parallel_analytics_file=${19}
energy_serial_analytics_file=${20}
energy_parallel_analytics_file=${21}
speedup_analytics_file=${22}
freeup_analytics_file=${23}
powerup_analytics_file=${24}
energyup_analytics_file=${25}
id=${26}
repo=${27}
repo_name=${28}
start_time=${29}
progress=${30}
thmgr_api=${31}
thmgr_lib_dir=${32}

serial_measurement=serial.csv
parallel_measurement=parallel.csv
parallel_slow_measurement=parallel_slow.csv
analysis_file=analysis.json

# parallel code generation config
parallel_plugin_so=MyRewriter.so
parallel_plugin_name=rew

echo "cleaning up"

# cleanup
rm -f $time_serial_analytics_file $time_parallel_analytics_file $time_parallel_slow_analytics_file $space_serial_analytics_file \
   $space_parallel_analytics_file $power_serial_analytics_file $power_parallel_analytics_file \
   $energy_serial_analytics_file $energy_parallel_analytics_file $speedup_analytics_file \
   $freeup_analytics_file $powerup_analytics_file $energyup_analytics_file \
   $serial_measurement $parallel_measurement $parallel_slow_measurement

echo "cleanup done"

{ IFS=, read -ra iva_arr_names; readarray -t iva_arr; } < $iva_data_file
readarray -t core_arr < $core_count_file

echo "read array files"

power_profile=()

while IFS=, read -r i p;
do power_profile+=($p);
done < $power_profile_file

# Extract first column from iva_arr for analytics JSON outputs
iva_first=()
for line in "${iva_arr[@]}"
do
  IFS=, read -ra cols <<< "$line"
  iva_first+=("${cols[0]}")
done

iva=()
core=()

for line in "${iva_arr[@]}"
do
  IFS=, read -ra cols <<< "$line"
  iva+=("${cols[0]}")
done

for i in ${core_arr[@]}
do
  core+=($i)
done

# generate compile_commands.json
repo_path="/data/repo-import/$repo_name"

json_output=$(jq -n \
  --arg dir "$repo_path" \
  '[
     {
       "directory": $dir,
       "command": "clang-18 -g -O0 -I/usr/include -c main_original.c",
       "file": "main_original.c"
     }
   ]')

# Output the JSON to a file
echo "$json_output" > compile_commands.json
echo "JSON data written to compile_commands.json"

# TALP coverage generation - start
echo "TALP coverage generation"

cp main_original.c main_original.bak.c
argv-to-klee main_original.c > main_original.c.tmp && mv main_original.c.tmp main_original.c

if docker exec talp-cov sh -c "cd $repo_path && analyze main_original.c"; then
  echo "TALP coverage generated successfully"
else
  echo "TALP coverage generation failed with exit code $?"
fi

mv main_original.bak.c main_original.c

for dir in klee-out-*-replay-*; do
    if [ -d "$dir" ]; then
        klee_dir="$dir"
        break
    fi
done
echo "$klee_dir"

cov-callgraph-generator --cov-file "$klee_dir/test000001-replay/test000001.cov" --output-file test000001.covgraph.json -p .
# TALP coverage generation - end

# Generate parallel analysis
#/usr/bin/clang-18 -g -emit-llvm -S -o main_original.ll main_original.c
#/usr/bin/opt-18 -load-pass-plugin=GanymedeAnalysisPlugin.so -passes="ganymede-analysis" main_original.ll

# Generate standalone parallel code
#/usr/bin/ganymede-codegen --analysis-file=parallelization_analysis.json --codegen-type=standalone main_original.c > main.c

# HACK - START
# HACK - fusion currently supports a standalone threadpool
# HACK - Remove when fusion supports a shared threadpool

# Generate thread manager parallel code
#/usr/bin/ganymede-codegen --analysis-file=parallelization_analysis.json --codegen-type=thmgr main_original.c > main_service.c
#cp main.c main_service.c
#sed -i 's/main(int/main_worker(int/' main_service.c
#sed -i 's/atoi(argv\[argc-1\])/atoi(argv[argc-2])/' main_service.c

# HACK -END

# make - serial
make -f Makefile-serial

# make a copy of original main file
main_file_extn="${main_file##*.}"
main_file_noextn="${main_file%.*}"
main_file_orig="$main_file_noextn"_original."$main_file_extn"
#cp $main_file $main_file_orig

# make a copy of original execuatble
algo_orig="$algo"_original
#mv $algo $algo_orig

# make - parallel
make -f Makefile-parallel

# make - thread manager service
make -f Makefile-thmgr repo_name=$repo_name lib_location=$thmgr_lib_dir

# Distributed measurement via controller
SERIAL_PROGRESS=25
PARALLEL_THMGR_PROGRESS=20
PARALLEL_DIRECT_PROGRESS=20
CURVE_FIT_PROGRESS=25
ANALYTICS_GENERATION_PROGRESS=10

repo_path="/data/repo-import/$repo_name"
delay=0  # No delay needed with per-request socket implementation

cluster_config_path="config/cluster.json"
task_config_path="config/task_distribution.json"
profiler_config_path="config/profiler.json"
worker_script_path="./worker.sh"
fit_script_path="/usr/bin/fit.py"

safe_repo_name=$(echo "$repo_name" | tr -cs '[:alnum:]' '_')
job_root="jobs/${safe_repo_name}_${id}"
shared_mount=$(jq -r '.shared_storage.mount_point' "$cluster_config_path")

if [[ -z "$shared_mount" || "$shared_mount" == "null" ]]; then
  echo "Error: shared storage mount point not defined in $cluster_config_path" >&2
  exit 1
fi

job_dir="$shared_mount/$job_root"
temp_dir_path="$job_dir/temp"

if [[ -d "$job_dir" ]]; then
  echo "Removing existing job workspace at $job_dir"
  rm -rf "$job_dir"
fi

if check_abort $repo_path; then exit 2; fi

echo "Loading repository into thread manager..."
load_response=$(curl -s -X POST -H "Content-Type: application/json" -d @- $thmgr_api/load <<EOF_LOAD
{
  "repo": "$repo_name"
}
EOF_LOAD
)
echo "Load response: $load_response"

current_progress=$progress

echo "printing distributed profiler parameters"
echo "cluster_config_path: $cluster_config_path"
echo "task_config_path: $task_config_path"
echo "profiler_config_path: $profiler_config_path"
echo "worker_script_path: $worker_script_path"
echo "fit_script_path: $fit_script_path"
echo "job_id: $id"
echo "job_root: $job_root"
echo "repo: $repo"
echo "repo_name: $repo_name"
echo "repo_path: $repo_path"
echo "analysis_file: $analysis_file"
echo "start_time: $start_time"
echo "current_progress: $current_progress"
echo "algo: $algo"
echo "algo_original: $algo_orig"
echo "iva_data: $iva_data"
echo "thmgr_api: $thmgr_api"
echo "iva_values: ${iva_arr[@]}"
echo "core_values: ${core[@]}"
echo "iva_data_file: $iva_data_file"
echo "core_count_file: $core_count_file"
echo "power_profile_file: $power_profile_file"
echo "serial_progress: $SERIAL_PROGRESS"
echo "request_delay: $delay"
echo "thmgr_progress: $PARALLEL_THMGR_PROGRESS"
echo "direct_progress: $PARALLEL_DIRECT_PROGRESS"

echo "Measuring time taken for distributed execution..."
st=$SECONDS

# Create log file for distributed profiler in the repo directory
distributed_log_file="distributed_profiler.log"
echo "Distributed profiler logs will be saved to: $distributed_log_file"

python3 distributed_profiler.py \
  --mode measurement \
  --cluster-config "$cluster_config_path" \
  --task-config "$task_config_path" \
  --profiler-config "$profiler_config_path" \
  --worker-script "$worker_script_path" \
  --fit-script "$fit_script_path" \
  --job-id "$id" \
  --job-root "$job_root" \
  --repo "$repo" \
  --repo-name "$repo_name" \
  --repo-path "$repo_path" \
  --analysis-file "$analysis_file" \
  --start-time "$start_time" \
  --start-progress "$current_progress" \
  --algo "$algo" \
  --algo-original "$algo_orig" \
  --iva-data "$iva_data" \
  --thmgr-api "$thmgr_api" \
  --iva-values "${iva_arr[@]}" \
  --core-values "${core[@]}" \
  --iva-data-file "$iva_data_file" \
  --core_count_file "$core_count_file" \
  --power_profile_file "$power_profile_file" \
  --serial-progress "$SERIAL_PROGRESS" \
  --request-delay "$delay" \
  --thmgr-progress "$PARALLEL_THMGR_PROGRESS" \
  --direct-progress "$PARALLEL_DIRECT_PROGRESS" \
  -vv 2>&1 | tee "$distributed_log_file" || {
    echo "Distributed measurement phase failed" >&2
    echo "Last 50 lines of distributed profiler log:" >&2
    tail -50 "$distributed_log_file" >&2
    exit 1
  }

elapsed_seconds=$(( SECONDS - st ))
echo "Elapsed time: $elapsed_seconds seconds"

echo "Distributed measurement phase completed"

MEASUREMENT_SUMMARY="$job_dir/measurement_summary.json"
if [[ ! -f "$MEASUREMENT_SUMMARY" ]]; then
  echo "Error: measurement summary not found at $MEASUREMENT_SUMMARY" >&2
  exit 1
fi

TEMP_DIR="$temp_dir_path"

read_summary_array iva_summary ".iva"
if [[ ${#iva_summary[@]} -gt 0 ]]; then
  iva=("${iva_summary[@]}")
fi

read_summary_array core_summary ".core"
if [[ ${#core_summary[@]} -gt 0 ]]; then
  core=("${core_summary[@]}")
fi

read_summary_array time_serial ".time_serial"
read_summary_array space_serial ".space_serial"
read_summary_array power_serial ".power_serial"
read_summary_array energy_serial ".energy_serial"
read_summary_array time_parallel ".time_parallel"
read_summary_array time_parallel_slow ".time_parallel_slow"
read_summary_array space_parallel ".space_parallel"
read_summary_array power_parallel ".power_parallel"
read_summary_array energy_parallel ".energy_parallel"
read_summary_array speedup ".speedup"
read_summary_array freeup ".freeup"
read_summary_array powerup ".powerup"
read_summary_array energyup ".energyup"

if [[ ${#time_serial[@]} -eq 0 ]]; then
  echo "Error: serial measurements unavailable in summary" >&2
  exit 1
fi

if [[ ${#time_parallel[@]} -eq 0 ]]; then
  echo "Warning: parallel THMGR measurements unavailable; falling back to direct timings" >&2
  time_parallel=("${time_parallel_slow[@]}")
fi

if [[ ${#power_serial[@]} -eq 0 ]]; then
  power_serial=()
  for ((idx=0; idx<${#iva[@]}; idx++)); do
    power_serial+=(${power_profile[0]})
  done
fi

if [[ ${#power_parallel[@]} -eq 0 ]]; then
  power_parallel=()
  for ((idx=0; idx<${#core[@]}; idx++)); do
    power_parallel+=("$(power_for_core "${core[$idx]}")")
  done
fi

if [[ ${#energy_serial[@]} -ne ${#time_serial[@]} ]]; then
  energy_serial=()
  for ((idx=0; idx<${#time_serial[@]}; idx++)); do
    ts=${time_serial[$idx]}
    ps=$(value_or_default power_serial "$idx" "${power_profile[0]}")
    energy_serial+=("$(printf "%.8f" "$(echo "$ts * $ps" | bc -l)")")
  done
fi

if [[ ${#energy_parallel[@]} -ne ${#time_parallel[@]} ]]; then
  energy_parallel=()
  for ((idx=0; idx<${#time_parallel[@]}; idx++)); do
    tp=${time_parallel[$idx]}
    pp=$(value_or_default power_parallel "$idx" "$(power_for_core "${core[$idx]}")")
    energy_parallel+=("$(printf "%.8f" "$(echo "$tp * $pp" | bc -l)")")
  done
fi

if [[ ${#speedup[@]} -ne ${#core[@]} ]]; then
  speedup=()
  base_time=$(value_or_default time_parallel 0 "1")
  for val in "${time_parallel[@]}"; do
    speedup+=("$(printf "%.6f" "$(echo "$base_time / $val" | bc -l)")")
  done
fi

if [[ ${#freeup[@]} -ne ${#core[@]} ]]; then
  freeup=()
  base_space=$(value_or_default space_parallel 0 "1")
  for val in "${space_parallel[@]}"; do
    freeup+=("$(printf "%.6f" "$(echo "$base_space / $val" | bc -l)")")
  done
fi

if [[ ${#powerup[@]} -ne ${#core[@]} ]]; then
  powerup=()
  base_power=$(value_or_default power_parallel 0 "$(power_for_core "${core[0]}")")
  for val in "${power_parallel[@]}"; do
    powerup+=("$(printf "%.6f" "$(echo "$base_power / $val" | bc -l)")")
  done
fi

if [[ ${#energyup[@]} -ne ${#core[@]} ]]; then
  energyup=()
  base_time=$(value_or_default time_parallel 0 "1")
  base_power=$(value_or_default power_parallel 0 "$(power_for_core "${core[0]}")")
  base_energy=$(printf "%.8f" "$(echo "$base_time * $base_power" | bc -l)")
  for val in "${energy_parallel[@]}"; do
    energyup+=("$(printf "%.6f" "$(echo "$base_energy / $val" | bc -l)")")
  done
fi

> "$serial_measurement"
for ((idx=0; idx<${#iva[@]}; idx++)); do
  ts=$(value_or_default time_serial "$idx" "0.001")
  ss=$(value_or_default space_serial "$idx" "1")
  ps=$(value_or_default power_serial "$idx" "${power_profile[0]}")
  es=$(value_or_default energy_serial "$idx" "")
  if [[ -z "$es" ]]; then
    es=$(printf "%.8f" "$(echo "$ts * $ps" | bc -l)")
  fi
  printf "%s,%s,%s,%s,%s\n" "${iva_arr[$idx]}" "$ts" "$ss" "$ps" "$es" >> "$serial_measurement"
done

> "$parallel_measurement"
for ((idx=0; idx<${#core[@]}; idx++)); do
  tp=$(value_or_default time_parallel "$idx" "0.001")
  sp=$(value_or_default space_parallel "$idx" "1")
  pp=$(value_or_default power_parallel "$idx" "$(power_for_core "${core[$idx]}")")
  ep=$(value_or_default energy_parallel "$idx" "")
  if [[ -z "$ep" ]]; then
    ep=$(printf "%.8f" "$(echo "$tp * $pp" | bc -l)")
  fi
  printf "%s,%s,%s,%s,%s\n" "${core[$idx]}" "$tp" "$sp" "$pp" "$ep" >> "$parallel_measurement"
done

> "$parallel_slow_measurement"
for ((idx=0; idx<${#core[@]}; idx++)); do
  tp_slow=$(value_or_default time_parallel_slow "$idx" "$(value_or_default time_parallel "$idx" "0.001")")
  printf "%s,%s\n" "${core[$idx]}" "$tp_slow" >> "$parallel_slow_measurement"
done

echo "Measurements collected"

# Note: Distributed profiler already updated progress to include measurements (65%)
# and set currentStep to "Curve Fitting", so we don't overwrite it here

# Use iva_arr_names from the IVA data file header as input variable names
input_var_names=("${iva_arr_names[@]}")

# Generate CSV files for fit-multivar.py

# time-serial.csv
echo "$(IFS=,; echo "${input_var_names[*]}"),time" > time-serial.csv
for i in "${!iva_arr[@]}"; do
  echo "${iva_arr[$i]},${time_serial[$i]}" >> time-serial.csv
done
FILE="time-serial.csv"
NCOLS=$(head -1 "$FILE" | awk -F',' '{print NF}')
(head -n 1 "$FILE" && tail -n +2 "$FILE" | sort -t',' -k"$NCOLS" -n) > "${FILE}.tmp" && mv "${FILE}.tmp" "$FILE"

# time-parallel.csv
echo "core,time" > time-parallel.csv
for i in "${!core[@]}"; do
  echo "${core[$i]},${time_parallel[$i]}" >> time-parallel.csv
done

# time-parallel-slow.csv
echo "core,time" > time-parallel-slow.csv
for i in "${!core[@]}"; do
  echo "${core[$i]},${time_parallel_slow[$i]}" >> time-parallel-slow.csv
done

# space-serial.csv
echo "$(IFS=,; echo "${input_var_names[*]}"),memory" > space-serial.csv
for i in "${!iva_arr[@]}"; do
  echo "${iva_arr[$i]},${space_serial[$i]}" >> space-serial.csv
done
FILE="space-serial.csv"
NCOLS=$(head -1 "$FILE" | awk -F',' '{print NF}')
(head -n 1 "$FILE" && tail -n +2 "$FILE" | sort -t',' -k"$NCOLS" -n) > "${FILE}.tmp" && mv "${FILE}.tmp" "$FILE"

# space-parallel.csv
echo "core,memory" > space-parallel.csv
for i in "${!core[@]}"; do
  echo "${core[$i]},${space_parallel[$i]}" >> space-parallel.csv
done

# power-serial.csv
echo "$(IFS=,; echo "${input_var_names[*]}"),power" > power-serial.csv
for i in "${!iva_arr[@]}"; do
  echo "${iva_arr[$i]},${power_serial[$i]}" >> power-serial.csv
done
FILE="power-serial.csv"
NCOLS=$(head -1 "$FILE" | awk -F',' '{print NF}')
(head -n 1 "$FILE" && tail -n +2 "$FILE" | sort -t',' -k"$NCOLS" -n) > "${FILE}.tmp" && mv "${FILE}.tmp" "$FILE"

# power-parallel.csv
echo "core,power" > power-parallel.csv
for i in "${!core[@]}"; do
  echo "${core[$i]},${power_parallel[$i]}" >> power-parallel.csv
done

# energy-serial.csv
echo "$(IFS=,; echo "${input_var_names[*]}"),energy" > energy-serial.csv
for i in "${!iva_arr[@]}"; do
  echo "${iva_arr[$i]},${energy_serial[$i]}" >> energy-serial.csv
done
FILE="energy-serial.csv"
NCOLS=$(head -1 "$FILE" | awk -F',' '{print NF}')
(head -n 1 "$FILE" && tail -n +2 "$FILE" | sort -t',' -k"$NCOLS" -n) > "${FILE}.tmp" && mv "${FILE}.tmp" "$FILE"

# energy-parallel.csv
echo "core,energy" > energy-parallel.csv
for i in "${!core[@]}"; do
  echo "${core[$i]},${energy_parallel[$i]}" >> energy-parallel.csv
done

# speedup.csv
echo "core,time" > speedup.csv
for i in "${!core[@]}"; do
  echo "${core[$i]},${speedup[$i]}" >> speedup.csv
done

# freeup.csv
echo "core,memory" > freeup.csv
for i in "${!core[@]}"; do
  echo "${core[$i]},${freeup[$i]}" >> freeup.csv
done

# powerup.csv
echo "core,power" > powerup.csv
for i in "${!core[@]}"; do
  echo "${core[$i]},${powerup[$i]}" >> powerup.csv
done

# energyup.csv
echo "core,energy" > energyup.csv
for i in "${!core[@]}"; do
  echo "${core[$i]},${energyup[$i]}" >> energyup.csv
done

# Get current progress from analysis file (should be 65% after distributed measurements)
current_progress=$(jq -r '.progress.percent' "$analysis_file" 2>/dev/null || echo "$progress")

progress_bandwidth=$CURVE_FIT_PROGRESS
fit_count=12

analysis_types=("time-serial" "time-parallel" "space-serial" "space-parallel" "power-serial" "power-parallel" "energy-serial" "energy-parallel" "speedup" "freeup" "powerup" "energyup")

for i in "${analysis_types[@]}"
do
  echo "${i}.csv"
  echo "${i}-fitted.json"
  call_fit $i.csv $i-fitted.json $current_progress $progress_bandwidth $fit_count $id $repo $repo_name $start_time $analysis_file
done

# Build iva JSON array from iva_arr and iva_arr_names
iva_json_parts=()
for col_idx in "${!iva_arr_names[@]}"; do
  col_data=()
  for line in "${iva_arr[@]}"; do
    IFS=, read -ra cols <<< "$line"
    col_data+=("${cols[$col_idx]}")
  done
  iva_json_parts+=("$(jo data="$(jo -a ${col_data[@]})" name="${iva_arr_names[$col_idx]}" unit=size)")
done
iva_json="$(jo -a "${iva_json_parts[@]}")"

# time serial
extn="${time_serial_analytics_file##*.}"
noextn="${time_serial_analytics_file%.*}"

time_serial_analytics_file_d="$noextn"."$extn"

jo -p \
iva="$iva_json" \
measurements=$(jo data=$(jo -a ${time_serial[@]}) name=time unit=seconds) \
fitted=$(jo data="`jq '.fitted' time-serial-fitted.json`" name=time unit=seconds) \
unoptimized=$(jo data=$(jo -a) name=time unit=seconds) \
fit_method="`jq -r '.method' time-serial-fitted.json`" \
mse="`jq '.mse' time-serial-fitted.json`" \
> $time_serial_analytics_file_d

# time parallel
extn="${time_parallel_analytics_file##*.}"
noextn="${time_parallel_analytics_file%.*}"

time_parallel_analytics_file_d="$noextn"."$extn"

jo -p \
iva="$(jo -a "$(jo data="$(jo -a ${core[@]})" name=core unit=count)")" \
measurements=$(jo data=$(jo -a ${time_parallel[@]}) name=time unit=seconds) \
unoptimized=$(jo data=$(jo -a ${time_parallel_slow[@]}) name=time unit=seconds) \
fitted=$(jo data="`jq '.fitted' time-parallel-fitted.json`" name=time unit=seconds) \
fit_method="`jq -r '.method' time-parallel-fitted.json`" \
mse="`jq '.mse' time-parallel-fitted.json`" \
> $time_parallel_analytics_file_d

# memory serial
extn="${space_serial_analytics_file##*.}"
noextn="${space_serial_analytics_file%.*}"

space_serial_analytics_file_d="$noextn"."$extn"

jo -p \
iva="$iva_json" \
measurements=$(jo data=$(jo -a ${space_serial[@]}) name=memory unit=MB) \
unoptimized=$(jo data=$(jo -a) name=memory unit=MB) \
fitted=$(jo data="`jq '.fitted' space-serial-fitted.json`" name=memory unit=MB) \
fit_method="`jq -r '.method' space-serial-fitted.json`" \
mse="`jq '.mse' space-serial-fitted.json`" \
> $space_serial_analytics_file_d

# memory parallel
extn="${space_parallel_analytics_file##*.}"
noextn="${space_parallel_analytics_file%.*}"

space_parallel_analytics_file_d="$noextn"."$extn"

jo -p \
iva="$(jo -a "$(jo data="$(jo -a ${core[@]})" name=core unit=count)")" \
measurements=$(jo data=$(jo -a ${space_parallel[@]}) name=memory unit=MB) \
unoptimized=$(jo data=$(jo -a) name=memory unit=MB) \
fitted=$(jo data="`jq '.fitted' space-parallel-fitted.json`" name=memory unit=MB) \
fit_method="`jq -r '.method' space-parallel-fitted.json`" \
mse="`jq '.mse' space-parallel-fitted.json`" \
> $space_parallel_analytics_file_d

# power serial
extn="${power_serial_analytics_file##*.}"
noextn="${power_serial_analytics_file%.*}"

power_serial_analytics_file_d="$noextn"."$extn"

jo -p \
iva="$iva_json" \
measurements=$(jo data=$(jo -a ${power_serial[@]}) name=power unit="watts") \
unoptimized=$(jo data=$(jo -a) name=power unit="watts") \
fitted=$(jo data="`jq '.fitted' power-serial-fitted.json`" name=power unit="watts") \
fit_method="`jq -r '.method' power-serial-fitted.json`" \
mse="`jq '.mse' power-serial-fitted.json`" \
> $power_serial_analytics_file_d

# power parallel
extn="${power_parallel_analytics_file##*.}"
noextn="${power_parallel_analytics_file%.*}"

power_parallel_analytics_file_d="$noextn"."$extn"

jo -p \
iva="$(jo -a "$(jo data="$(jo -a ${core[@]})" name=core unit=count)")" \
measurements=$(jo data=$(jo -a ${power_parallel[@]}) name=power unit="watts") \
fitted=$(jo data="`jq '.fitted' power-parallel-fitted.json`" name=power unit="watts") \
unoptimized=$(jo data=$(jo -a) name=power unit="watts") \
fit_method="`jq -r '.method' power-parallel-fitted.json`" \
mse="`jq '.mse' power-parallel-fitted.json`" \
> $power_parallel_analytics_file_d

# energy serial
extn="${energy_serial_analytics_file##*.}"
noextn="${energy_serial_analytics_file%.*}"

energy_serial_analytics_file_d="$noextn"."$extn"

jo -p \
iva="$iva_json" \
measurements=$(jo data=$(jo -a ${energy_serial[@]}) name=energy unit="watt-seconds") \
unoptimized=$(jo data=$(jo -a) name=energy unit="watt-seconds") \
fitted=$(jo data="`jq '.fitted' energy-serial-fitted.json`" name=energy unit="watt-seconds") \
fit_method="`jq -r '.method' energy-serial-fitted.json`" \
mse="`jq '.mse' energy-serial-fitted.json`" \
> $energy_serial_analytics_file_d

# energy parallel
extn="${energy_parallel_analytics_file##*.}"
noextn="${energy_parallel_analytics_file%.*}"

energy_parallel_analytics_file_d="$noextn"."$extn"

jo -p \
iva="$(jo -a "$(jo data="$(jo -a ${core[@]})" name=core unit=count)")" \
measurements=$(jo data=$(jo -a ${energy_parallel[@]}) name=energy unit="watt-seconds") \
unoptimized=$(jo data=$(jo -a) name=energy unit="watt-seconds") \
fitted=$(jo data="`jq '.fitted' energy-parallel-fitted.json`" name=energy unit="watt-seconds") \
fit_method="`jq -r '.method' energy-parallel-fitted.json`" \
mse="`jq '.mse' energy-parallel-fitted.json`" \
> $energy_parallel_analytics_file_d

# speedup
extn="${speedup_analytics_file##*.}"
noextn="${speedup_analytics_file%.*}"

speedup_analytics_file_d="$noextn"."$extn"

jo -p \
iva="$(jo -a "$(jo data="$(jo -a ${core[@]})" name=core unit=count)")" \
measurements=$(jo data=$(jo -a ${speedup[@]}) name='T1/Tcore' unit='') \
unoptimized=$(jo data=$(jo -a) name='T1/Tcore' unit='') \
fitted=$(jo data="`jq '.fitted' speedup-fitted.json`" name='T1/Tcore' unit='') \
fit_method="`jq -r '.method' speedup-fitted.json`" \
mse="`jq '.mse' speedup-fitted.json`" \
> $speedup_analytics_file_d

# freeup
extn="${freeup_analytics_file##*.}"
noextn="${freeup_analytics_file%.*}"

freeup_analytics_file_d="$noextn"."$extn"

jo -p \
iva="$(jo -a "$(jo data="$(jo -a ${core[@]})" name=core unit=count)")" \
measurements=$(jo data=$(jo -a ${freeup[@]}) name='S1/Score' unit='') \
unoptimized=$(jo data=$(jo -a) name='S1/Score' unit='') \
fitted=$(jo data="`jq '.fitted' freeup-fitted.json`" name='S1/Score' unit='') \
fit_method="`jq -r '.method' freeup-fitted.json`" \
mse="`jq '.mse' freeup-fitted.json`" \
> $freeup_analytics_file_d

# powerup
extn="${powerup_analytics_file##*.}"
noextn="${powerup_analytics_file%.*}"

powerup_analytics_file_d="$noextn"."$extn"

jo -p \
iva="$(jo -a "$(jo data="$(jo -a ${core[@]})" name=core unit=count)")" \
measurements=$(jo data=$(jo -a ${powerup[@]}) name='PowerEfficiency(P1/Pcore)' unit='') \
unoptimized=$(jo data=$(jo -a) name='PowerEfficiency(P1/Pcore)' unit='') \
fitted=$(jo data="`jq '.fitted' powerup-fitted.json`" name='PowerEfficiency(P1/Pcore)' unit='') \
fit_method="`jq -r '.method' powerup-fitted.json`" \
mse="`jq '.mse' powerup-fitted.json`" \
> $powerup_analytics_file_d

# energyup
extn="${energyup_analytics_file##*.}"
noextn="${energyup_analytics_file%.*}"

energyup_analytics_file_d="$noextn"."$extn"

jo -p \
iva="$(jo -a "$(jo data="$(jo -a ${core[@]})" name=core unit=count)")" \
measurements=$(jo data=$(jo -a ${energyup[@]}) name='EnergyEfficiency(E1/Ecore)' unit='') \
unoptimized=$(jo data=$(jo -a) name='EnergyEfficiency(E1/Ecore)' unit='') \
fitted=$(jo data="`jq '.fitted' energyup-fitted.json`" name='EnergyEfficiency(E1/Ecore)' unit='') \
fit_method="`jq -r '.method' energyup-fitted.json`" \
mse="`jq '.mse' energyup-fitted.json`" \
> $energyup_analytics_file_d

echo "Analytics generation complete! Orion.cpp will finalize status."
