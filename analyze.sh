#!/bin/bash

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

#  fit.py --in-file "${1}" --out-file "${2}" --format csv
  fit-multivar.py --data "${in_file}" --model "${in_file%.*}.pkl" --visualization "${in_file%.*}.png"
  predict.py --model "${in_file%.*}.pkl" --data "${in_file}" --format json --output "${out_file}" --output-header

  progress=`echo "scale=1; p=$progress; bw=$progress_bandwidth; l=$fit_count; p + (bw/l)" | bc -l`

  echo "{\"id\":\"$id\",\"repo\":\"$repo\",\"repoName\":\"$repo_name\",\"startTime\":\"$start_time\",\
  \"endTime\":\"\",\"status\":\"In progress\",\"progress\":{\"currentStep\":\"Predictive Model Generation\",\
  \"nextStep\":\"None\",\"percent\":$progress},\
  \"result\":{\"errorCode\":0,\"message\":\"\",\"repo\":\"\"}}" > $analysis_file
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

{ read; readarray -t iva_arr; } < $iva_data_file
readarray -t core_arr < $core_count_file

echo "read array files"

power_profile=()

while IFS=, read -r i p;
do power_profile+=($p);
done < $power_profile_file

core=()

for i in ${core_arr[@]}
do
  core+=($i)
done

# Extract first column from iva_arr for analytics JSON outputs
iva_first=()
for line in "${iva_arr[@]}"
do
  IFS=, read -ra cols <<< "$line"
  iva_first+=("${cols[0]}")
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

# serial run

time_serial=()
space_serial=()
power_serial=()
energy_serial=()

repo_path="/app/data/repo-import/$repo_name"

if check_abort $repo_path; then exit 2; fi

# time - serial
progress_bandwidth=10

echo "starting.."

for iva_line in "${iva_arr[@]}"
do
  # Parse CSV line into array
  IFS=, read -ra iva_cols <<< "$iva_line"

  # time
  start=`date +%s.%N`;\
  ./$algo_orig ${iva_cols[@]};\
  end=`date +%s.%N`;\
  time_serial+=(`printf '%.8f' $( echo "$end - $start" | bc -l )`);

  progress=`echo "scale=1; p=$progress; bw=$progress_bandwidth; l=${#iva_arr[@]}; p + (bw/l)" | bc -l`

  echo "{\"id\":\"$id\",\"repo\":\"$repo\",\"repoName\":\"$repo_name\",\"startTime\":\"$start_time\",\
  \"endTime\":\"\",\"status\":\"In progress\",\"progress\":{\"currentStep\":\"Serial Time Measurement\",\
  \"nextStep\":\"Serial Memory Measurement\",\"percent\":$progress},\
  \"result\":{\"errorCode\":0,\"message\":\"\",\"repo\":\"\"}}" > $analysis_file
done

if check_abort $repo_path; then exit 2; fi

# memory - serial
progress_bandwidth=10

count=1
for iva_line in "${iva_arr[@]}"
do
  # Parse CSV line into array
  IFS=, read -ra iva_cols <<< "$iva_line"

  # memory
  heaptrack -o "$algo.$count" ./$algo_orig ${iva_cols[@]};\
  space_serial+=(`heaptrack --analyze "$algo.$count.zst"  | grep "peak heap memory consumption" | awk '{print $5}'`);
  count=$((count+1))

  progress=`echo "scale=1; p=$progress; bw=$progress_bandwidth; l=${#iva_arr[@]}; p + (bw/l)" | bc -l`

  echo "{\"id\":\"$id\",\"repo\":\"$repo\",\"repoName\":\"$repo_name\",\"startTime\":\"$start_time\",\
  \"endTime\":\"\",\"status\":\"In progress\",\"progress\":{\"currentStep\":\"Serial Memory Measurement\",\
  \"nextStep\":\"Serial Power Measurement\",\"percent\":$progress},\
  \"result\":{\"errorCode\":0,\"message\":\"\",\"repo\":\"\"}}" > $analysis_file

done

if check_abort $repo_path; then exit 2; fi

# power - serial
progress_bandwidth=10

for iva_line in "${iva_arr[@]}"
do
  # power
  power_serial+=(${power_profile[0]})

  progress=`echo "scale=1; p=$progress; bw=$progress_bandwidth; l=${#iva_arr[@]}; p + (bw/l)" | bc -l`

  echo "{\"id\":\"$id\",\"repo\":\"$repo\",\"repoName\":\"$repo_name\",\"startTime\":\"$start_time\",\
  \"endTime\":\"\",\"status\":\"In progress\",\"progress\":{\"currentStep\":\"Serial Power Measurement\",\
  \"nextStep\":\"Parallel Time Measurement\",\"percent\":$progress},\
  \"result\":{\"errorCode\":0,\"message\":\"\",\"repo\":\"\"}}" > $analysis_file

done

if check_abort $repo_path; then exit 2; fi

# energy - serial
for i in "${!iva_arr[@]}"
do
  # energy
  energy_serial+=(`echo "tm=${time_serial[i]};pw=${power_serial[i]};tm * pw" | bc -l`);
done

# serial measurement file
for i in "${!iva_arr[@]}"
do
  echo "${iva_arr[i]},${time_serial[i]},${space_serial[i]},${power_serial[i]},${energy_serial[i]}" >> "$serial_measurement"
done

# parallel run

if check_abort $repo_path; then exit 2; fi

time_parallel=()
space_parallel=()
power_parallel=()
energy_parallel=()

# Add new parallel slow measurement arrays
time_parallel_slow=()

# time - parallel
progress_bandwidth=10

# Configuration
POLL_INTERVAL=3    # seconds between polls
MAX_ATTEMPTS=100   # maximum number of polling attempts
TIMEOUT=10         # curl timeout in seconds

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to log with timestamp
log() {
    echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Condition checking
poll_api_until_condition() {
    local url="$1"
    local condition_field="$2"
    local condition="$3"
    local attempt=1
    local -n response_body_ref=$4

    log "Polling API ${url} until condition '${condition}' is found in '${condition_field}'"

    while [ $attempt -le $MAX_ATTEMPTS ]; do
        log "${YELLOW}Attempt ${attempt}/${MAX_ATTEMPTS}${NC}"

        response=$(curl -s -w "\n%{http_code}" --connect-timeout $TIMEOUT --max-time $TIMEOUT "$url")

        if [ $? -eq 0 ]; then
            http_code=$(echo "$response" | tail -n1)
            response_body=$(echo "$response" | sed '$d')
            response_body_ref="$response_body"

            if [ "$http_code" -eq 200 ]; then
                log "Response: ${response_body}"

                # Check if condition is met (simple string matching)
                if [ $(echo "$response_body" | jq $condition_field | tr -d '"') = "$condition" ]; then
                    log "${GREEN}✓ Condition '${condition}' found!${NC}"
                    return 0
                else
                    log "Condition '${condition}' not found yet"
                fi
            else
                log "${RED}✗ API returned status ${http_code}${NC}"
            fi
        else
            log "${RED}✗ API call failed${NC}"
        fi

        if [ $attempt -eq $MAX_ATTEMPTS ]; then
            log "${RED}Maximum attempts reached. Condition not found.${NC}"
            return 1
        fi

        sleep $POLL_INTERVAL
        ((attempt++))
    done
}

# lib load
load_response=$(curl -s -X POST -H "Content-Type: application/json" -d @- $thmgr_api/load <<EOF
{
  "repo": "$repo_name"
}
EOF
)

echo "load response = $load_response"

num_samples=50000000
mandel_size=3000
max_iter=2500
num_intervals=50000000
rt_size=1500
pw_len=6

# lib run
for i in ${core[@]}
do
  # time
  run_response=$(curl -s -X POST -H "Content-Type: application/json" -d @- $thmgr_api/run <<EOF
{
  "repo": "$repo_name",
  "core": $i,
  "argv": ["main", "$num_samples", "$mandel_size", "$max_iter", "$num_intervals", "$rt_size", "$pw_len", "$i"]
}
EOF
)

  job_id=$(echo $run_response | jq '.id' | tr -d '"')
  poll_api_until_condition "$thmgr_api/job/$job_id" ".data.attributes.status" "complete" response_body

  start=`date -d "$(echo $response_body | jq '.data.attributes.start_time' | tr -d '"')" +%s.%N`
  end=`date -d "$(echo $response_body | jq '.data.attributes.end_time' | tr -d '"')" +%s.%N`
  time_parallel+=(`printf '%.8f' $( echo "$end - $start" | bc -l )`)

  progress=`echo "scale=1; p=$progress; bw=$progress_bandwidth; l=${#core[@]}; p + (bw/l)" | bc -l`

  echo "{\"id\":\"$id\",\"repo\":\"$repo\",\"repoName\":\"$repo_name\",\"startTime\":\"$start_time\",\
  \"endTime\":\"\",\"status\":\"In progress\",\"progress\":{\"currentStep\":\"Parallel Time Measurement\",\
  \"nextStep\":\"Parallel Slow Time Measurement\",\"percent\":$progress},\
  \"result\":{\"errorCode\":0,\"message\":\"\",\"repo\":\"\"}}" > $analysis_file
done

if check_abort $repo_path; then exit 2; fi

# time - parallel slow
progress_bandwidth=10

for i in ${core[@]}
do
  # time using direct execution
  start=`date +%s.%N`;\
  ./$algo $num_samples $mandel_size $max_iter $num_intervals $rt_size $pw_len $i;\
  end=`date +%s.%N`;\
  time_parallel_slow+=(`printf '%.8f' $( echo "$end - $start" | bc -l )`);

  progress=`echo "scale=1; p=$progress; bw=$progress_bandwidth; l=${#core[@]}; p + (bw/l)" | bc -l`

  echo "{\"id\":\"$id\",\"repo\":\"$repo\",\"repoName\":\"$repo_name\",\"startTime\":\"$start_time\",\
  \"endTime\":\"\",\"status\":\"In progress\",\"progress\":{\"currentStep\":\"Parallel Slow Time Measurement\",\
  \"nextStep\":\"Parallel Memory Measurement\",\"percent\":$progress},\
  \"result\":{\"errorCode\":0,\"message\":\"\",\"repo\":\"\"}}" > $analysis_file
done

if check_abort $repo_path; then exit 2; fi

# memory - parallel
progress_bandwidth=10

count=1
for i in ${core[@]}
do
  # memory
  heaptrack -o "$algo.$count" ./$algo $num_samples $mandel_size $max_iter $num_intervals $rt_size $pw_len $i;\
  space_parallel+=(`heaptrack --analyze "$algo.$count.zst"  | grep "peak heap memory consumption" | awk '{print $5}'`);
  count=$((count+1))

  progress=`echo "scale=1; p=$progress; bw=$progress_bandwidth; l=${#core[@]}; p + (bw/l)" | bc -l`

  echo "{\"id\":\"$id\",\"repo\":\"$repo\",\"repoName\":\"$repo_name\",\"startTime\":\"$start_time\",\
  \"endTime\":\"\",\"status\":\"In progress\",\"progress\":{\"currentStep\":\"Parallel Memory Measurement\",\
  \"nextStep\":\"Parallel Power Measurement\",\"percent\":$progress},\
  \"result\":{\"errorCode\":0,\"message\":\"\",\"repo\":\"\"}}" > $analysis_file

done

if check_abort $repo_path; then exit 2; fi

# power - parallel
progress_bandwidth=10

for i in ${core[@]}
do
  # power
  power_parallel+=(${power_profile[i-1]})

  progress=`echo "scale=1; p=$progress; bw=$progress_bandwidth; l=${#core[@]}; p + (bw/l)" | bc -l`

  echo "{\"id\":\"$id\",\"repo\":\"$repo\",\"repoName\":\"$repo_name\",\"startTime\":\"$start_time\",\
  \"endTime\":\"\",\"status\":\"In progress\",\"progress\":{\"currentStep\":\"Parallel Power Measurement\",\
  \"nextStep\":\"Predictive Model Generation\",\"percent\":$progress},\
  \"result\":{\"errorCode\":0,\"message\":\"\",\"repo\":\"\"}}" > $analysis_file
done

if check_abort $repo_path; then exit 2; fi

# energy - parallel
for i in "${!core[@]}"
do
  # energy
  energy_parallel+=(`echo "tm=${time_parallel[i]};pw=${power_parallel[i]};tm * pw" | bc`);
done

if check_abort $repo_path; then exit 2; fi

# parallel measurement file
for i in "${!core[@]}"
do
  echo "${core[i]},${time_parallel[i]},${space_parallel[i]},${power_parallel[i]},${energy_parallel[i]}" >> "$parallel_measurement"
done

if check_abort $repo_path; then exit 2; fi

# parallel slow measurement file
for i in "${!core[@]}"
do
  echo "${core[i]},${time_parallel_slow[i]}" >> "$parallel_slow_measurement"
done

if check_abort $repo_path; then exit 2; fi

# data prep
for i in "${!space_serial[@]}"; do
  if [[ ${space_serial[$i]: -1} == "K" ]]; then
    val=${space_serial[$i]::-1}
    space_serial[$i]=`printf '%.4f' $(echo "v=$val;v * 0.001" | bc)`
  else
    space_serial[$i]=${space_serial[$i]::-1}
  fi
done

for i in "${!space_parallel[@]}"; do
  if [[ ${space_parallel[$i]: -1} == "K" ]]; then
    val=${space_parallel[$i]::-1}
    space_parallel[$i]=`printf '%.4f' $(echo "v=$val;v * 0.001" | bc)`
  else
    space_parallel[$i]=${space_parallel[$i]::-1}
  fi
done

# speedup
speedup=()
t_max=${time_parallel[0]}
for t in "${time_parallel[@]}"; do
  speedup+=(`echo "scale=2;$t_max/$t" | bc`)
done

# freeup
freeup=()
s_max=${space_parallel[0]}
for s in "${space_parallel[@]}"; do
  freeup+=(`echo "scale=2;$s_max/$s" | bc`)
done

# powerup
powerup=()
p_1=${power_parallel[0]}
for p_core in "${power_parallel[@]}"; do
  powerup+=(`echo "scale=4;$p_1/$p_core" | bc`)
done

# energyup
energyup=()
e_1=${energy_parallel[0]}
for e_core in "${energy_parallel[@]}"; do
  energyup+=(`echo "scale=4;$e_1/$e_core" | bc`)
done

# Generate CSV files for fit.py
# time-serial.csv
#echo "$iva_name,size,time" > time-serial.csv
echo "$num_samples,$mandel_size,$max_iter,$num_intervals,$rt_size,$pw_len,time" > time-serial.csv
for i in "${!iva_arr[@]}"; do
  # Extract all columns from the row for multivariate fitting
  echo "${iva_arr[$i]},${time_serial[$i]}" >> time-serial.csv
done
# Sort by time column (last column)
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
echo "$num_samples,$mandel_size,$max_iter,$num_intervals,$rt_size,$pw_len,memory" > space-serial.csv
for i in "${!iva_arr[@]}"; do
  # Extract all columns from the row for multivariate fitting
  echo "${iva_arr[$i]},${space_serial[$i]}" >> space-serial.csv
done
# Sort by memory column (last column)
FILE="space-serial.csv"
NCOLS=$(head -1 "$FILE" | awk -F',' '{print NF}')
(head -n 1 "$FILE" && tail -n +2 "$FILE" | sort -t',' -k"$NCOLS" -n) > "${FILE}.tmp" && mv "${FILE}.tmp" "$FILE"

# space-parallel.csv
echo "core,memory" > space-parallel.csv
for i in "${!core[@]}"; do
  echo "${core[$i]},${space_parallel[$i]}" >> space-parallel.csv
done

# power-serial.csv
echo "$num_samples,$mandel_size,$max_iter,$num_intervals,$rt_size,$pw_len,power" > power-serial.csv
for i in "${!iva_arr[@]}"; do
  # Extract all columns from the row for multivariate fitting
  echo "${iva_arr[$i]},${power_serial[$i]}" >> power-serial.csv
done
# Sort by power column (last column)
FILE="power-serial.csv"
NCOLS=$(head -1 "$FILE" | awk -F',' '{print NF}')
(head -n 1 "$FILE" && tail -n +2 "$FILE" | sort -t',' -k"$NCOLS" -n) > "${FILE}.tmp" && mv "${FILE}.tmp" "$FILE"

# power-parallel.csv
echo "core,power" > power-parallel.csv
for i in "${!core[@]}"; do
  echo "${core[$i]},${power_parallel[$i]}" >> power-parallel.csv
done

# energy-serial.csv
echo "$num_samples,$mandel_size,$max_iter,$num_intervals,$rt_size,$pw_len,energy" > energy-serial.csv
for i in "${!iva_arr[@]}"; do
  # Extract all columns from the row for multivariate fitting
  echo "${iva_arr[$i]},${energy_serial[$i]}" >> energy-serial.csv
done
# Sort by energy column (last column)
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

if check_abort $repo_path; then exit 2; fi

# curve fitting

progress_bandwidth=10
fit_count=13
analysis_types=('time-serial' 'time-parallel' 'space-serial' 'space-parallel' 'power-serial' 'power-parallel' 'energy-serial' 'energy-parallel' 'speedup' 'freeup' 'powerup' 'energyup')

for i in "${analysis_types[@]}"
do
  echo "${i}.csv"
  echo "${i}-fitted.json"
  call_fit $i.csv $i-fitted.json $progress $progress_bandwidth $fit_count $id $repo $repo_name "$start_time" $analysis_file
done

# time serial
extn="${time_serial_analytics_file##*.}"
noextn="${time_serial_analytics_file%.*}"

time_serial_analytics_file_d="$noextn"."$extn"

jo -p \
iva=$(jo data=$(jo -a ${iva_first[@]}) name=$iva_name unit=size) \
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
iva=$(jo data=$(jo -a ${core[@]}) name=core unit=count) \
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
iva=$(jo data=$(jo -a ${iva_first[@]}) name=$iva_name unit=size) \
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
iva=$(jo data=$(jo -a ${core[@]}) name=core unit=count) \
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
iva=$(jo data=$(jo -a ${iva_first[@]}) name=$iva_name unit=size) \
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
iva=$(jo data=$(jo -a ${core[@]}) name=core unit=count) \
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
iva=$(jo data=$(jo -a ${iva_first[@]}) name=$iva_name unit=size) \
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
iva=$(jo data=$(jo -a ${core[@]}) name=core unit=count) \
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
iva=$(jo data=$(jo -a ${core[@]}) name=core unit=count) \
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
iva=$(jo data=$(jo -a ${core[@]}) name=core unit=count) \
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
iva=$(jo data=$(jo -a ${core[@]}) name=core unit=count) \
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
iva=$(jo data=$(jo -a ${core[@]}) name=core unit=count) \
measurements=$(jo data=$(jo -a ${energyup[@]}) name='EnergyEfficiency(E1/Ecore)' unit='') \
unoptimized=$(jo data=$(jo -a) name='EnergyEfficiency(E1/Ecore)' unit='') \
fitted=$(jo data="`jq '.fitted' energyup-fitted.json`" name='EnergyEfficiency(E1/Ecore)' unit='') \
fit_method="`jq -r '.method' energyup-fitted.json`" \
mse="`jq '.mse' energyup-fitted.json`" \
> $energyup_analytics_file_d
