#!/usr/bin/env bash
# Universal-ish squeue view + per-node resource summary (CPU/RAM/GPU) for Slurm.
# - No hardcoded node capacities: totals are discovered from Slurm (scontrol/sinfo).
# - Uses squeue -a (all partitions); no need to enumerate partitions.  (Slurm docs)
# - Best-effort parsing of TRES (cpu/mem/gres). Falls back if fields not available.
#
# Env knobs:
#   SHARDS_PER_GPU=100     # only used if your site has gres/shard
#   SUMMARY_ALL=1          # print all nodes (can be long on big clusters)
#   SUMMARY_NODES="n01,gn01"  # print only these nodes (comma-separated)
#
set -euo pipefail
export LC_ALL=C

# ------------------------------ locate commands ------------------------------
SQUEUE_BIN="${SQUEUE_BIN:-$(command -v squeue || true)}"
SCONTROL_BIN="${SCONTROL_BIN:-$(command -v scontrol || true)}"
SINFO_BIN="${SINFO_BIN:-$(command -v sinfo || true)}"

if [[ -z "${SQUEUE_BIN}" || -z "${SCONTROL_BIN}" || -z "${SINFO_BIN}" ]]; then
  echo "ERROR: This script requires Slurm client commands: squeue, scontrol, sinfo." >&2
  echo "       (Missing: squeue='${SQUEUE_BIN:-}', scontrol='${SCONTROL_BIN:-}', sinfo='${SINFO_BIN:-}')" >&2
  exit 1
fi

# optional sudo wrapper (kept, but script works without it)
declare -a SQUEUE_CMD SCONTROL_CMD SINFO_CMD
if [[ $EUID -ne 0 ]] && command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
  SQUEUE_CMD=(sudo -n "$SQUEUE_BIN")
  SCONTROL_CMD=(sudo -n "$SCONTROL_BIN")
  SINFO_CMD=(sudo -n "$SINFO_BIN")
else
  SQUEUE_CMD=("$SQUEUE_BIN")
  SCONTROL_CMD=("$SCONTROL_BIN")
  SINFO_CMD=("$SINFO_BIN")
fi

# ------------------------------ helpers ------------------------------
have_squeue_O_field() {
  # usage: have_squeue_O_field "tres-alloc"
  local f="$1"
  "${SQUEUE_CMD[@]}" -h -a -O "${f}" >/dev/null 2>&1
}

trim() { sed -e 's/^[[:space:]]\+//' -e 's/[[:space:]]\+$//' ; }

# Convert memory strings to GiB (float)
# Accepts: 690000M, 240G, 1T, 123456 (bytes-ish fallback), 0, empty
to_gib() {
  local v="${1:-0}"
  if   [[ "$v" =~ ^([0-9]+)[Tt]$ ]]; then awk -v x="${BASH_REMATCH[1]}" 'BEGIN{printf("%.3f", x*1024)}'
  elif [[ "$v" =~ ^([0-9]+)[Gg]$ ]]; then awk -v x="${BASH_REMATCH[1]}" 'BEGIN{printf("%.3f", x)}'
  elif [[ "$v" =~ ^([0-9]+)[Mm]$ ]]; then awk -v x="${BASH_REMATCH[1]}" 'BEGIN{printf("%.3f", x/1024)}'
  elif [[ "$v" =~ ^([0-9]+)[Kk]$ ]]; then awk -v x="${BASH_REMATCH[1]}" 'BEGIN{printf("%.6f", x/1048576)}'
  elif [[ "$v" =~ ^[0-9]+$      ]]; then awk -v x="$v" 'BEGIN{printf("%.6f", x/1024/1024)}'
  else echo "0"; fi
}

# Parse gpu count from a Gres/GRES string like:
#   gpu:2
#   gpu:tesla:4(S:0-3)
#   gpu:a100:8,shard:800
parse_gpu_total_from_gres() {
  local gres="${1:-}"
  [[ -z "$gres" || "$gres" == "(null)" ]] && { echo 0; return; }
  # strip spaces
  gres="$(tr -d ' ' <<<"$gres")"
  awk -v g="$gres" '
    BEGIN{
      n=split(g,a,","); tot=0;
      for(i=1;i<=n;i++){
        x=a[i];
        sub(/\(.*/,"",x);            # drop "(S:...)"
        if (x ~ /^gpu(:[^,:]+)*:[0-9]+$/){
          m=split(x,p,":");
          tot += p[m] + 0;
        } else if (x ~ /^gpu(:[^,:]+)*=[0-9]+$/){
          m=split(x,p,"=");
          tot += p[m] + 0;
        }
      }
      print tot;
    }'
}

# ------------------------------ main table (jobs) ------------------------------
# We use squeue -O and append "|" as suffix to each field for reliable parsing.
# Suffix support is part of squeue formatting rules. :contentReference[oaicite:2]{index=2}
SQUEUE_HAS_TRES=0
have_squeue_O_field "tres-alloc" && SQUEUE_HAS_TRES=1

print_jobs_table() {
  echo -e "JOBID\tPART\tNAME\tUSER\tCS_USER\tST\tTIME\tNODES\tCPU\tMEM\tGRES\tNODELIST(REASON)"

  if [[ "$SQUEUE_HAS_TRES" -eq 1 ]]; then
    "${SQUEUE_CMD[@]}" -h -a "$@" \
      -O jobid:\|,partition:\|,name:\|,username:\|,statecompact:\|,timeused:\|,numnodes:\|,tres-alloc:\|,nodelist:\|,reason:\|,comment:\| \
    | awk -F'|' '
      function trm(s){ gsub(/^[ \t\r\n]+|[ \t\r\n]+$/, "", s); return s }
      function get_kv(s, key,   n,t,i,v) {
        if (s=="" || s=="N/A") return "";
        n=split(s,t,",");
        for (i=1;i<=n;i++){
          if (t[i] ~ ("^" key "=")) { split(t[i],v,"="); return v[2] }
          # handle keys like gres/gpu:tesla=2 in TRES
          if (key ~ /^gres\// && t[i] ~ ("^" key "(:[^=]+)?=")) {
            # return whole token
            return t[i]
          }
        }
        return "";
      }
      function get_cpu(s){ return get_kv(s,"cpu") }
      function get_mem(s){ return get_kv(s,"mem") }
      function join_gres(s,   n,t,i,o) {
        if (s=="" || s=="N/A") return "-";
        n=split(s,t,","); o="";
        for(i=1;i<=n;i++) if (t[i] ~ /^gres\//) o = o (o?"+":"") t[i];
        return (o=="" ? "-" : o);
      }
      function get_csuser(c,   m) {
        if (match(c, /csuser=([^ ]+)/, m)) return m[1];
        return "-";
      }
      {
        for(i=1;i<=NF;i++) $i=trm($i);

        job=$1; part=$2; name=$3; user=$4; st=$5; time=$6; nodes=$7;
        tres=$8; nodelist=$9; reason=$10; comment=$11;

        cpu=get_cpu(tres); mem=get_mem(tres);
        gres=join_gres(tres);

        nlr = (st ~ /^PD|^PEND/) ? "(" reason ")" : nodelist;
        csu = get_csuser(comment);

        line = job "\t" part "\t" name "\t" user "\t" csu "\t" st "\t" time "\t" nodes "\t" (cpu?cpu:"-") "\t" (mem?mem:"-") "\t" gres "\t" nlr;

        if (st ~ /^PD|^PEND/) pend[++np]=line; else other[++no]=line;
      }
      END{
        for(i=1;i<=np;i++) print pend[i];
        for(i=1;i<=no;i++) print other[i];
      }'
  else
    # Fallback (older Slurm): no TRES fields. We still show a usable table.
    # Note: %R already shows (Reason) for pending jobs. :contentReference[oaicite:3]{index=3}
    "${SQUEUE_CMD[@]}" -h -a "$@" \
      -o "%i|%P|%j|%u|%t|%M|%D|%C|%m|%R|%k|" \
    | awk -F'|' '
      function trm(s){ gsub(/^[ \t\r\n]+|[ \t\r\n]+$/, "", s); return s }
      function get_csuser(c,   m) { if (match(c, /csuser=([^ ]+)/, m)) return m[1]; return "-" }
      {
        for(i=1;i<=NF;i++) $i=trm($i);
        job=$1; part=$2; name=$3; user=$4; st=$5; time=$6; nn=$7; cpu=$8; mem_mb=$9; r=$10; cmt=$11;
        csu=get_csuser(cmt);
        mem = (mem_mb=="" || mem_mb=="0") ? "-" : (mem_mb "M");
        print job "\t" part "\t" name "\t" user "\t" csu "\t" st "\t" time "\t" nn "\t" (cpu?cpu:"-") "\t" mem "\t" "-" "\t" r;
      }'
  fi
}

if command -v column >/dev/null 2>&1; then
  print_jobs_table "$@" | column -s $'\t' -t
else
  print_jobs_table "$@"
fi

# ------------------------------ per-node summary (CPU/RAM/GPU) ------------------------------
# Discover node totals
declare -A NODE_CPU_TOTAL NODE_MEM_TOTAL_GIB NODE_GPU_TOTAL
nodes_raw="$("${SCONTROL_CMD[@]}" show node -o 2>/dev/null || true)"

if [[ -n "$nodes_raw" ]]; then
  while IFS=$'\t' read -r n cpu_tot mem_mb gres; do
    [[ -z "$n" ]] && continue
    NODE_CPU_TOTAL["$n"]="${cpu_tot:-0}"
    NODE_MEM_TOTAL_GIB["$n"]="$(awk -v m="${mem_mb:-0}" 'BEGIN{printf("%.3f", m/1024)}')"
    NODE_GPU_TOTAL["$n"]="$(parse_gpu_total_from_gres "${gres:-}")"
  done < <(
    awk '
      function get(k,   i,a){
        for(i=1;i<=NF;i++){
          if($i ~ ("^"k"=")){ split($i,a,"="); return a[2] }
        }
        return ""
      }
      {
        n=get("NodeName");
        c=get("CPUTot"); if(c=="") c=get("CPUs");
        m=get("RealMemory");
        g=get("Gres");
        if(n!="") printf "%s\t%s\t%s\t%s\n", n,c,m,g;
      }' <<<"$nodes_raw"
  )
else
  # Fallback to sinfo (may be duplicate per partition; we uniq by node and keep max values)
  sinfo_raw="$("${SINFO_CMD[@]}" -N -h -a -o "%n|%c|%m|%G" 2>/dev/null || true)"
  if [[ -z "$sinfo_raw" ]]; then
    echo
    echo "==== Resources per node (RUNNING now) ===="
    echo "WARN: Could not query node totals via scontrol/sinfo. Skipping node summary." >&2
    exit 0
  fi

  # Keep the maximum observed cpu/mem/gpu for each node (covers duplicates across partitions)
  while IFS='|' read -r n cpu mem_mb gres; do
    n="$(tr -d ' ' <<<"$n")"
    [[ -z "$n" ]] && continue
    cpu="${cpu// /}"; mem_mb="${mem_mb// /}"
    gtot="$(parse_gpu_total_from_gres "${gres:-}")"

    oldc="${NODE_CPU_TOTAL[$n]:-0}"
    oldm="${NODE_MEM_TOTAL_GIB[$n]:-0}"
    oldg="${NODE_GPU_TOTAL[$n]:-0}"

    # choose max
    if [[ "${cpu:-0}" -gt "${oldc:-0}" ]]; then NODE_CPU_TOTAL["$n"]="${cpu:-0}"; fi
    mgib="$(awk -v m="${mem_mb:-0}" 'BEGIN{printf("%.3f", m/1024)}')"
    awk -v a="$mgib" -v b="$oldm" 'BEGIN{exit !(a>b)}' && NODE_MEM_TOTAL_GIB["$n"]="$mgib"
    if [[ "$gtot" -gt "$oldg" ]]; then NODE_GPU_TOTAL["$n"]="$gtot"; fi
  done <<<"$sinfo_raw"
fi

# Accumulators (used)
declare -A CPU_USED MEM_USED_GIB GPU_USED_EQ
for n in "${!NODE_CPU_TOTAL[@]}"; do
  CPU_USED["$n"]=0
  MEM_USED_GIB["$n"]=0
  GPU_USED_EQ["$n"]=0
done

SHARDS_PER_GPU="${SHARDS_PER_GPU:-100}"

# Build running-job iterator
if [[ "$SQUEUE_HAS_TRES" -eq 1 ]]; then
  RUN_STREAM=$("${SQUEUE_CMD[@]}" -h -a -t RUNNING "$@" -O jobid:\|,tres-alloc:\|,nodelist:\| 2>/dev/null || true)
  while IFS='|' read -r job tres nodes; do
    job="$(trim <<<"${job:-}")"
    tres="$(trim <<<"${tres:-}")"
    nodes="$(trim <<<"${nodes:-}")"
    [[ -z "$nodes" ]] && continue

    cpu="$(grep -oE 'cpu=[0-9]+' <<<"$tres" | head -n1 | cut -d= -f2 || true)"; cpu="${cpu:-0}"
    memv="$(grep -oE 'mem=[0-9]+[KMGTPkmgpt]?' <<<"$tres" | head -n1 | cut -d= -f2 || true)"; memv="${memv:-0}"
    memg="$(to_gib "$memv")"

    # GPUs: handle gres/gpu=, gres/gpu:TYPE=
    gwhole="$(grep -oE 'gres/gpu(:[^=,]+)?=[0-9]+' <<<"$tres" | awk -F= '{s+=$2} END{print s+0}' || true)"
    gshard="$(grep -oE 'gres/shard=[0-9]+' <<<"$tres" | head -n1 | cut -d= -f2 || true)"; gshard="${gshard:-0}"
    geq="$(awk -v gw="${gwhole:-0}" -v gs="${gshard:-0}" -v spg="$SHARDS_PER_GPU" 'BEGIN{printf("%.6f", gw + (spg>0?gs/spg:0))}')"

    # Expand hostlist via scontrol (best available)
    hosts_str="$("${SCONTROL_CMD[@]}" show hostnames "$nodes" 2>/dev/null || echo "$nodes")"
    readarray -t hosts <<<"$hosts_str"
    hn=${#hosts[@]}
    (( hn > 0 )) || continue

    cpu_share="$(awk -v c="$cpu" -v n="$hn" 'BEGIN{printf("%.6f", (n>0)?(c/n):0)}')"
    mem_share="$(awk -v m="$memg" -v n="$hn" 'BEGIN{printf("%.6f", (n>0)?(m/n):0)}')"
    geq_share="$(awk -v g="$geq" -v n="$hn" 'BEGIN{printf("%.6f", (n>0)?(g/n):0)}')"

    for h in "${hosts[@]}"; do
      [[ -z "${NODE_CPU_TOTAL[$h]:-}" ]] && continue
      CPU_USED["$h"]="$(awk -v a="${CPU_USED[$h]}" -v b="$cpu_share" 'BEGIN{printf("%.6f", a+b)}')"
      MEM_USED_GIB["$h"]="$(awk -v a="${MEM_USED_GIB[$h]}" -v b="$mem_share" 'BEGIN{printf("%.6f", a+b)}')"
      GPU_USED_EQ["$h"]="$(awk -v a="${GPU_USED_EQ[$h]}" -v b="$geq_share" 'BEGIN{printf("%.6f", a+b)}')"
    done
  done <<<"$RUN_STREAM"
else
  # Fallback: cpu=%C, mem=%m (MB), nodes=%N ; no GPU accounting
  RUN_STREAM=$("${SQUEUE_CMD[@]}" -h -a -t RUNNING "$@" -o "%C|%m|%N|" 2>/dev/null || true)
  while IFS='|' read -r cpu mem_mb nodes; do
    cpu="$(trim <<<"${cpu:-0}")"; mem_mb="$(trim <<<"${mem_mb:-0}")"; nodes="$(trim <<<"${nodes:-}")"
    [[ -z "$nodes" ]] && continue
    memg="$(awk -v m="${mem_mb:-0}" 'BEGIN{printf("%.6f", m/1024)}')"

    hosts_str="$("${SCONTROL_CMD[@]}" show hostnames "$nodes" 2>/dev/null || echo "$nodes")"
    readarray -t hosts <<<"$hosts_str"
    hn=${#hosts[@]}
    (( hn > 0 )) || continue

    cpu_share="$(awk -v c="${cpu:-0}" -v n="$hn" 'BEGIN{printf("%.6f", (n>0)?(c/n):0)}')"
    mem_share="$(awk -v m="$memg" -v n="$hn" 'BEGIN{printf("%.6f", (n>0)?(m/n):0)}')"

    for h in "${hosts[@]}"; do
      [[ -z "${NODE_CPU_TOTAL[$h]:-}" ]] && continue
      CPU_USED["$h"]="$(awk -v a="${CPU_USED[$h]}" -v b="$cpu_share" 'BEGIN{printf("%.6f", a+b)}')"
      MEM_USED_GIB["$h"]="$(awk -v a="${MEM_USED_GIB[$h]}" -v b="$mem_share" 'BEGIN{printf("%.6f", a+b)}')"
    done
  done <<<"$RUN_STREAM"
fi

# Decide which nodes to print
SUMMARY_ALL="${SUMMARY_ALL:-0}"
SUMMARY_NODES="${SUMMARY_NODES:-}"

declare -a PRINT_NODES
if [[ -n "$SUMMARY_NODES" ]]; then
  IFS=',' read -r -a PRINT_NODES <<<"$SUMMARY_NODES"
else
  # if small cluster => print all nodes; else print only nodes with usage > 0 (or with GPUs)
  node_count="${#NODE_CPU_TOTAL[@]}"
  if [[ "$SUMMARY_ALL" -eq 1 || "$node_count" -le 40 ]]; then
    mapfile -t PRINT_NODES < <(printf "%s\n" "${!NODE_CPU_TOTAL[@]}" | sort)
  else
    mapfile -t PRINT_NODES < <(
      for n in "${!NODE_CPU_TOTAL[@]}"; do
        cu="${CPU_USED[$n]:-0}"; gu="${GPU_USED_EQ[$n]:-0}"
        awk -v c="$cu" -v g="$gu" 'BEGIN{exit !((c>0.001)||(g>0.001))}' && echo "$n"
      done | sort
    )
  fi
fi

echo
echo "==== Resources per node (RUNNING now) ===="
printf "%-20s | CPU: tot %6s  use %8s  free %8s || RAM: tot %10s GiB  use %10s GiB  free %10s GiB || GPU: tot %5s  use %8s  free %8s\n" \
       "NODE" "" "" "" "" "" "" "" "" ""
printf -- "-----------------------------------------------------------------------------------------------------------------------------------------------\n"

for n in "${PRINT_NODES[@]}"; do
  [[ -z "${NODE_CPU_TOTAL[$n]:-}" ]] && continue

  ctot="${NODE_CPU_TOTAL[$n]}"
  mtot="${NODE_MEM_TOTAL_GIB[$n]:-0}"
  gtot="${NODE_GPU_TOTAL[$n]:-0}"

  cu="${CPU_USED[$n]:-0}"
  mu="${MEM_USED_GIB[$n]:-0}"
  gu="${GPU_USED_EQ[$n]:-0}"

  cfree="$(awk -v t="$ctot" -v u="$cu" 'BEGIN{f=t-u; if(f<0) f=0; printf("%.0f", f)}')"
  mfree="$(awk -v t="$mtot" -v u="$mu" 'BEGIN{f=t-u; if(f<0) f=0; printf("%.3f", f)}')"
  gfree="$(awk -v t="$gtot" -v u="$gu" 'BEGIN{f=t-u; if(f<0) f=0; printf("%.3f", f)}')"

  printf "%-20s | CPU: tot %6d  use %8.0f  free %8s || RAM: tot %10.3f GiB  use %10.3f GiB  free %10s GiB || GPU: tot %5d  use %8.3f  free %8s\n" \
         "$n" "$ctot" "$cu" "$cfree" "$mtot" "$mu" "$mfree" "$gtot" "$gu" "$gfree"
done


