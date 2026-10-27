#!/usr/bin/env bash
# Vigia do job: garante que "ais-anchorage-events" esteja rodando no cluster.
#
# - Espera o JobManager responder.
# - Se o job não estiver RUNNING, submete de novo a partir do checkpoint mais
#   recente (assim o estado dos navios sobrevive a um restart do Docker).
# - Repete a cada CHECK_INTERVAL segundos. O cluster não tem alta
#   disponibilidade, então quem traz o job de volta depois de um restart é este vigia.
set -uo pipefail

JM="${JOBMANAGER:-flink-jobmanager:8081}"
JOB_NAME="ais-anchorage-events"
CHECK_INTERVAL="${CHECK_INTERVAL:-60}"
CKPT_ROOT="/flink-data/checkpoints"

job_running() {
  wget -qO- "http://${JM}/jobs/overview" 2>/dev/null \
    | grep -Eq "\"name\":\"${JOB_NAME}\"[^}]*\"state\":\"(RUNNING|CREATED|RESTARTING|INITIALIZING)\""
}

latest_checkpoint() {
  # chk-N mais recente que tenha o arquivo _metadata (checkpoint completo)
  ls -dt ${CKPT_ROOT}/*/chk-* 2>/dev/null | while read -r d; do
    [ -f "$d/_metadata" ] && { echo "$d"; break; }
  done
}

echo "[vigia] aguardando o JobManager em ${JM}..."
until wget -qO- "http://${JM}/overview" >/dev/null 2>&1; do sleep 3; done

while true; do
  if ! job_running; then
    CKPT="$(latest_checkpoint)"
    ARGS=()
    if [ -n "${CKPT}" ]; then
      echo "[vigia] job parado; retomando do checkpoint ${CKPT}"
      ARGS=(-s "${CKPT}" --allowNonRestoredState)
    else
      echo "[vigia] job parado; primeira execução (sem checkpoint)"
    fi
    if ! flink run -m "${JM}" -d "${ARGS[@]}" -py /opt/job/anchorage_job.py -pyfs /opt/job; then
      if [ -n "${CKPT}" ]; then
        echo "[vigia] falhou retomando do checkpoint; tentando do zero na próxima volta"
        mv "${CKPT}" "$(dirname "${CKPT}")/invalido-$(basename "${CKPT}")" 2>/dev/null || true
      fi
    fi
  fi
  sleep "${CHECK_INTERVAL}"
done
