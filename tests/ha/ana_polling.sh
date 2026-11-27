# Print the optimized ANA group IDs for a gateway once the expected count is reached.
# Arguments: gateway, expected count, subsystem NQN, and any peers that should be running.
expect_optimized() (
  # The RPC response includes all 512 ANA states.
  set +x
  gateway=$1
  expected=$2
  nqn=$3
  shift 3
  rpc=/usr/libexec/spdk/scripts/rpc.py
  if [ -z "$gateway" ]; then
    echo "No gateway container found while waiting for $nqn" >&2
    return 1
  fi
  deadline=$(($(date +%s) + ${CONVERGE_TIMEOUT:-120}))
  optimized=

  while :; do
    # Check gateway health before declaring a convergence timeout.
    for node in "$gateway" "$@"; do
      if [ -z "$node" ]; then
        echo "No peer gateway container found while waiting for $nqn" >&2
        return 1
      fi
      state=$(timeout 5 docker inspect "$node" \
        --format '{{.State.Status}} (exit {{.State.ExitCode}})') || return 1
      case "$state" in
        running\ *) ;;
        *)
          echo "Gateway $node is $state while waiting for $gateway" \
            "to own $expected ANA groups for $nqn" >&2
          return 1
          ;;
      esac
    done

    remaining=$((deadline - $(date +%s)))
    [ "$remaining" -gt 0 ] || break
    [ "$remaining" -le 5 ] || remaining=5

    # The socket may not exist yet after restarting the gateway.
    if ! socket=$(timeout "$remaining" docker exec "$gateway" find /var/tmp -name spdk.sock) ||
        [ -z "$socket" ]; then
      sleep 1
      continue
    fi

    remaining=$((deadline - $(date +%s)))
    if [ "$remaining" -le 0 ]; then
      continue
    fi
    [ "$remaining" -le 5 ] || remaining=5
    if response=$(timeout "$remaining" docker exec "$gateway" "$rpc" -s "$socket" \
        nvmf_subsystem_get_listeners "$nqn") &&
        groups=$(printf '%s\n' "$response" | jq -ce '
          .[0].ana_states | arrays | map(select(.ana_state == "optimized") | .ana_group)
        '); then
      optimized=$groups
      count=$(printf '%s\n' "$optimized" | jq 'length')
      if [ "$count" -eq "$expected" ]; then
        printf '%s\n' "$optimized" | jq -r '.[]'
        return
      fi
    fi
    sleep 1
  done
  echo "Timed out waiting for $gateway to own $expected optimized ANA groups" \
    "for $nqn (last: ${optimized:-none})" >&2
  return 1
)
