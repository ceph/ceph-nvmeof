# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi

ITERATIONS=7
for i in $(seq $ITERATIONS); do
  echo "Iteration #$i"
  source $test_dir/state_transitions.sh
done
