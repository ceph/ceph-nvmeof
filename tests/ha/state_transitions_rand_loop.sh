# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi

ITERATIONS=7
for i in $(seq $ITERATIONS); do
  test_name="state_transitions"
  if [ "$((RANDOM % 2))" -eq "1" ]; then
    test_name="state_transitions_both_gws"
  fi
  echo "Iteration #$i $test_name"
  source $test_dir/$test_name.sh
done
