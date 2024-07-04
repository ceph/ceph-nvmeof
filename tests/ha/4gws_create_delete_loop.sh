# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi

ITERATIONS=2
for i in $(seq $ITERATIONS); do
  echo "Iteration #$i"
  source $test_dir/4gws_create_delete.sh
done
