#!/bin/bash
set -xe
# See
# - https://github.com/spdk/spdk/blob/master/doc/jsonrpc.md
# - https://spdk.io/doc/nvmf_multipath_howto.html

GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW2_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /2/ {print $1}')

ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
ip2="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW2_NAME")"

calc_written_bytes_in_sec()
{
   IP=$1
   num_bytes=$(docker compose run --rm nvmeof-cli --server-address $IP --server-port 5500 --output stdio --format json namespace get_io_stats -n nqn.2016-06.io.spdk:cnode1 --nsid 1 | jq '.bytes_written'| sed 's/[^0-9]*//g');

  sleep 1;
  num_bytes1=$(docker compose run --rm nvmeof-cli --server-address $IP --server-port 5500 --output stdio --format json namespace get_io_stats -n nqn.2016-06.io.spdk:cnode1 --nsid 1 | jq '.bytes_written'| sed 's/[^0-9]*//g');
   
  res=$(expr $num_bytes1 - $num_bytes );
  #echo "Bytes written in sec: $res";
  if [ "$res" -gt 0 ]; then
     # limit values to boolean for simplify futher analysis
     res=1;
  else   
     res=0;
  fi;
  echo "$res";
}


echo -n "ℹ️  Starting bdevperf container"
docker compose up -d bdevperf
sleep 10
echo "ℹ️  bdevperf start up logs"
make logs SVC=bdevperf
BDEVPERF_SOCKET=/tmp/bdevperf.sock
NVMEOF_DISC_PORT=8009


echo "ℹ️  Using discovery service in gateway $GW1 ip  $ip"
rpc="/usr/libexec/spdk/scripts/rpc.py"
echo "ℹ️  bdevperf bdev_nvme_set_options"
make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_set_options -r -1"
echo "ℹ️  bdevperf start discovery ip: $ip port: $NVMEOF_DISC_PORT"
make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_start_discovery -b Nvme0 -t tcp -a $ip -s $NVMEOF_DISC_PORT -f ipv4 -w"
echo "ℹ️  bdevperf bdev_nvme_get_discovery_info"
make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_get_discovery_info"
echo "ℹ️  bdevperf perform_tests"
eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_TEST_DURATION | tr -d '\n\r' )

timeout=$(expr $BDEVPERF_TEST_DURATION \* 2)

echo $timeout
bdevperf="/usr/libexec/spdk/scripts/bdevperf.py"
echo "run io test"
make exec SVC=bdevperf OPTS=-T CMD="$bdevperf -v -t $timeout -s $BDEVPERF_SOCKET perform_tests" &
#test write ios for the ns1 

(
sleep 8;   
lb_group=1;

docker compose run -T --rm nvmeof-cli --server-address $ip --server-port 5500  namespace change_load_balancing_group -n nqn.2016-06.io.spdk:cnode1 --nsid 1 --load-balancing-group $lb_group;
priv_res1=$(calc_written_bytes_in_sec $ip) ; 

echo "ℹ️  written bytes through $ip   $priv_res1 ";

priv_res2=$(calc_written_bytes_in_sec $ip2);

echo "ℹ️  written bytes through $ip2  $priv_res2 ";


for i in $(seq 6); do
   if [ $lb_group -eq 1 ]; then
       lb_group=2
       IP=$ip
   else 
       lb_group=1
       IP=$ip2
   fi;

   echo "ℹ️ ℹ️ Change lb group of ns 1 to $lb_group :" ;
   docker compose run -T --rm nvmeof-cli --server-address $IP --server-port 5500  namespace change_load_balancing_group -n nqn.2016-06.io.spdk:cnode1 --nsid 1 --load-balancing-group $lb_group;
   sleep 4;
 
   res1=$(calc_written_bytes_in_sec $ip) ;
 
   echo "ℹ️  written bytes through $ip ?:  $res1";

   res2=$(calc_written_bytes_in_sec $ip2) ;
 
   echo "ℹ️  written bytes through $ip2 ?: $res2 ";
   echo "ℹ️ ℹ️ ℹ️  DEBUG  iteration $i : priv_res1 and res1 : $priv_res1 ,  $res1 ,  priv_res2 and res2 : $priv_res2 ,  $res2  ";

   #check that io is switched each iteration to different Gateway
   if [ $res1 -eq $res2 ]; then
	echo " ℹ️ ℹ️ ℹ️ res1 and res2 : $res1  $res2 ";   
	exit 1  #both eq 0 - no traffic at all
   fi;

   if [ $res1 -ne $priv_res1 ] && [ $res2 -ne $priv_res2 ]; then
     echo " ℹ️ ℹ️ Valid traffic  results";
   else 
     echo "ℹ️ ℹ️ ℹ️  Not valid checks !!! : priv_res1 and res1 : $priv_res1   $res1 ,  priv_res2 and res2 : $priv_res2   $res2  ";
     exit 1;	   
   fi;
   
   priv_res1=$res1;
   priv_res2=$res2;
   
done;


echo "wait for join";

) &

wait
exit 0
