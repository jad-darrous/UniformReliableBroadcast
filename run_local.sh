if [ "$#" -ne 1 ]; then
    echo "usage: sh run_local.sh <nb_peers>"
    exit
fi

HOSTS_FILE="hosts.conf"
> $HOSTS_FILE
for i in `seq $1`; do
  echo `hostname -I | xargs`:$((12344+$i)) >> $HOSTS_FILE
done

for i in `seq $1 -1 1`; do
  gnome-terminal -e "python peer.py $HOSTS_FILE $i"
  sleep .5
done
