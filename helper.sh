#!/bin/bash

SYSCALL_INTERCEPTOR=/home/wnj/Projects/NextFS/build/third_party/syscall_intercept/libsyscall_intercept.so.0
INTERCEPTOR=/home/wnj/Projects/NextFS/build/src/interceptor/libnextfs_interceptor.so
DAEMON=/home/wnj/Projects/NextFS/build/src/daemon/nextfs_daemon
CONFIG_TEMPLATE=./config_exp.yaml
DATA_PATH=/mnt/nextfs/wnj/data_1

WORK_DIR=/home/wnj/nextfs_test

# node_list=(s13 s12)
# ip_list=("192.168.200.13" "192.168.200.12")
# node_list=(s13 s12 s51 s52)
# ip_list=("192.168.200.13" "192.168.200.12" "192.168.200.51" "192.168.200.52")
# node_list=(s51 s52)
# ip_list=("192.168.200.51" "192.168.200.52")
# node_list=(s51 s52 s53)
# ip_list=("192.168.200.51" "192.168.200.52" "192.168.200.53")
# node_list=(s13 s12 s14 s51 s52 s53)
# ip_list=("192.168.200.13" "192.168.200.12" "192.168.200.14" "192.168.200.51" "192.168.200.52" "192.168.200.53")
# node_list=(s13 s51 s53)
# ip_list=("192.168.200.13" "192.168.200.51" "192.168.200.53")
node_list=(s14)
ip_list=("192.168.200.14")
# node_list=(s13 s12 s14 s51)
# ip_list=("192.168.200.13" "192.168.200.12" "192.168.200.14" "192.168.200.51")
mount_point=/mnt/nextfs/wnj/mount
dir=("a" "a/b" "a/b/c" "a/b/c/d" "a/b/c/d/e" "a/b/c/d/e/f" "a/b/c/d/e/f/g" "a/b/c/d/e/f/g/h")

function install {
    gen_config
    for node in ${node_list[@]}
    do
        echo "installing to $node"
        scp $INTERCEPTOR $DAEMON $SYSCALL_INTERCEPTOR $node:$WORK_DIR
        scp ${node}.yaml $node:$WORK_DIR/config.yaml
    done
}

function install_config {
    gen_config
    for node in ${node_list[@]}
    do
        echo "installing config to $node"
        scp ${node}.yaml $node:$WORK_DIR/config.yaml
    done
}

function gen_config {
    in_gourp_config=""
    for ((i=0;i<${#node_list[@]};++i))
    do
        id=$(expr $i + 1)
        ip=${ip_list[$i]}
        in_gourp_config="$in_gourp_config | with(.in-group-node[$i]; .server-id = $id | .ip = \"$ip\" | .port = 7878 | . style=\"flow\")"
    done
    # echo $in_gourp_config

    for ((i=0;i<${#node_list[@]};++i))
    do
        id=$(expr $i + 1)
        ip=${ip_list[$i]}
        yq "
            .block_size = 524288 |
            .rpc_mempool_capacity = 4096 |
            .server-id = $id |
            .server-ip = \"$ip\"
            $in_gourp_config
        " $CONFIG_TEMPLATE > ${node_list[$i]}.yaml
    done
}

function run_daemon {
    # revert loop node_list
    for ((i=${#node_list[@]}-1;i>=0;--i))
    do
        node=${node_list[$i]}
        echo "running daemon on $node"
        # clear data
        ssh $node "rm -rf $DATA_PATH/*"
        ssh $node "NEXTFS_CONFIG_PATH=$WORK_DIR/config.yaml nohup $WORK_DIR/nextfs_daemon &> $WORK_DIR/daemon.log < /dev/null &"
    done
}

function stop_daemon {
    for node in ${node_list[@]}
    do
        echo "stopping daemon on $node"
        ssh $node "pkill nextfs_daemon"
    done
    sleep 5
    for node in ${node_list[@]}
    do
        echo "kill daemon on $node"
        ssh $node "pkill -9 nextfs_daemon"
    done
}

# check alive
function check_alive {
    for node in ${node_list[@]}
    do
        echo "checking daemon on $node"
        ssh $node "ps -ef | grep nextfs_daemon"
    done
}

function drop_cache {
    for node in ${node_list[@]}
    do
        echo "drop cache on $node"
        ssh $node "sudo bash -c 'echo 1 > /proc/sys/vm/drop_caches'"
    done
}

function tail_log {
    for node in ${node_list[@]}
    do
        echo "daemon.log on $node"
        ssh $node "tail $WORK_DIR/daemon.log"
        echo ""
    done
}

function ll_inode {
    for node in ${node_list[@]}
    do
        echo "ll inode on $node"
        ssh $node "ls -lh /mnt/nextfs/wnj/data_1/file_inode.txt"
        echo ""
    done
}

function create_dir {
    # for node in ${node_list[@]}
    # do
    #     echo "create dir $ on $node"
    #     ssh $node "mkdir -p $mount_point/${dir[$]}"
    # done
    # for ((i=0;i<${#node_list[@]};++i))
    # do
    #     node=${node_list[$i]}
    #     path=$mount_point/${dir[$i]}
    #     echo "create dir $path on $node"
    #     ssh $node "LD_LIBRARY_PATH=$WORK_DIR LD_PRELOAD=$WORK_DIR/libnextfs_interceptor.so NEXTFS_CONFIG_PATH=$WORK_DIR/config.yaml mkdir $path"
    # done
    mk_env="LD_LIBRARY_PATH=$WORK_DIR LD_PRELOAD=$WORK_DIR/libnextfs_interceptor.so NEXTFS_CONFIG_PATH=$WORK_DIR/config.yaml"
    ssh s53 "$mk_env mkdir $mount_point/${dir[0]} && sleep 1"
    ssh s51 "$mk_env mkdir $mount_point/${dir[1]} && sleep 1"
    ssh s53 "$mk_env mkdir $mount_point/${dir[2]} && sleep 1"
    ssh s51 "$mk_env mkdir $mount_point/${dir[3]} && sleep 1"
    ssh s53 "$mk_env mkdir $mount_point/${dir[4]} && sleep 1"
    ssh s51 "$mk_env mkdir $mount_point/${dir[5]} && sleep 1"
    ssh s53 "$mk_env mkdir $mount_point/${dir[6]} && sleep 1"
    ssh s51 "$mk_env mkdir $mount_point/${dir[7]} && sleep 1"
}

if [ "$1" == "install" ]; then
    install
fi

if [ "$1" == "install_config" ]; then
    install_config
fi

if [ "$1" == "run" ]; then
    run_daemon
fi

if [ "$1" == "stop" ]; then
    stop_daemon
fi

if [ "$1" == "check" ]; then
    check_alive
fi

if [ "$1" == "drop_cache" ]; then
    drop_cache
fi

if [ "$1" == "tail_log" ]; then
    tail_log
fi

if [ "$1" == "ll_inode" ]; then
    ll_inode
fi

if [ "$1" == "create_dir" ]; then
    create_dir
fi
