#!/bin/bash

#AWS EMR Bootstrap script
#tested with emr-5.10.0

IS_MASTER=true


if [ -f /mnt/var/lib/info/instance.json ]
then
        if grep isMaster /mnt/var/lib/info/instance.json | grep true;
        then
        IS_MASTER=true
        else
        IS_MASTER=false
        fi
fi

sudo mkdir -p /usr/lib/presto/plugin/queryeventlistener
sudo /usr/bin/aws s3 cp s3://replace-with-your-bucket/QueryEventListener.jar /tmp
sudo cp /tmp/QueryEventListener.jar /usr/lib/presto/plugin/queryeventlistener/

if [ "$IS_MASTER" = true ]; then
sudo mkdir -p /usr/lib/presto/etc
sudo bash -c 'cat <<EOT >> /usr/lib/presto/etc/event-listener.properties
event-listener.name=event-listener
EOT'
fi
