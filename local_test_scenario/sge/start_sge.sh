#!/bin/bash

export SGE_ROOT=/opt/sge
export PATH=$SGE_ROOT/bin/lx-amd64:$PATH
export SGE_CELL=default

HOST=$(hostname)
echo "Modificant node $HOST..."

# Posa user_lists a NONE amb -rattr
qconf -rattr exechost user_lists NONE "$HOST"

# Afegir usuari metascheduler a access list "arusers"
qconf -au metascheduler arusers

echo "Node $HOST configurat."
#service ssh start
exec /usr/sbin/sshd -D

