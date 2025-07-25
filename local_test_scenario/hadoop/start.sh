#!/bin/bash
set -e

# Ruta al bash profile de metascheduler
PROFILE_FILE="/home/metascheduler/.bash_profile"

# Activar controladors si no estan activats
echo "+cpu +memory" > /sys/fs/cgroup/cgroup.subtree_control || echo "Ja estaven activats."

# Defineix JAVA_HOME i Hadoop bin
JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64/jre"
HADOOP_BIN="/opt/hadoop/bin"

# Funció per afegir exportacions si no existeixen ja al perfil
add_export_if_missing() {
  local varname="$1"
  local value="$2"
  if ! grep -q "^export $varname=" "$PROFILE_FILE" 2>/dev/null; then
    echo "export $varname=$value" >> "$PROFILE_FILE"
  fi
}

# Assegura que el .bash_profile existeix
touch "$PROFILE_FILE"
chown metascheduler:metascheduler "$PROFILE_FILE"

# Afegeix JAVA_HOME i PATH al .bash_profile si no estan
add_export_if_missing "JAVA_HOME" "$JAVA_HOME"
if ! grep -q "$HADOOP_BIN" "$PROFILE_FILE"; then
  echo "export PATH=\$JAVA_HOME/bin:$HADOOP_BIN:\$PATH" >> "$PROFILE_FILE"
fi

# Exporta a la sessió actual per executar yarn ara mateix
export JAVA_HOME="$JAVA_HOME"
export PATH="$JAVA_HOME/bin:$HADOOP_BIN:$PATH"

# Neteja el directori de dades del datanode (opcional)
/bin/rm -rf /opt/hadoop/dfs/data/*

# Iniciar sshd en background
/usr/sbin/sshd &

# Iniciar yarn nodemanager en primer pla (el procés principal)
yarn nodemanager
