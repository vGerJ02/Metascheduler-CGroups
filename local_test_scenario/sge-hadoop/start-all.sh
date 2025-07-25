#!/bin/bash

set -e

# ------------------------------ SGE ------------------------------ #
export SGE_ROOT=/opt/sge
export PATH=$SGE_ROOT/bin/lx-amd64:$PATH
export SGE_CELL=default

HOST=$(hostname)
echo "Modificant node $HOST..."

# Posa user_lists a NONE amb -rattr
qconf -rattr exechost user_lists NONE "$HOST"

# Afegir usuari metascheduler a access list "arusers"
qconf -au metascheduler arusers


# Activa la recollida d'informació de jobs al planificador
echo "Configurant schedd_job_info..."

TMP_CONF=$(mktemp)
qconf -ssconf > "$TMP_CONF"

if grep -q '^schedd_job_info' "$TMP_CONF"; then
    sed -i 's/^schedd_job_info.*/schedd_job_info                 true/' "$TMP_CONF"
else
    echo 'schedd_job_info                 true' >> "$TMP_CONF"
fi

# ✅ Fem servir el fitxer com a argument, NO com redirecció
qconf -Msconf "$TMP_CONF"

rm "$TMP_CONF"

echo "Node $HOST configurat."

# ------------------------------ HADOOP ------------------------------ #
USER_HOME="/home/metascheduler"
PROFILE_FILE="$USER_HOME/.bash_profile"
SSH_ENV="$USER_HOME/.ssh/environment"

JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
HADOOP_HOME="/opt/hadoop"
HADOOP_BIN="$HADOOP_HOME/bin"
HADOOP_SBIN="$HADOOP_HOME/sbin"
HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"

export JAVA_HOME="$JAVA_HOME"
export HADOOP_HOME="$HADOOP_HOME"
export HADOOP_CONF_DIR="$HADOOP_CONF_DIR"
export PATH="$JAVA_HOME/bin:$HADOOP_BIN:$HADOOP_SBIN:$PATH"

mkdir -p "$USER_HOME/.ssh"
chown -R metascheduler:metascheduler "$USER_HOME/.ssh"
chmod 700 "$USER_HOME/.ssh"


# Crear clau ssh per a l'usuari metascheduler si no existeix
if [ ! -f "$USER_HOME/.ssh/id_rsa" ]; then
  echo "Generant clau ssh per a metascheduler..."
  sudo -u metascheduler ssh-keygen -t rsa -N "" -f "$USER_HOME/.ssh/id_rsa"
fi

# Afegir clau pública a authorized_keys per a ssh sense password
grep -qxF "$(cat $USER_HOME/.ssh/id_rsa.pub)" "$USER_HOME/.ssh/authorized_keys" || cat "$USER_HOME/.ssh/id_rsa.pub" >> "$USER_HOME/.ssh/authorized_keys"
chmod 600 "$USER_HOME/.ssh/authorized_keys"
chown metascheduler:metascheduler "$USER_HOME/.ssh/authorized_keys"

echo "JAVA_HOME=$JAVA_HOME" > "$SSH_ENV"
chmod 600 "$SSH_ENV"
chown metascheduler:metascheduler "$SSH_ENV"

# Reninicar sshd per actualitzar la configuració
echo "Reiniciant sshd perquè carregui entorns..."

if /usr/sbin/sshd -t; then
    # Si sshd està corrent, mata'l
    if pgrep sshd > /dev/null; then
        pkill sshd
        sleep 1  # dona temps a que s'acabi de tancar
    fi
    # Llança sshd en background
    /usr/sbin/sshd &
    echo "sshd reiniciat correctament."
else
    echo "Error en la configuració sshd, no es reinicia."
    exit 1
fi

echo "JAVA_HOME carregat: $JAVA_HOME"


# Funció per afegir exportacions al .bash_profile si no hi són
add_export_if_missing() {
  local varname="$1"
  local value="$2"
  if ! grep -q "^export $varname=" "$PROFILE_FILE" 2>/dev/null; then
    echo "Afegint export $varname=$value a $PROFILE_FILE"
    echo "export $varname=$value" >> "$PROFILE_FILE"
  fi
}

# Assegura que el .bash_profile existeix
touch "$PROFILE_FILE"
chown metascheduler:metascheduler "$PROFILE_FILE"

# Afegeix les variables al .bash_profile si no hi són
add_export_if_missing "JAVA_HOME" "$JAVA_HOME"
add_export_if_missing "HADOOP_HOME" "$HADOOP_HOME"
add_export_if_missing "HADOOP_CONF_DIR" "$HADOOP_CONF_DIR"

if ! grep -q 'export PATH=.*HADOOP_BIN.*HADOOP_SBIN' "$PROFILE_FILE"; then
  echo "export PATH=\$JAVA_HOME/bin:$HADOOP_BIN:$HADOOP_SBIN:\$PATH" >> "$PROFILE_FILE"
fi

# Comprova que els fitxers XML de configuració existeixen
for f in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml; do
  if [ ! -f "$HADOOP_CONF_DIR/$f" ]; then
    echo "Error: fitxer de configuració $f no trobat a $HADOOP_CONF_DIR"
    exit 1
  fi
done

echo "Fitxers XML configurats correctament."

# Exportar els usuaris per Hadoop per evitar l’error de root
export HDFS_NAMENODE_USER=metascheduler
export HDFS_DATANODE_USER=metascheduler
export HDFS_SECONDARYNAMENODE_USER=metascheduler
export YARN_RESOURCEMANAGER_USER=metascheduler
export YARN_NODEMANAGER_USER=metascheduler
echo "Usuaris per Hadoop exportats correctament."

# Neteja opcional del directori de dades (descomenta si vols netejar)
#/bin/rm -rf /opt/hadoop/dfs/data/* || true

# Permisos
chown -R metascheduler:metascheduler /opt/hadoop

# Comprova si sshd està corrent, si no, llança'l en background
if ! pgrep -x sshd > /dev/null; then
  echo "Iniciant sshd..."
  /usr/sbin/sshd &
  echo "Iniciat sshd..."
  sleep 2
fi

#source "$PROFILE_FILE"

# SSH known_hosts
echo "Afegint localhost a known_hosts per evitar problemes SSH..."
touch "$USER_HOME/.ssh/known_hosts"
if ! grep -q localhost "$USER_HOME/.ssh/known_hosts"; then
  echo "Afegint localhost a known_hosts per evitar problemes SSH..."
  sudo -u metascheduler ssh-keyscan -H localhost >> "$USER_HOME/.ssh/known_hosts"
fi
chown metascheduler:metascheduler "$USER_HOME/.ssh/known_hosts"
chmod 600 "$USER_HOME/.ssh/known_hosts"
echo "known_hosts configurat correctament."

echo "export JAVA_HOME=$JAVA_HOME" > /etc/profile.d/java.sh
echo "export HADOOP_HOME=$HADOOP_HOME" > /etc/profile.d/hadoop.sh
chmod +x /etc/profile.d/*.sh

NN_DIR="/opt/hadoop/dfs/name"
if [ ! -d "$NN_DIR" ] || [ -z "$(ls -A "$NN_DIR")" ]; then
  echo "⚠️ NameNode no formatat. Formatant..."
  sudo -E -u metascheduler env \
    JAVA_HOME="$JAVA_HOME" \
    HADOOP_HOME="$HADOOP_HOME" \
    HADOOP_CONF_DIR="$HADOOP_CONF_DIR" \
    PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH" \
    hdfs namenode -format -force -nonInteractive
else
  echo "✅ NameNode ja formatat."
fi

# Inicia HDFS com a metascheduler
echo "Iniciant HDFS com a metascheduler..."
if sudo -E -u metascheduler env \
    JAVA_HOME="$JAVA_HOME" \
    HADOOP_HOME="$HADOOP_HOME" \
    HADOOP_CONF_DIR="$HADOOP_CONF_DIR" \
    PATH="$JAVA_HOME/bin:$HADOOP_BIN:$HADOOP_SBIN:$PATH" \
    "$HADOOP_SBIN/start-dfs.sh"; then
  echo "✅ HDFS iniciat correctament."
else
  echo "❌ Error en iniciar HDFS"
  exit 1
fi

# Inicia YARN com a metascheduler
echo "Iniciant YARN com a metascheduler..."
if sudo -E -u metascheduler env \
    JAVA_HOME="$JAVA_HOME" \
    HADOOP_HOME="$HADOOP_HOME" \
    HADOOP_CONF_DIR="$HADOOP_CONF_DIR" \
    PATH="$JAVA_HOME/bin:$HADOOP_BIN:$HADOOP_SBIN:$PATH" \
    "$HADOOP_SBIN/start-yarn.sh"; then
  echo "✅ YARN iniciat correctament."
else
  echo "❌ Error en iniciar YARN"
  exit 1
fi

# Manté el contenidor viu mostrant logs Hadoop
# Ajusta el path dels logs segons la teva instal·lació
LOG_DIR="$HADOOP_HOME/logs"
echo "Mostrant logs Hadoop a $LOG_DIR"

# Manté el contenidor viu mostrant logs Hadoop o espera si no n'hi ha
if compgen -G "$LOG_DIR/*" > /dev/null; then
  echo "Contenidor en execució. Esperant logs..."
  tail -F "$LOG_DIR"/*
else
  echo "No s'han trobat logs a $LOG_DIR, mantenint el contenidor viu..."
  tail -f /dev/null
fi
