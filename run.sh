#!/bin/bash -e

JAR_DIR="build/libs"
CHECKPOINT_PATH="data/flink/checkpoint/"

bold() {
  printf "\e[1m%s\e[m" "$1"
}

# Jar file will be at "build/libs/lard-lad-exchange-*-uber.jar"
build_jar() {
  ./gradlew uberJar
}

# Name of the most recent uber jar, so the version doesn't need to be hard-coded.
find_jar() {
  local jar
  jar="$(ls -t "$JAR_DIR"/*-uber.jar 2>/dev/null | head -n 1)"
  if [ -z "$jar" ]; then
    echo "Uber jar not found at '$JAR_DIR'!" >&2
    exit 1
  fi
  basename "$jar"
}

# Flink can create the path automatically, so this function is not required but!
# Just to make it easier for you to know where the checkpoints are being created,
# I'm keeping it here.
function create_checkpoint_path() {
  mkdir -p "./$CHECKPOINT_PATH"
}

printf "+------------------------------------------+\n"
printf "| 🍩 %s                     |\n" "$(bold "Lard Lad Exchange")"
printf "| Kafka UI        ➜ http://localhost:8080  |\n"
printf "| Flink Dashboard ➜ http://localhost:8081  |\n"
printf "+------------------------------------------+\n\n"

printf "📦 %s\n" "$(bold "Building jar file...")"
build_jar
JAR_FILE="$(find_jar)"
create_checkpoint_path

printf "\n🚀 %s\n" "$(bold "Starting application...")"
# About Checkpoints, you can start from a previous Job checkpoint
# by passing "-s" as the FIRST argument 👇
#  >>> ./run.sh -s "<checkpoint-path>/<job-id>/chk-<n>" --pipeline "..." ...
#  >>> ./run.sh -s "/data/flink/checkpoint/f0ed1839c082c5466537788ca4481034/chk-2" --pipeline "..." ...
# Flink options must come before the jar, otherwise they are sent to the application.
FLINK_OPTS=()
if [ "${1:-}" = "-s" ]; then
  FLINK_OPTS=(-s "${2:?Missing the checkpoint path after -s}")
  shift 2
fi

# The script exits with the same code of the `flink run` command.
docker compose exec flink-jobmanager /opt/flink/bin/flink run "${FLINK_OPTS[@]}" "$JAR_FILE" "$@"
