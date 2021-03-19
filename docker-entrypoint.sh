#!/bin/sh

if ! whoami >/dev/null 2>&1; then
  if [ -w /etc/passwd ]; then
    echo "${USER_NAME:-default}:x:$(id -u):0:${USER_NAME:-default} user:${APP_ROOT}:/sbin/nologin" >> /etc/passwd
  fi
fi

exec "$@"
