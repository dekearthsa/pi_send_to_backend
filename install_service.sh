#!/usr/bin/env bash
# Install app.py as a systemd service that starts on boot and restarts if it crashes.
# Run on the Pi from this folder:   sudo ./install_service.sh
set -euo pipefail

SERVICE="tongdy-forwarder"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_DIR="/home/pi/tongdy_mqtt_teat/pi_send_to_backend"
UNIT_DST="/etc/systemd/system/${SERVICE}.service"

if [[ ${EUID} -ne 0 ]]; then
  echo "Please run with sudo:  sudo ./install_service.sh" >&2
  exit 1
fi

RUN_USER="${SUDO_USER:-}"
if [[ -z "${RUN_USER}" || "${RUN_USER}" == "root" ]]; then
  echo "Run this as your normal Pi user with sudo (the service will run as that user)." >&2
  exit 1
fi
RUN_GROUP="$(id -gn "${RUN_USER}")"
as_user() { sudo -H -u "${RUN_USER}" "$@"; }

echo "==> app folder : ${APP_DIR}"
echo "==> run as     : ${RUN_USER}:${RUN_GROUP}"

# 1) Python virtualenv (a .venv copied from the Mac does not work on the Pi, so rebuild it)
if ! "${APP_DIR}/.venv/bin/python" -c "import sys" >/dev/null 2>&1; then
  echo "==> creating .venv"
  if ! as_user python3 -m venv --clear "${APP_DIR}/.venv"; then
    apt-get install -y python3-venv
    as_user python3 -m venv --clear "${APP_DIR}/.venv"
  fi
fi
echo "==> installing requirements"
as_user "${APP_DIR}/.venv/bin/pip" install -q --upgrade pip
as_user "${APP_DIR}/.venv/bin/pip" install -q -r "${APP_DIR}/requirements.txt"

# 2) settings sanity checks (.env is optional: without it app.py uses the defaults written in app.py)
if [[ ! -f "${APP_DIR}/.env" ]]; then
  echo "==> no .env found: using the defaults in app.py (broker localhost:1884, API_URL in app.py)"
elif grep -Eq '^API_URL=' "${APP_DIR}/.env" && ! grep -Eq '^API_URL=https?://' "${APP_DIR}/.env"; then
  echo "!! WARNING: API_URL in .env is not a full http(s) URL. Fix it, or delete that line to use the default." >&2
fi
if grep -Eq '^TEST_MODE[[:space:]]*=[[:space:]]*True' "${APP_DIR}/app.py"; then
  echo "!! NOTE: TEST_MODE = True in app.py, so the service will only log readings and send nothing." >&2
fi

# 3) unit file with this user and folder filled in
sed -e "s|^User=.*|User=${RUN_USER}|" \
    -e "s|^Group=.*|Group=${RUN_GROUP}|" \
    -e "s|${TEMPLATE_DIR}|${APP_DIR}|g" \
    "${APP_DIR}/${SERVICE}.service" > "${UNIT_DST}"
chmod 644 "${UNIT_DST}"
echo "==> wrote ${UNIT_DST}"

systemctl daemon-reload
systemctl enable "${SERVICE}"
systemctl restart "${SERVICE}"
sleep 2
systemctl --no-pager --lines=10 status "${SERVICE}" || true

echo
echo "Done. Useful commands:"
echo "  journalctl -u ${SERVICE} -f          # live logs"
echo "  sudo systemctl restart ${SERVICE}    # after editing app.py or .env"
echo "  sudo systemctl stop ${SERVICE}       # stop (it will not restart until start/reboot)"
echo "  sudo systemctl disable --now ${SERVICE}   # remove from boot"
