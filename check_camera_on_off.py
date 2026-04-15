import time
import socket
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import requests
import yaml


@dataclass
class MainConfig:
    BASE_HOST: str
    URI: str
    URI_CONFIG: str
    URI_TOKEN: str
    PAYLOAD_TOKEN: dict
    TIMEOUT: int
    TIME_TO_GET_CAM: int
    AREA_CODE_ALLOW: Optional[str]
    SERVER_ID: Optional[str]

    @property
    def URL_CONFIG(self) -> str:
        return self.BASE_HOST + self.URI_CONFIG

    @property
    def URL_TOKEN(self) -> str:
        return self.BASE_HOST + self.URI_TOKEN

    @property
    def URL_DEVICE_STATUS(self) -> str:
        return f"{self.BASE_HOST}/Service/api/device/update-device-status"


@dataclass
class CameraConfig:
    index: int
    id: str
    name: Optional[str]
    code: Optional[str]
    rtsp: Optional[str]
    compId: Optional[int]
    serverId: Optional[int]
    areaCode: Optional[str]


class AccessToken:
    value: Optional[str] = None
    id_company: Optional[str] = 1


accessToken = AccessToken()


def load_main_config() -> MainConfig:
    config_path = Path(__file__).resolve().parent / "resources" / "configs" / "main.yaml"
    data = yaml.safe_load(config_path.read_text())
    return MainConfig(
        BASE_HOST=data["BASE_HOST"],
        URI=data["URI"],
        URI_CONFIG=data["URI_CONFIG"],
        URI_TOKEN=data["URI_TOKEN"],
        PAYLOAD_TOKEN=data["PAYLOAD_TOKEN"],
        TIMEOUT=data.get("TIMEOUT", 10),
        TIME_TO_GET_CAM=data.get("TIME_TO_GET_CAM", 10),
        AREA_CODE_ALLOW=(__import__("os").environ.get("AREA_CODE") or __import__("os").environ.get("AREA_CODE_ALLOW")),
        SERVER_ID=__import__("os").environ.get("SERVER_ID"),
    )


mainConfig = load_main_config()


def log(message: str):
    print(f"[camera-status] {message}", flush=True)


def normalize_rtsp_url(url: Optional[str]) -> Optional[str]:
    if not url or "://" not in url:
        return url
    try:
        parsed = urlsplit(url)
    except ValueError:
        return url
    if parsed.scheme.lower() != "rtsp" or parsed.hostname is None:
        return url
    if parsed.username is None and parsed.password is None:
        return url

    username = quote(unquote(parsed.username or ""), safe="")
    password = quote(unquote(parsed.password or ""), safe="")
    hostname = parsed.hostname
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"

    userinfo = username
    if parsed.password is not None:
        userinfo = f"{userinfo}:{password}"

    netloc = f"{userinfo}@{hostname}"
    if parsed.port is not None:
        netloc = f"{netloc}:{parsed.port}"

    return urlunsplit((
        parsed.scheme,
        netloc,
        parsed.path,
        parsed.query,
        parsed.fragment,
    ))


def api_get_token(url: str, payload: dict, timeout: int = 5):
    try:
        response = requests.post(url, json=payload, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        return data.get("access_token"), data.get("comId")
    except Exception as exc:
        log(f"get token failed: {exc}")
        return None, None


def ensure_token():
    while True:
        token, id_company = api_get_token(mainConfig.URL_TOKEN, mainConfig.PAYLOAD_TOKEN, mainConfig.TIMEOUT)
        if token:
            accessToken.value = token
            accessToken.id_company = id_company
            log("get token success")
            return
        time.sleep(1)


def build_headers():
    return {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + accessToken.value,
    }


def _should_refresh_token(response: Optional[requests.Response], exc: Exception) -> bool:
    if response is not None and response.status_code == 401:
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code == 401
    return False


def _parse_cameras(data: List[dict]) -> List[CameraConfig]:
    cameras: List[CameraConfig] = []
    for index, device in enumerate(data):
        camera = CameraConfig(
            index=index,
            id=device.get("id"),
            name=device.get("deviceName") or device.get("name"),
            code=device.get("deviceCode") or device.get("code") or str(device.get("id")),
            rtsp=normalize_rtsp_url(device.get("rstpLink") or device.get("link") or device.get("rtsp")),
            compId=device.get("compId"),
            serverId=device.get("serverId") or device.get("serverID"),
            areaCode=device.get("areaCode"),
        )
        if camera.id is None or not camera.rtsp:
            continue
        if mainConfig.SERVER_ID and str(camera.serverId) != str(mainConfig.SERVER_ID):
            continue
        if mainConfig.AREA_CODE_ALLOW and camera.areaCode != mainConfig.AREA_CODE_ALLOW:
            continue
        cameras.append(camera)
    return cameras


def fetch_cameras() -> Optional[List[CameraConfig]]:
    url_config = mainConfig.URL_CONFIG
    if accessToken.id_company is not None and "compId=" not in url_config:
        separator = "&" if "?" in url_config else "?"
        url_config = f"{url_config}{separator}compId={accessToken.id_company}"

    response = None
    try:
        response = requests.get(url_config, headers=build_headers(), timeout=mainConfig.TIMEOUT)
        response.raise_for_status()
        data = response.json().get("data", [])
        return _parse_cameras(data)
    except Exception as exc:
        if _should_refresh_token(response, exc):
            log("fetch cameras got 401, refreshing token")
            ensure_token()
            try:
                response = requests.get(url_config, headers=build_headers(), timeout=mainConfig.TIMEOUT)
                response.raise_for_status()
                data = response.json().get("data", [])
                return _parse_cameras(data)
            except Exception as retry_exc:
                log(f"fetch cameras retry failed: {retry_exc}")
        log(f"fetch cameras failed: {exc}")
        return None


def probe_camera_live(camera: CameraConfig) -> bool:
    try:
        parsed = urlsplit(camera.rtsp)
        host = parsed.hostname
        if not host:
            return False
        port = parsed.port or 554
        with socket.create_connection((host, port), timeout=3):
            return True
    except Exception as exc:
        log(f"probe failed for device {camera.id}: {exc}")
        return False


def sync_date_string() -> str:
    return datetime.now().strftime("%Y/%m/%d %H:%M:%S.%f") + "0"


def post_device_status(status_items: List[dict]) -> bool:
    if not status_items:
        return True
    payload = {
        "SyncDate": sync_date_string(),
        "Data": status_items,
    }
    response = None
    try:
        response = requests.put(
            mainConfig.URL_DEVICE_STATUS,
            json=payload,
            headers=build_headers(),
            timeout=mainConfig.TIMEOUT,
        )
        response.raise_for_status()
        log(f"posted {len(status_items)} device statuses")
        return True
    except Exception as exc:
        if _should_refresh_token(response, exc):
            log("post device status got 401, refreshing token")
            ensure_token()
            try:
                response = requests.put(
                    mainConfig.URL_DEVICE_STATUS,
                    json=payload,
                    headers=build_headers(),
                    timeout=mainConfig.TIMEOUT,
                )
                response.raise_for_status()
                log(f"posted {len(status_items)} device statuses")
                return True
            except Exception as retry_exc:
                log(f"post device status retry failed: {retry_exc}")
        log(f"post device status failed: {exc}")
        return False


def build_status_updates(
    cameras: List[CameraConfig],
    last_status_by_device: Dict[str, int],
) -> List[dict]:
    updates: List[dict] = []
    for camera in cameras:
        current_status = 1 if probe_camera_live(camera) else 2
        device_id = str(camera.id)
        previous_status = last_status_by_device.get(device_id)
        last_status_by_device[device_id] = current_status
        updates.append(
            {
                "DeviceId": device_id,
                "CurrentStatus": str(current_status),
            }
        )
        if previous_status != current_status:
            log(
                f"device {device_id} ({camera.code or camera.name}) -> "
                f"{'on' if current_status == 1 else 'off'}"
            )
    return updates


def main():
    ensure_token()
    last_status_by_device: Dict[str, int] = {}
    cached_cameras: List[CameraConfig] = []
    while True:
        log("starting camera status check cycle")
        fetched_cameras = fetch_cameras()
        if fetched_cameras is not None:
            cached_cameras = fetched_cameras
            log(f"loaded {len(cached_cameras)} cameras from api")
        elif cached_cameras:
            log(f"using cached camera config ({len(cached_cameras)} devices)")
        else:
            log("no cached camera config available")
            time.sleep(300)
            continue
        cameras = cached_cameras
        updates = build_status_updates(cameras, last_status_by_device)
        log(f"prepared {len(updates)} device statuses")
        post_device_status(updates)
        log("camera status check cycle complete")
        time.sleep(300)


if __name__ == "__main__":
    main()
