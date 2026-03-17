import subprocess
import logging
import socket
import uuid
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

from config import (
    COUNTRY_CODE_PREFIX,
    CALL_TIMEOUT_SECONDS,
    PJSUA_DURATION_SECONDS,
)

logger = logging.getLogger(__name__)

_TCP_TIMEOUT = 4
_UDP_TIMEOUT = 4
_DNS_TIMEOUT = 6


def resolve_domain(domain: str) -> tuple[str | None, str | None]:
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(socket.gethostbyname, domain)
    try:
        ip = future.result(timeout=_DNS_TIMEOUT)
        executor.shutdown(wait=False)
        logger.info("DNS: %s -> %s", domain, ip)
        return ip, None
    except FutureTimeout:
        executor.shutdown(wait=False)
        logger.warning("DNS timeout for %s", domain)
        return None, "timeout"
    except Exception as e:
        executor.shutdown(wait=False)
        logger.warning("DNS error for %s: %s", domain, e)
        return None, str(e)


def try_tcp(domain: str, port: int) -> bool:
    try:
        sock = socket.create_connection((domain, port), timeout=_TCP_TIMEOUT)
        sock.close()
        logger.info("TCP OK: %s:%d", domain, port)
        return True
    except OSError as e:
        logger.debug("TCP fail %s:%d - %s", domain, port, e)
        return False


def _build_sip_register(domain, username, port):
    branch = "z9hG4bK" + uuid.uuid4().hex[:10]
    tag = uuid.uuid4().hex[:8]
    call_id = uuid.uuid4().hex
    return (
        f"REGISTER sip:{domain} SIP/2.0\r\n"
        f"Via: SIP/2.0/UDP {domain}:{port};branch={branch}\r\n"
        f"From: <sip:{username}@{domain}>;tag={tag}\r\n"
        f"To: <sip:{username}@{domain}>\r\n"
        f"Call-ID: {call_id}\r\n"
        f"CSeq: 1 REGISTER\r\n"
        f"Contact: <sip:{username}@{domain}:{port}>\r\n"
        f"Expires: 60\r\n"
        f"Content-Length: 0\r\n\r\n"
    )


def try_udp(domain: str, username: str, port: int) -> tuple[bool | None, str | None]:
    sock = None
    try:
        msg = _build_sip_register(domain, username, port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(_UDP_TIMEOUT)
        sock.sendto(msg.encode(), (domain, port))
        data, _ = sock.recvfrom(4096)
        response = data.decode(errors="ignore")
        logger.info("UDP %s:%d response: %s", domain, port, response[:120])

        if "SIP/2.0" not in response:
            return None, None
        if "401 " in response or "407 " in response or "200 " in response:
            return True, "SIP server verified via UDP."
        if "403" in response:
            return False, "Server rejected credentials (403). Check username/password."
        if "404" in response:
            return False, "Username not found on this server (404)."
        return True, "SIP server reachable via UDP."

    except socket.timeout:
        logger.debug("UDP timeout %s:%d", domain, port)
        return None, None
    except OSError as e:
        logger.debug("UDP error %s:%d - %s", domain, port, e)
        return None, None
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass


def test_sip_connection(domain: str, username: str, password: str):
    ip, err = resolve_domain(domain)
    if not ip:
        return False, f"Cannot resolve domain '{domain}'. Please check the domain name."

    for port in (5060, 5061):
        if try_tcp(domain, port):
            return True, f"SIP server reachable (TCP:{port})."

    for port in (5060, 5061):
        ok, msg = try_udp(domain, username, port)
        if ok is True:
            return True, msg
        if ok is False:
            return False, msg

    return True, (
        f"Domain found ({domain} -> {ip}), "
        "but full SIP test was unavailable in this environment. "
        "Credentials saved - they will be verified when the first call is made."
    )


def convert_to_wav(audio_path: str) -> str | None:
    if audio_path.lower().endswith(".wav"):
        probe_cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "a:0",
            "-show_entries", "stream=codec_name,sample_rate,channels",
            "-of", "csv=p=0",
            audio_path,
        ]
        try:
            probe = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=10)
            info = probe.stdout.strip()
            if "pcm_s16le" in info and "8000" in info and info.endswith(",1"):
                logger.info("WAV already in correct format: %s", audio_path)
                return audio_path
        except Exception:
            pass

    wav_path = os.path.splitext(audio_path)[0] + "_pjsua.wav"
    cmd = [
        "ffmpeg", "-y",
        "-i", audio_path,
        "-ar", "8000",
        "-ac", "1",
        "-acodec", "pcm_s16le",
        wav_path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0 and os.path.isfile(wav_path):
            logger.info("Converted %s -> %s", audio_path, wav_path)
            return wav_path
        else:
            logger.error("ffmpeg conversion failed: %s", result.stderr[:500])
            return None
    except FileNotFoundError:
        logger.error("ffmpeg not found. Cannot convert audio.")
        return None
    except Exception as e:
        logger.exception("Audio conversion error: %s", e)
        return None


def strip_country_code(phone_number: str, prefix: str = None) -> str:
    if prefix is None:
        prefix = COUNTRY_CODE_PREFIX
    num = phone_number.strip()
    if prefix and num.startswith(prefix):
        num = num[len(prefix):]
    elif prefix and prefix.startswith("+") and num.startswith(prefix[1:]):
        num = num[len(prefix) - 1:]
    return num


def _read_output_with_timeout(proc, timeout):
    output_lines = []
    stop_event = threading.Event()

    def _reader():
        try:
            while not stop_event.is_set():
                line = proc.stdout.readline()
                if not line:
                    break
                output_lines.append(line.decode(errors="ignore"))
        except Exception:
            pass

    reader_thread = threading.Thread(target=_reader, daemon=True)
    reader_thread.start()

    start_time = time.time()
    call_confirmed = False
    call_disconnected = False

    while time.time() - start_time < timeout:
        if not reader_thread.is_alive():
            break

        full_output = "".join(output_lines)
        if "CONFIRMED" in full_output:
            call_confirmed = True
        if call_confirmed and ("DISCONNECTED" in full_output or "state changed to DISCONNCTD" in full_output):
            call_disconnected = True
            break
        if "403 " in full_output or "404 " in full_output:
            break
        if "Registration failed" in full_output and "503" in full_output:
            break
        if any(code in full_output for code in ("408 ", "480 ", "486 ", "487 ")):
            time.sleep(2)
            break

        time.sleep(0.5)

    stop_event.set()
    return "".join(output_lines)


def _run_pjsua(sip_uri, sip_domain, sip_username, sip_password, wav_path, with_registration=True):
    cmd = [
        "pjsua",
        "--app-log-level=4",
        f"--id=sip:{sip_username}@{sip_domain}",
        "--realm=*",
        f"--username={sip_username}",
        f"--password={sip_password}",
        "--no-vad",
        "--no-tcp",
        f"--play-file={wav_path}",
        "--auto-play",
        "--auto-play-hangup",
        f"--duration={PJSUA_DURATION_SECONDS}",
    ]

    if with_registration:
        cmd.extend([
            f"--registrar=sip:{sip_domain}",
            "--reg-timeout=300",
        ])

    cmd.append(sip_uri)

    logger.info("Running pjsua: %s", " ".join(cmd))

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    try:
        output = _read_output_with_timeout(proc, CALL_TIMEOUT_SECONDS)
    except Exception as e:
        logger.error("Error reading pjsua output: %s", e)
        output = ""

    try:
        proc.stdin.write(b"q\n")
        proc.stdin.flush()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
            proc.wait(timeout=3)
        except Exception:
            pass

    return output


def _parse_pjsua_output(output: str) -> str:
    if "CONFIRMED" in output:
        return "answered"
    if "403 " in output:
        return "auth_rejected"
    if "404 " in output:
        return "user_not_found"
    if "Registration failed" in output and "503" in output:
        return "reg_503"
    if "486 " in output or "Busy Here" in output:
        return "busy"
    if "408 " in output or "Request Timeout" in output:
        return "timeout"
    if "480 " in output or "Temporarily Unavailable" in output:
        return "unavailable"
    if "487 " in output or "Request Terminated" in output:
        return "terminated"
    if "No route" in output or "PJSIP_ETRANSPORT" in output:
        return "network_error"
    return "unknown"


def place_sip_call(sip_domain, sip_username, sip_password, phone_number, audio_path, country_code_prefix=None):
    if not os.path.isfile(audio_path):
        logger.error("Audio file not found: %s", audio_path)
        return "failed", "Audio file not found"

    if audio_path.lower().endswith(".wav"):
        wav_path = audio_path
        wav_is_temp = False
    else:
        wav_path = convert_to_wav(audio_path)
        if wav_path is None:
            logger.error("Could not convert audio to WAV: %s", audio_path)
            return "failed", "Audio conversion failed"
        wav_is_temp = (wav_path != audio_path)

    dial_number = strip_country_code(phone_number, prefix=country_code_prefix)
    sip_uri = f"sip:{dial_number}@{sip_domain}"
    logger.info("SIP URI: %s (original: %s)", sip_uri, phone_number)

    try:
        logger.info("Attempt 1: calling %s with registration", phone_number)
        output1 = _run_pjsua(sip_uri, sip_domain, sip_username, sip_password, wav_path, with_registration=True)
        result1 = _parse_pjsua_output(output1)
        logger.info("pjsua [registered -> %s]: result=%s\nOutput:\n%s", phone_number, result1, output1[-2000:])

        if result1 == "answered":
            return "answered", "Call answered and audio played"
        if result1 == "auth_rejected":
            return "failed", "SIP server rejected credentials (403)"
        if result1 == "user_not_found":
            return "failed", "SIP username not found on server (404)"
        if result1 == "busy":
            return "not_answered", "Line was busy (486)"
        if result1 == "timeout":
            return "not_answered", "Call timed out - no answer (408)"
        if result1 == "unavailable":
            return "not_answered", "Number temporarily unavailable (480)"
        if result1 == "terminated":
            return "not_answered", "Call was terminated (487)"
        if result1 == "network_error":
            return "failed", "Network error - cannot reach SIP server"

        if result1 == "reg_503":
            logger.info("Registration returned 503. Retrying %s without registration (IP-auth)", phone_number)
            output2 = _run_pjsua(sip_uri, sip_domain, sip_username, sip_password, wav_path, with_registration=False)
            result2 = _parse_pjsua_output(output2)
            logger.info("pjsua [no-reg -> %s]: result=%s\nOutput:\n%s", phone_number, result2, output2[-2000:])

            if result2 == "answered":
                return "answered", "Call answered (IP-auth mode after 503)"
            if result2 == "auth_rejected":
                return "failed", "SIP auth rejected (403) in IP-auth mode"
            if result2 == "user_not_found":
                return "failed", "SIP user not found (404) in IP-auth mode"
            if result2 == "busy":
                return "not_answered", "Line was busy (486)"
            if result2 == "timeout":
                return "not_answered", "No answer (408)"
            if result2 == "unavailable":
                return "not_answered", "Number unavailable (480)"
            if result2 == "terminated":
                return "not_answered", "Call terminated (487)"
            if result2 == "network_error":
                return "failed", "Network error - cannot reach SIP server"
            return "not_answered", "Call completed but status unclear (IP-auth mode)"

        return "not_answered", "Call completed but status unclear"

    except FileNotFoundError:
        logger.error("pjsua not installed")
        return "failed", "pjsua is not installed on this server"
    except Exception as exc:
        logger.exception("Unexpected error placing call: %s", exc)
        return "failed", f"Unexpected error: {exc}"
    finally:
        if wav_is_temp and wav_path and os.path.isfile(wav_path):
            try:
                os.remove(wav_path)
            except Exception:
                pass
