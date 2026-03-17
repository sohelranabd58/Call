import subprocess
import logging
import socket
import uuid
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

logger = logging.getLogger(__name__)

_TCP_TIMEOUT = 4
_UDP_TIMEOUT = 4
_DNS_TIMEOUT = 6


# ── DNS ────────────────────────────────────────────────────────────────────────

def resolve_domain(domain: str) -> tuple[str | None, str | None]:
    """
    Resolve a domain to an IP address with a hard timeout.
    Returns (ip, None) on success, (None, error_msg) on failure.
    """
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(socket.gethostbyname, domain)
    try:
        ip = future.result(timeout=_DNS_TIMEOUT)
        executor.shutdown(wait=False)
        logger.info("DNS: %s → %s", domain, ip)
        return ip, None
    except FutureTimeout:
        executor.shutdown(wait=False)
        logger.warning("DNS timeout for %s", domain)
        return None, "timeout"
    except Exception as e:
        executor.shutdown(wait=False)
        logger.warning("DNS error for %s: %s", domain, e)
        return None, str(e)


# ── TCP ────────────────────────────────────────────────────────────────────────

def try_tcp(domain: str, port: int) -> bool:
    """Returns True if a TCP connection to domain:port succeeded."""
    try:
        sock = socket.create_connection((domain, port), timeout=_TCP_TIMEOUT)
        sock.close()
        logger.info("TCP OK: %s:%d", domain, port)
        return True
    except OSError as e:
        logger.debug("TCP fail %s:%d — %s", domain, port, e)
        return False


# ── UDP SIP ────────────────────────────────────────────────────────────────────

def _build_sip_register(domain, username, port):
    branch  = "z9hG4bK" + uuid.uuid4().hex[:10]
    tag     = uuid.uuid4().hex[:8]
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
    """
    Sends a SIP REGISTER over UDP and waits for a response.
    Returns:
      (True, msg)  — server replied positively (401/200/407)
      (False, msg) — server replied with a credential error (403/404)
      (None, None) — no response (timeout/unreachable)
    """
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
            return True, "✅ SIP server verified via UDP."
        if "403" in response:
            return False, "❌ Server rejected credentials (403). Check username/password."
        if "404" in response:
            return False, "❌ Username not found on this server (404)."
        return True, "✅ SIP server reachable via UDP."

    except socket.timeout:
        logger.debug("UDP timeout %s:%d", domain, port)
        return None, None
    except OSError as e:
        logger.debug("UDP error %s:%d — %s", domain, port, e)
        return None, None
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass


# ── Combined test (used by scheduler for background retries) ───────────────────

def test_sip_connection(domain: str, username: str, password: str):
    """
    Quick combined test. Returns (success: bool, message: str).
    Used internally; the bot handler uses the step functions above for live feedback.
    """
    ip, err = resolve_domain(domain)
    if not ip:
        return False, f"❌ Cannot resolve domain '{domain}'. Please check the domain name."

    # TCP
    for port in (5060, 5061):
        if try_tcp(domain, port):
            return True, f"✅ SIP server reachable (TCP:{port})."

    # UDP
    for port in (5060, 5061):
        ok, msg = try_udp(domain, username, port)
        if ok is True:
            return True, msg
        if ok is False:
            return False, msg

    # Domain resolves but no SIP response — allow with warning
    return True, (
        f"⚠️ <b>Domain found</b> ({domain} → {ip}), "
        "but full SIP test was unavailable in this environment.\n"
        "Credentials saved — they will be verified when the first call is made."
    )


# ── Audio conversion ───────────────────────────────────────────────────────────

def _convert_to_wav(audio_path: str) -> str | None:
    """
    Convert any audio file to a pjsua-compatible WAV (PCM, 16-bit, 8000 Hz, mono).
    Returns the path to the WAV file, or None on failure.
    The caller is responsible for deleting the returned temp file if it differs
    from the original.
    """
    if audio_path.lower().endswith(".wav"):
        return audio_path

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
            logger.info("Converted %s → %s", audio_path, wav_path)
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


# ── Call placement ─────────────────────────────────────────────────────────────

def place_sip_call(sip_domain, sip_username, sip_password, phone_number, audio_path):
    """
    Places an outbound SIP call using pjsua.
    Returns: "answered" | "not_answered" | "failed"
    """
    if not os.path.isfile(audio_path):
        logger.error("Audio file not found: %s", audio_path)
        return "failed"

    wav_path = _convert_to_wav(audio_path)
    if wav_path is None:
        logger.error("Could not convert audio to WAV: %s", audio_path)
        return "failed"

    sip_uri = f"sip:{phone_number}@{sip_domain}"

    def _run_pjsua(extra_args):
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
            "--duration=55",
        ] + extra_args + [sip_uri]
        return subprocess.run(
            cmd,
            timeout=120,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
        )

    def _parse_result(output, label):
        logger.info("pjsua output [%s → %s]:\n%s", label, phone_number, output)
        if "CONFIRMED" in output:
            return "answered"
        if "403 " in output:
            return "failed"
        if "Registration failed" in output and "503" in output:
            return None  # signal: try fallback
        if any(c in output for c in (
            "408 ", "480 ", "486 ", "487 ",
            "Request Timeout", "Temporarily Unavailable",
            "Busy Here", "Request Terminated",
        )):
            return "not_answered"
        return None  # inconclusive — try fallback

    try:
        logger.info("Calling %s via %s@%s (with registration)", phone_number, sip_username, sip_domain)
        r1 = _run_pjsua([
            f"--registrar=sip:{sip_domain}",
            "--reg-timeout=300",
        ])
        res = _parse_result(r1.stdout + r1.stderr, "registered")

        if res == "answered":
            logger.info("Call to %s: ANSWERED", phone_number)
            return "answered"
        if res == "failed":
            logger.error("SIP hard failure for %s", phone_number)
            return "failed"

        # Registration failed (503) or inconclusive → retry without registration
        logger.info("Retrying call to %s without registration (IP-auth mode)", phone_number)
        r2 = _run_pjsua([])
        output2 = r2.stdout + r2.stderr
        logger.info("pjsua output [no-reg → %s]:\n%s", phone_number, output2)

        if "CONFIRMED" in output2:
            logger.info("Call to %s: ANSWERED (no-reg mode)", phone_number)
            return "answered"
        if "403 " in output2:
            logger.error("SIP auth rejected (403) for %s", phone_number)
            return "failed"
        if any(c in output2 for c in (
            "408 ", "480 ", "486 ", "487 ",
            "Request Timeout", "Temporarily Unavailable",
            "Busy Here", "Request Terminated",
        )):
            logger.info("Call to %s: NOT ANSWERED", phone_number)
            return "not_answered"

        logger.info("Call to %s: completed (status unclear)", phone_number)
        return "not_answered"

    except FileNotFoundError:
        logger.error("pjsua not installed.")
        return "failed"
    except subprocess.TimeoutExpired:
        logger.error("Call to %s timed out.", phone_number)
        return "not_answered"
    except Exception as exc:
        logger.exception("Unexpected error placing call: %s", exc)
        return "failed"
    finally:
        if wav_path != audio_path and os.path.isfile(wav_path):
            try:
                os.remove(wav_path)
            except Exception:
                pass
