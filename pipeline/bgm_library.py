"""
Copyright-Safe Cheerful/Upbeat BGM Library Manager (`pipeline/bgm_library.py`).

Responsibilities (User Requirement #2):
  - Maintains a curated pool of 4 cheerful, pleasant, upbeat instrumental tracks in `assets/bgm/`:
      1. `bgm_upbeat_1_sunny_desk.m4a`     (C Major, 112 BPM — bright acoustic/marimba review groove)
      2. `bgm_upbeat_2_fresh_morning.m4a`  (G Major, 116 BPM — warm electric piano & light ukulele pulse)
      3. `bgm_upbeat_3_smart_living.m4a`   (F Major, 108 BPM — cheerful lo-fi pop pluck & soft shaker)
      4. `bgm_upbeat_4_happy_kitchen.m4a`  (D Major, 120 BPM — upbeat cozy marimba & Rhodes progression)
  - 100% original procedural studio synthesis (CC0 / zero Content-ID / zero copyright claims).
  - Records verifiable license metadata in `assets/bgm/license.json`.
  - Randomly selects 1 track per video (`pick_random_bgm_track()`), mixed at subtle low volume
    with sidechain ducking under the voiceover in M4.
"""
from __future__ import annotations

import json
import math
import random
import subprocess
import wave
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BGM_DIR = PROJECT_ROOT / "assets" / "bgm"
LICENSE_JSON_PATH = BGM_DIR / "license.json"

BGM_CATALOG: List[Dict[str, object]] = [
    {
        "track_id": "bgm_upbeat_1_sunny_desk",
        "filename": "bgm_upbeat_1_sunny_desk.m4a",
        "title": "Sunny Desk Groove (Upbeat Review #1)",
        "key": "C Major (I - V - vi - IV)",
        "bpm": 112,
        "duration_sec": 64.0,
        "license": "CC0-1.0 Public Domain / Original Procedural Studio Synthesis (Zero Copyright / No Content-ID)",
        # C4-E4-G4-B4, G3-B3-D4-F#4, A3-C4-E4-G4, F3-A3-C4-E4
        "chords": [
            (261.63, 329.63, 392.00, 493.88),
            (196.00, 246.94, 293.66, 392.00),
            (220.00, 261.63, 329.63, 392.00),
            (174.61, 220.00, 261.63, 329.63),
        ],
        "arp_pattern": [0, 1, 2, 3, 2, 1, 3, 2],
    },
    {
        "track_id": "bgm_upbeat_2_fresh_morning",
        "filename": "bgm_upbeat_2_fresh_morning.m4a",
        "title": "Fresh Morning Pop (Upbeat Review #2)",
        "key": "G Major (I - vi - IV - V)",
        "bpm": 116,
        "duration_sec": 64.0,
        "license": "CC0-1.0 Public Domain / Original Procedural Studio Synthesis (Zero Copyright / No Content-ID)",
        # G3-B3-D4-G4, E3-G3-B3-D4, C4-E4-G4-B4, D4-F#4-A4-C5
        "chords": [
            (196.00, 246.94, 293.66, 392.00),
            (164.81, 196.00, 246.94, 293.66),
            (261.63, 329.63, 392.00, 493.88),
            (293.66, 369.99, 440.00, 523.25),
        ],
        "arp_pattern": [0, 2, 1, 3, 0, 3, 2, 1],
    },
    {
        "track_id": "bgm_upbeat_3_smart_living",
        "filename": "bgm_upbeat_3_smart_living.m4a",
        "title": "Smart Living Pluck (Upbeat Review #3)",
        "key": "F Major (I - IV - ii - V)",
        "bpm": 108,
        "duration_sec": 64.0,
        "license": "CC0-1.0 Public Domain / Original Procedural Studio Synthesis (Zero Copyright / No Content-ID)",
        # F3-A3-C4-E4, Bb3-D4-F4-A4, G3-Bb3-D4-F4, C4-E4-G4-Bb4
        "chords": [
            (174.61, 220.00, 261.63, 329.63),
            (233.08, 293.66, 349.23, 440.00),
            (196.00, 233.08, 293.66, 349.23),
            (261.63, 329.63, 392.00, 466.16),
        ],
        "arp_pattern": [0, 1, 3, 2, 0, 2, 3, 1],
    },
    {
        "track_id": "bgm_upbeat_4_happy_kitchen",
        "filename": "bgm_upbeat_4_happy_kitchen.m4a",
        "title": "Happy Kitchen & Desk Bounce (Upbeat Review #4)",
        "key": "D Major (I - V - vi - IV)",
        "bpm": 120,
        "duration_sec": 64.0,
        "license": "CC0-1.0 Public Domain / Original Procedural Studio Synthesis (Zero Copyright / No Content-ID)",
        # D4-F#4-A4-C#5, A3-C#4-E4-A4, B3-D4-F#4-A4, G3-B3-D4-G4
        "chords": [
            (293.66, 369.99, 440.00, 554.37),
            (220.00, 277.18, 329.63, 440.00),
            (246.94, 293.66, 369.99, 440.00),
            (196.00, 246.94, 293.66, 392.00),
        ],
        "arp_pattern": [0, 2, 3, 1, 2, 0, 3, 2],
    },
]


def _synthesize_upbeat_track_wav(
    wav_path: Path,
    chords: List[Tuple[float, float, float, float]],
    arp_pattern: List[int],
    bpm: int,
    duration_sec: float = 64.0,
    sr: int = 44100,
) -> None:
    """Synthesize a warm, cheerful acoustic-pluck & marimba instrumental track."""
    n_samples = int(duration_sec * sr)
    left = np.zeros(n_samples, dtype=np.float32)
    right = np.zeros(n_samples, dtype=np.float32)

    beat_sec = 60.0 / float(bpm)
    bar_sec = beat_sec * 4.0
    eighth_sec = beat_sec / 2.0

    rng = np.random.default_rng(seed=bpm * 17)

    # 1. Warm Rhodes/electric-piano pad chords on every bar
    t_bar = 0.0
    bar_idx = 0
    while t_bar < duration_sec:
        chord = chords[bar_idx % len(chords)]
        start_i = int(t_bar * sr)
        len_i = min(int(bar_sec * sr), n_samples - start_i)
        if len_i <= 0:
            break
        tt = np.arange(len_i, dtype=np.float32) / sr
        env = np.minimum(1.0, tt / 0.04) * np.exp(-tt / (bar_sec * 0.75))

        pad_l = np.zeros(len_i, dtype=np.float32)
        pad_r = np.zeros(len_i, dtype=np.float32)
        # Warm root bass note (1 octave down)
        bass_f = chord[0] * 0.5
        bass_wave = 0.16 * np.sin(2.0 * math.pi * bass_f * tt) * np.exp(-tt / (bar_sec * 0.55))
        pad_l += bass_wave
        pad_r += bass_wave

        for idx_n, freq in enumerate(chord):
            tone = (
                0.08 * np.sin(2.0 * math.pi * freq * tt)
                + 0.025 * np.sin(2.0 * math.pi * (freq * 2.0) * tt)
            ) * env
            pan = 0.35 + 0.2 * (idx_n % 2)
            pad_l += tone * pan
            pad_r += tone * (1.0 - pan)

        left[start_i : start_i + len_i] += pad_l
        right[start_i : start_i + len_i] += pad_r
        t_bar += bar_sec
        bar_idx += 1

    # 2. Cheerful marimba / ukulele eighth-note arpeggio plucks
    t_step = 0.0
    step_idx = 0
    while t_step < duration_sec:
        cur_bar = int(t_step // bar_sec)
        chord = chords[cur_bar % len(chords)]
        note_idx = arp_pattern[step_idx % len(arp_pattern)]
        freq = chord[note_idx] * (2.0 if (step_idx % 8 in (2, 6)) else 1.0)

        start_i = int(t_step * sr)
        len_i = min(int(eighth_sec * 1.35 * sr), n_samples - start_i)
        if len_i <= 0:
            break
        tt = np.arange(len_i, dtype=np.float32) / sr
        # Plucked marimba envelope: fast attack, woody harmonic decay
        env = np.minimum(1.0, tt / 0.006) * np.exp(-tt / 0.19)
        pluck = (
            0.14 * np.sin(2.0 * math.pi * freq * tt)
            + 0.045 * np.sin(2.0 * math.pi * (freq * 2.0) * tt) * np.exp(-tt / 0.08)
            + 0.015 * np.sin(2.0 * math.pi * (freq * 3.0) * tt) * np.exp(-tt / 0.04)
        ) * env

        pan = 0.3 + 0.4 * ((step_idx % 3) / 2.0)
        left[start_i : start_i + len_i] += pluck * pan
        right[start_i : start_i + len_i] += pluck * (1.0 - pan)

        # Subtle soft shaker on off-beats
        if step_idx % 2 == 1:
            sh_len = min(int(0.055 * sr), n_samples - start_i)
            if sh_len > 0:
                sh_tt = np.arange(sh_len, dtype=np.float32) / sr
                sh_env = np.sin(math.pi * sh_tt / 0.055) ** 2
                noise = rng.normal(0.0, 0.018, size=sh_len).astype(np.float32) * sh_env
                left[start_i : start_i + sh_len] += noise
                right[start_i : start_i + sh_len] += noise

        t_step += eighth_sec
        step_idx += 1

    # Normalize cleanly to -6 dBFS peak before AAC encoding
    stereo = np.stack([left, right], axis=1)
    peak = float(np.max(np.abs(stereo))) or 1.0
    stereo = (stereo / peak) * 0.48
    pcm16 = np.clip(stereo * 32767.0, -32768, 32767).astype(np.int16)

    wav_path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(wav_path), "wb") as wf:
        wf.setnchannels(2)
        wf.setsampwidth(2)
        wf.setframerate(sr)
        wf.writeframes(pcm16.tobytes())


def ensure_bgm_library(bgm_dir: Path = BGM_DIR) -> List[Path]:
    """Ensure all 4 cheerful/upbeat royalty-free BGM tracks and `license.json` exist on disk."""
    bgm_dir.mkdir(parents=True, exist_ok=True)
    m4a_paths: List[Path] = []

    for item in BGM_CATALOG:
        m4a_path = bgm_dir / str(item["filename"])
        if not m4a_path.exists() or m4a_path.stat().st_size < 10_000:
            tmp_wav = bgm_dir / f"{item['track_id']}.tmp.wav"
            _synthesize_upbeat_track_wav(
                wav_path=tmp_wav,
                chords=list(item["chords"]),  # type: ignore[arg-type]
                arp_pattern=list(item["arp_pattern"]),  # type: ignore[arg-type]
                bpm=int(item["bpm"]),  # type: ignore[arg-type]
                duration_sec=float(item["duration_sec"]),  # type: ignore[arg-type]
            )
            subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-i",
                    str(tmp_wav),
                    "-af",
                    "lowpass=f=7500,highpass=f=65",
                    "-c:a",
                    "aac",
                    "-b:a",
                    "160k",
                    str(m4a_path),
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            tmp_wav.unlink(missing_ok=True)
        m4a_paths.append(m4a_path)

    license_payload = {
        "schema_version": "1.0.0",
        "policy": "100% Copyright-Safe / CC0 Original Procedural Instrumental Library (Zero Third-Party Claims)",
        "tracks": [
            {
                "track_id": item["track_id"],
                "filename": item["filename"],
                "title": item["title"],
                "key": item["key"],
                "bpm": item["bpm"],
                "duration_sec": item["duration_sec"],
                "license": item["license"],
            }
            for item in BGM_CATALOG
        ],
    }
    license_file = bgm_dir / "license.json"
    license_file.write_text(
        json.dumps(license_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return m4a_paths


def pick_random_bgm_track(
    bgm_dir: Path = BGM_DIR,
    exclude_filenames: Optional[List[str]] = None,
) -> Path:
    """Randomly pick 1 cheerful/upbeat track from `assets/bgm/` (avoiding recent tracks when possible)."""
    tracks = ensure_bgm_library(bgm_dir)
    excluded = set(exclude_filenames or [])
    fresh = [p for p in tracks if p.name not in excluded]
    pool = fresh if fresh else tracks
    return random.choice(pool)
