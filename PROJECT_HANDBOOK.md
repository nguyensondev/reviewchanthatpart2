# Project Handbook — Autonomous Short-Form Product Review & Affiliate Pipeline

> **Purpose:** Concise architectural & operational handoff document so any new conversation or cloned niche workspace immediately understands 100% of the system without replaying chat history. Always read alongside `AGENTS.md`.

---

## 1. End-to-End Black-Box Pipeline (`M_TREND -> M5`)
Every stage module is strictly decoupled (0 cross-stage imports, JSON In/Out, verified by `python3 -m pipeline.verify_all_5_criteria`):

1. **`M_TREND` (`pipeline/m_trend_intelligence.py` + `config/trend_sources_registry.json`):**
   - Polls VN RSS feeds, GDELT, YouTube Data API v3, and owned Shopee CSV signals into a self-healing SQLite3 store (`data/trend_intelligence.sqlite3`) with HTTP `304`, 6h Stage-B cache, Half-Life time-decay, 3-tier re-entry, and `NORMAL_TREND` / `FAST_TRACK_TREND` priority tiers.
2. **`M0` (`pipeline/m0_discovery.py`):**
   - Autonomous category rotation (`CATEGORY_ROTATION_TAXONOMY`) + multi-brand candidate discovery + zero-quota YouTube search (`yt-dlp`).
   - Enforces **14-day category cooldown** and **30-day `(brand, category_id)` sibling-variant dedup** via `SKURegistryManager` (`pipeline/sku_registry.py`).
3. **`M1` (`pipeline/m1_filter.py` + `pipeline/brand_video_gate.py`):**
   - Hard gate (dealbreakers, ambiguous listings, official brand video gate, 30-day `RECENT_BRAND_CATEGORY_SIBLING` block) + 100-point objective scorer + head-to-head verdict.
4. **`M1.5` (`pipeline/m1_5_combo_curator.py`):**
   - Natural workflow combo curator (strictly bypasses forced/unnatural combos).
5. **`M2` (`pipeline/m2_script_gen.py`):**
   - Generates 5-beat Vietnamese review scripts across 6 Upgrade-1 narrative angles (`REVERSE_CAVEAT_FIRST`, `HYPE_VS_REALITY_TEST`, `SPEC_TRAP_AND_HIDDEN_COST`, `ANTI_DUST_DRAWER_TEST`, `HABIT_DIAGNOSIS_STORY`, `FAST_25S_MICRO_LOOP`).
   - Strict rules: 100% accented UTF-8 Vietnamese, 2 crisp 10–16 word sentences per beat, **common-sense product-context caveats** (never criticize a product outside its intended design), logical Beat-5 verdict, **never read raw factory codes or long English titles aloud**, and **never mention "Shopee/Lazada/Tiki" in spoken audio** (keeps MP4 safe for TikTok + YouTube + Facebook).
6. **`M3` (`pipeline/m3_qaqc.py`):**
   - 3-layer QA/QC auditor: deterministic Layer-1 (`check_vietnamese_diacritics`, `audit_tts_readability`), Layer-2 LLM semantic + **Common-Sense Product Logic & Verdict Gate**, and Layer-3 TTS/Subtitle split (`tts_voiceover_vi` vs `subtitle_text_vi`).
7. **`M4` (`pipeline/m4_video_producer.py`):**
   - **Primary TTS (100%):** **Gemini 2.5 Flash TTS (`gemini-2.5-flash-preview-tts`, `gemini_aoede` Female / `gemini_puck` Male)** synthesized in **1 single API call per video** (`_synth_full_video_with_gemini_tts`) + FFmpeg `silencedetect` clause alignment.
   - **Fallback Chain:** `TikTok TTS (BV074/BV075)` -> `Microsoft Edge Neural TTS (HoaiMy/NamMinh)` -> Emergency `Google TTS`.
   - **Audio Polish:** Studio de-breath (`highpass=95Hz`, `agate`), warm EQ, `atempo=1.06`, `0.30s` opening cushion + `1.8s` linear fade-in (`curve=tri`), `-15.5 LUFS`, CC0 procedural BGM ducking.
   - **Visuals:** Layout `B1` (`1080×1920`), `<= 3.6s` face-free/caption-free official clips, word-safe PIL spec/caveat/end cards, crisp `Fontsize=44` ASS subtitles, and **Uniform 9:16 Studio SKU Cover Thumbnail (`*_cover.jpg`)** with locked `900×850` product frame.
   - **Self-Heal:** Auto-purges unrenderable scripts (`0 verified official brand videos`) from queue.
8. **`M4.5` (`pipeline/m4_5_video_qaqc.py`):**
   - Gemini Vision 6-frame storyboard audit; auto-purges defective videos, blacklists offending source video IDs, and refunds M4 quota/cooldown.
9. **`PublishPackager` (`pipeline/publish_packager.py`) + `CloudflareShortLinkManager` (`pipeline/cloudflare_shortener.py`):**
   - Generates Dual-Head SEO titles (`format_vnd_short`, `truncate_words_clean` — no mid-word cutoffs), YouTube 4-block SEO description, Facebook Reels checklist caption, **0%-Shopee High-SEO TikTok caption**, AI disclosure footer + `#AIReview`, and merges/syncs Cloudflare Worker short links + Bio Hub + `/terms`, `/privacy`, `/tiktok/callback`.
10. **`M5` (`pipeline/m5_publisher.py`) + `TelegramHealthBot` (`pipeline/telegram_health_bot.py`):**
    - Human-paced publishing (`active hours`, `weekly random rest day`, `2–4h cooldown`, per-platform crash-resume across **YouTube Shorts** + **Facebook Reels** + **TikTok Content Posting API v2**).
    - Uploads custom 9:16 SKU cover thumbnails via YouTube `thumbnails.set` and Facebook `/{video_id}/thumbnails`, sets YouTube `containsSyntheticMedia=True`, and auto-refreshes TikTok 365-day `refresh_token`.
    - Telegram Bot provides status, module health, fresh error scan (`< 6h`), and 1-tap `/update` (`git pull` + hot-reload).

---

## 2. Checklist When Cloning This Project to a New Niche (e.g., Cleaning & Mini Home Appliances)
1. **Niche Taxonomy & Keywords:**
   - Replace `CATEGORY_ROTATION_TAXONOMY` in `pipeline/m0_discovery.py` and `CATEGORY_SHORT_KEYWORDS_VI` in `pipeline/publish_packager.py`.
   - Update `config/trend_sources_registry.json` and `data/brand_channel_whitelist.json` with official YouTube channels for the new niche's top brands.
2. **Channel Branding & Visual Identity:**
   - Update channel name & accent/gradient palette in `render_uniform_sku_cover_jpeg()` (`pipeline/m4_video_producer.py`) and Bio Hub header (`pipeline/cloudflare_shortener.py`) so the new channel has a distinct visual identity.
3. **Isolated Credentials & Quotas (Never Commit to Git):**
   - Create a fresh `.env` with a **separate `GEMINI_API_KEY`** (so channels never compete for Free-Tier RPD/TTS quota), separate `TELEGRAM_BOT_TOKEN` (new bot via `@BotFather`), and new `CLOUDFLARE_WORKER_NAME`.
   - Create a fresh `config/platform_credentials.json` with the new channel's YouTube OAuth2, Facebook Page Token, and TikTok Sandbox/Production credentials (Shopee `affiliate_id` can be shared safely).
4. **Staggered `launchd` Schedule on MacBook Pro 2015:**
   - Use distinct plist labels (e.g. `com.review.<niche>.daily` and `com.review.<niche>.bot`) and offset start minutes by ~15–20 minutes from Channel #1 so two FFmpeg renders never collide on Dual-Core i5.
