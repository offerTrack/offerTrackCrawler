# Web Crawler Architecture for Job Information Extraction

**Implementation:** the production crawler is the Rust binary **`offertrack-crawl`** (`rust/offertrack-crawler/`). HTML + Schema.org logic lives under **`src/html/`** (`schema_org.rs`, `mod.rs`). This document describes behavior at a high level.

## Overview

The web crawler component is designed to automatically discover and extract job postings from various job sites with a strict freshness window (configurable, default aligned with `freshness_days` in `crawl_sites.json`). The system aims for coverage while respecting site policies and rate limits.

## Architecture Components

### 1. Crawler Engine