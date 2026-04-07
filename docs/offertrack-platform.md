# offerTrackCrawler → offerTrackPlatform

本仓库的爬取入口为 **Rust 单一实现**（`rust/`），**导出契约**为 `out/jobs.json`、`state/jobs.db`、稳定 `job_id`；需要表格时对 `offertrack-crawl` 传 **`--csv`** 才会生成 **`out/jobs.csv`**：

- **`cargo run -p offertrack-crawler --bin offertrack-crawl`**：读取 `config/crawl_sites.json` 中的 **`api_sources`**（Greenhouse、Lever、Ashby、RSS/Atom、Jobright、**`amazon_jobs`**、**`workday`**）以及启用的 HTML **`sites`**（`GenericSchemaOrgExtractor` / Schema.org），适合日批全量；多源结果在写出前 **按规范 URL 合并去重**（见 `README`）。

## 推送到主站职位库

主站提供管理员接口，按爬虫的稳定键 `job_id`（写入平台的 `externalId`）与 `source: company_site` 做 upsert；公司名会映射为平台内的 `Company`（无 HR 用户的占位公司，按名称去重）。

1. 启动 **offerTrackPlatform** 后端（默认 `http://localhost:3000`）。
2. 若后端设置了 `ADMIN_API_KEY`，本地需导出密钥。
3. 使用 **完整** `jobs.json`（含 `title`, `company`, `url`, `job_id` 等）。**不要用** `--minimal-export` 的 `jobs_min.json` 推送（缺少 title/company/url）。

```bash
cd /path/to/offerTrackCrawler

# Rust（需当前 stable Rust，见 rust/rust-toolchain.toml）
cd rust && cargo run --release -p offertrack-crawler --bin offertrack-crawl -- \
  --config ../config/crawl_sites.json --out ../out/jobs.json --db ../state/jobs.db
cd ..

export OFFERTRACK_API_URL=http://localhost:3000
# export OFFERTRACK_ADMIN_KEY=...   # 若平台配置了 ADMIN_API_KEY

cd rust && cargo run --release -p offertrack-crawler --bin offertrack-push -- ../out/jobs.json
# 干跑：同上命令在路径后加 --dry-run
```

成功后，候选人在主站刷新职位即可看到新数据（与现有匹配/embedding 配置有关）。

## 为什么平台 `Job.id` 和爬虫 `job_id` 不一样？

- **爬虫 `job_id`**：多源结果先 **按规范申请 URL 合并去重**（`source` 会合并成多标签字符串），再用 **仅规范 URL** 算出的稳定 UUID v5；SQLite 行键与之对齐。  
- **平台 `Job.id`**：主站里所有职位（HR 手工、爬虫、以后别的渠道）共用的主键，API、投递、向量融合都认它。

从爬虫 **`ingest/crawler-jobs`** 写入且带 **`source` + `externalId`**（爬虫里的 `job_id`）时：

1. **`Job.id`** 使用可逆编码 **`encodeCrawlerJobId(companyId, externalId)`**，形如 **`cj_` + base64url(JSON)**，同一公司 + 同一条爬虫键在任意机器上 **id 字符串一致**，且 **无需多存一份「合成键」**：用 **`decodeCrawlerJobId(jobId)`** 即可取回 `companyId` 与爬虫 `job_id`。  
2. **`externalId`** 字段仍会写入职位对象，方便按「列表键」过滤、和爬虫导出对照；与 `cj_` 里编码的信息一致（非加密，只是冗余便利）。  
3. **「公司名 + job_id」可读形式**（如 `stripe:uuid…`）不单独落库；在 **`GET .../jobs-for-embedding.json`** 里用 **`stableIngestKey`** / **`crawlerIngest`** **现算**即可。

HR 在后台新建的职位仍用随机 UUID（无 `cj_` 前缀，`decodeCrawlerJobId` 返回 `null`）。

## JD 向量与 ModelTraining（与平台 Job.id 对齐）

离线向量里的 **`jd_id` 必须等于平台 `Job.id`**。导出接口与 `platform_export_jd_text_parquet.py` 已按该 id 输出；爬虫侧的 `job_id` 请对照 **`externalId`** / **`stableIngestKey`** 理解，不要直接当平台主键。

已实现流程：

1. 推送爬虫结果到平台（上文）。
2. 调用 **`GET /api/v1/admin/jobs-for-embedding.json`**（返回 `jobs[]`，每条含 **`id` 与 `jd_id`（相同）**、`title`、`description`、`skills` 等）；或直接在 ModelTraining 里跑一键脚本：
   - `python training/scripts/platform_export_jd_text_parquet.py --output-parquet ... [--save-json ...]`
3. 用导出的 Parquet / JSON 继续 **`python -m embedding`** 或 **`embed_rag_corpus_ray.py --jd-json`**（快照需为 **JSON 数组**；`--save-json` 保存的即是数组，可直接给 `--jd-json`）。

英文步骤与命令全文：`offerTrackModelTraining/docs/platform-embedding-integration.md`（**Crawler → Platform → JD embeddings**）。

## 相关路径（同 monorepo 时）

| 目录 | 作用 |
|------|------|
| `offerTrackCrawler/rust/` | Rust：`api_sources` + HTML `sites` → `out/jobs.json` + SQLite |
| `offerTrackCrawler/` | Rust `offertrack-push`（推送导出） |
| `offerTrackPlatform/backend/` | `POST .../ingest/crawler-jobs`、`GET .../jobs-for-embedding.json` |
| `offerTrackModelTraining/` | `training/scripts/platform_export_jd_text_parquet.py`、嵌入与 Serving |
