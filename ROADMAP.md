# Yol haritası: rakiplerin hepsinin yaptığı + fazlası

Hedef: Node/TS ekosisteminde en çok kullanılan resilience paketi olmak.
Kaynaklar: cockatiel (2.6M/hafta), opossum (1.7M), @julr/tenace, @nestjs/resilience, p-retry (71M), async-retry, exponential-backoff, bottleneck, rate-limiter-flexible, axios-retry, fetch-retry, p-limit/p-queue, Polly v8, Netflix concurrency-limits.

Durum: ✅ var · 🔨 bu dalgada · ⏳ planlı

## Dalga A — çekirdek (paylaşılan dosyalara dokunur, sırayla yapılır)

| Özellik | Kimde var | Durum |
|---|---|---|
| Retry / timeout / breaker / fallback / hedging / bulkhead / token bucket / chaos | Polly, cockatiel, tenace | ✅ |
| 4xx ve red-farkında varsayılanlar, Duration string, count window, halfOpenMaxCalls, deadline propagation | @nestjs/resilience | ✅ |
| Her ortamda çalışma (tarayıcı, Bun ✓ test edildi, Deno, Workers): çekirdekte statik `node:` importu olmasın | cockatiel, opossum, tenace | ✅ |
| `timeProvider` + `FakeTime` (beklemeden test) | Polly TimeProvider, tenace TestClock | ✅ |
| Art arda N hata ile açılan breaker | cockatiel ConsecutiveBreaker, tenace | ✅ |
| Breaker state kaydet / geri yükle (serverless) | cockatiel, opossum | ✅ |
| Cooperative timeout (callback'i bekle) seçeneği | Polly, cockatiel | ✅ |
| Tek satır API: `retry(fn)`, `timeout(fn)`, `NonRetryableError` | p-retry, async-retry, exponential-backoff | ✅ |

## Dalga B — yeni stratejiler (ayrı dosyalar, paralel yapılabilir)

| Özellik | Kimde var | Durum |
|---|---|---|
| Retry budget (retry fırtınası koruması) | tenace, Google SRE | ✅ |
| Cache + istek birleştirme (single-flight) + stale fallback | opossum, tenace | ✅ (stale-while-revalidate ⏳) |
| Fixed / sliding window, anahtarlı (kullanıcı başına) rate limit, ağırlık, öncelik | rate-limiter-flexible, bottleneck, Polly | ✅ (öncelik kuyruğu, FIFO bekleme ⏳) |
| Adaptive concurrency (Vegas / Gradient2 / AIMD) | Netflix (JS'te yok) | ✅ (periyodik minRTT probe ⏳) |
| Lock (in-memory + store arayüzü, lease yenileme, fencing token) | tenace, @nestjs/locks | ✅ |
| `all` / `map` (eşzamanlılık + policy), `waitFor` | tenace, p-map | ✅ |
| Breaker warm-up ✅ · latency → OTel/Prometheus histogram ✅ · periyodik healthCheck ⏳ | opossum | ✅/⏳ |

## Dalga C — adaptörler (subpath export, kullanılmazsa pakete girmez)

| Özellik | Kimde var | Durum |
|---|---|---|
| `/fetch` (hazır HTTP katmanı) ✅ · `/axios` interceptor ⏳ | axios-retry, fetch-retry, Microsoft.Extensions.Http.Resilience | ✅/⏳ |
| `/otel`, `/prometheus` | tenace-otel, opossum-prometheus | ✅ |
| Store arayüzü + `/redis` (breaker, rate limit, lock, cache) | tenace adaptörleri, rate-limiter-flexible, zenvark | ⏳ |
| Standart (TC39) + legacy decorator `@resilient` | cockatiel usePolicy | ✅ |
| Express/Connect + Hono middleware, `httpStatusFor` ✅ · Fastify/Koa ⏳ | @nestjs/resilience, @fastify/circuit-breaker | ✅/⏳ |
| Nest alt yolu: çakışan decorator isimlerini kaldır | — | ⏳ |
| RxJS `executeObservable` | @nestjs/resilience | ⏳ |

## Dalga D — benimsetme

| İş | Durum |
|---|---|
| cockatiel / opossum / p-retry / tenace ile overhead benchmark'ı | ⏳ |
| Polly, cockatiel, opossum, p-retry'dan geçiş rehberleri | ⏳ |
| Doküman sitesi | ⏳ |
| Paket boyutu: `PipelineBuilder` + `xStrategy()` fabrikaları ile tree-shaking (retry 2.4 KB, retry+breaker+timeout 4.3 KB; cockatiel aynı üçlü 3.7 KB, tümü 5.5 KB), `npm run check:size` bütçe kontrolü | ✅ |
| GitHub repo, npm yayını | ⏳ (en son) |

## Bilerek ertelenen
- Tenace tarzı tipli hata birliği (`Result.match`): tüm builder tiplerini değiştirir; Dalga D sonrası ayrı karar.

## Dalga E — rakip analizinde bulunan açıklar (2026-11-08) — E1, E2, E3 ✅ tamamlandı (183 test)
Kaynak: cockatiel 4.0, opossum 10.0 (+prometheus), @julr/tenace 2.0.1 (+otel), @nestjs/resilience, Polly v8, p-retry, async-retry, exponential-backoff, axios-retry, fetch-retry, bottleneck, rate-limiter-flexible. Her madde kodumuzda grep ile doğrulandı.

### E1 — Hatalar / doğruluk (önce bunlar)
- [x] `/http` dolu bulkhead'i 429 döndürüyor, 503 olmalı → ayrı `BulkheadRejectedError` (reason, active, queued)
- [x] Takılan half-open probe breaker'ı sonsuza kadar half-open bırakıyor → `halfOpenMaxWait`
- [x] Hedging denemeleri aynı `properties` Map'ini paylaşıyor (paralel yazışma) → deneme başına kopya, kazananınki geri birleşsin
- [x] Retry, kendi hatalarımızdaki `retryAfter`'ı (açık devre, rate limit) okumuyor
- [x] ESM+CJS çift paket: `instanceof` iki kopyada başarısız olabilir → hata `code` + duck-typed guard'lar
- [x] Concurrency limiter `active`/`queued` builder'dan erişilemiyor → provider nesnesi
- [x] `AbortSignal.any` uzun ömürlü parent signal'de sızıntı yapıyor mu → ölç

### E2 — Yüksek değer
- [x] Callback context'inde deneme numarası (`attemptNumber`) — p-retry, async-retry, cockatiel, tenace, Nest
- [x] Eksik olaylar: circuit rejected, retry exhausted/gave up, her deneme (ExecutionAttempt + süre), coalesce joined, cache store failed
- [x] OTel: execution + attempt span'ları, attempt metrikleri, circuit state gauge, süre saniye cinsinden
- [x] Yerleşik istatistik nesnesi: sayaçlar + latency yüzdelikleri + periyodik snapshot (opossum `stats`)
- [x] Paylaşılan store'lar + Redis/Valkey: rate limit, breaker, cache, lock; rate-limiter-flexible adaptörü (BentoCache / Verrou adaptörleri 2026-11-08'de kaldırıldı: kendi addCache/addLock + Redis store'ları yeterli)
- [x] axios adaptörü
- [x] `/fetch`: istek başına override (retries, retryOn), özel retry koşulu, host başına pipeline

### E3 — Orta
- [x] `wrap(fn)` / fonksiyon sarmalayıcı (opossum, p-retry makeRetriable, bottleneck)
- [x] Çalışırken aç/kapa (kill switch) — opossum enable/disable
- [x] Periyodik health-check probe — opossum
- [x] Çalıştırma başına `idempotent: false` (sadece hiç başlamamış çağrıyı retry et) — tenace
- [x] dispose / shutdown / config reload — Polly, bottleneck
- [x] Tüm breaker ve limiter'ları listeleme (health), `describe()` — Nest, tenace, Polly.Testing
- [x] Hatalarda pipeline/strateji adı — Polly, Nest
- [x] Telemetri `report()`'u özel stratejiler için dışa aç — Polly, tenace
- [x] Global chaos anahtarı — tenace
- [x] Rate limit: başarıda kalan/reset bilgisi + X-RateLimit-* header'ları, block süresi / ceza / ödül, allow/deny listesi, yedek (insurance) limiter — rate-limiter-flexible
- [x] Öncelik kuyruğu, FIFO bekleme, en eskiyi düşür — bottleneck
- [x] Zamanlayıcı `unref` — cockatiel, p-retry
- [x] Retry: üstel `factor`, ilk denemeden önce bekleme, retry tüketmeyen hatalar, `maxRetryTime`, son deneme dahil `onFailedAttempt` (retriesLeft)
- [x] Nest `forRootAsync`; GraphQL/RPC hata eşleme

### E4 — Düşük ✅ (2026-11-08: hepsi yapıldı; Prometheus varsayılan Node metrikleri bilinçli olarak kapsam dışı)
Tipli hata kanalı + Result araçları (tenace, felsefe farkı, L) · streaming execution · `trip()` / üst üste isolate · half-open başarı eşiği · takılabilir breaker algoritması · snapshot'a pencereyi dahil et · `lastFailure` · group etiketi · Hystrix stream · UMD build · tipli property anahtarları · registry ad formatlayıcı · chaos ağırlıklı üreticiler + Polly olay adları · istek batching · bursty limiter · limiter dump/restore · execEvenly · esnek fixed window · waitFor backoff aralığı · `w` birimi ve 2^31 ms üstü zamanlayıcı · Fastify/Koa middleware

### E sonrası açık konular
- [x] Boyut: health check ve state paylaşımı ayrı eklenti modülüne taşındı (`circuitHealthCheck`, `sharedCircuitState`); retry+breaker+timeout 6.0 → 5.7 KB. Kalan breaker boyutu özelliklerden geliyor (iki pencere türü, probe yönetimi); daha fazlası özellik çıkarmayı gerektirir.
- [x] Deneme süresi olayı (OnExecutionAttempt) artık pipeline'dan, her callback çağrısında üretiliyor (hedging-only dahil).
- [x] rate-limiter-flexible sınırı kütüphanenin API'sinden geliyor; README'de belgelendi (rate-limiter-flexible güvenli tarafta: limit aşılmaz).
- [x] Gerçek node-redis istemcisi ve 3 düğümlü Redis Cluster ile test edildi (cluster testi `REDIS_CLUSTER` verilince çalışır).

## Tenace API referansı karşılaştırması (2026-11-08) ✅
tenace.julr.dev/reference/api sayfasındaki 18 eksik eklendi: Outcome.try/all/partition/unwrap/unwrapErr, yabancı Result adaptörleri (registerResultAdapter), `./outcome` alt yolu, addFallbackOn / addMapError / addStaleFallback (tip düzeyinde hata düşürme dahil), pipeline.isError, çalıştırma başına name/attributes/timeProvider, pipeline başına instrumentation/telemetry kapatma, tip yardımcıları (PipelineErrors, OutcomeValue, OutcomeError, ErrorCodes, ErrorWithCode), backoff yardımcıları, otel `errorType`.
Fark: bizim `attemptNumber` 0'dan başlıyor (tenace `attempt` 1'den).

