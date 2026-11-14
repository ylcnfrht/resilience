# @ylcnfrht/resilience doküman sitesi: plan

Hedef: tenace.julr.dev'den daha iyi bir site. Tenace iyi bir referans: Astro + Starlight, kısa ve kod ağırlıklı metinler,
sekmeli kod örnekleri, strateji sayfaları, arama. Eksikleri: interaktif hiçbir şey yok, diyagram yok, karşılaştırma ve
geçiş rehberi yok, kod örnekleri tip kontrolünden geçmiyor, API referansı elle yazılmış.

## Bizi öne çıkaracak 8 şey
1. **Canlı playground.** Kütüphane tarayıcıda çalıştığı için gerçek pipeline tarayıcıda koşar. "Arızalı servis" simülatörü
   (hata oranı, gecikme, kesinti kaydırıcıları) + strateji aç/kapa + zaman çizelgesi (denemeler, retry'lar, devre durumu,
   reddedilen çağrılar) + canlı istatistikler (`createStats`: p50/p99, başarı oranı).
2. **Görsel strateji açıklamaları.** Seçeneklerden canlı hesaplanan backoff grafiği (jitter dahil), animasyonlu circuit breaker
   durum makinesi, hedging zaman çizelgesi, adaptive concurrency limit grafiği.
3. **Pipeline konfigüratörü.** Stratejileri seç ve sırala; TypeScript kodu, bundle boyutu ve olası hata tipleri canlı üretilsin.
4. **Tipi kontrol edilen kod örnekleri.** Expressive Code + Twoslash: kod bloklarında hover ile tipler görünür, örnekler build'de
   tip kontrolünden geçer. Bozuk örnek siteyi yayına sokmaz.
5. **Otomatik API referansı.** TypeDoc + starlight-typedoc ile kaynak koddan üretilir, her zaman eksiksiz.
6. **Karşılaştırma ve geçiş.** cockatiel, opossum, tenace, p-retry, Polly ve @nestjs/resilience ile dürüst özellik tablosu ve
   bundle boyutları; her biri için yan yana kodlu geçiş rehberi.
7. **Tarif kitabı (recipes).** HTTP istemcileri, Postgres sorgu iptali, Redis ile çoklu instance, NestJS, Fastify/Koa/Express/Hono,
   serverless state kalıcılığı, LLM streaming, dış API kotaları, testler.
8. **AI dostu.** `llms.txt` ve `llms-full.txt`, sayfa başına "Markdown olarak kopyala". Kod asistanları kütüphaneyi doğru kullansın.

## Teknoloji
- **Astro 5 + Starlight:** dokümantasyon iskeleti, Pagefind araması, karanlık/aydınlık tema, i18n hazırlığı.
- **Özel landing sayfası** (Starlight'ın splash şablonu değil): tasarım serbestliği için.
- **Preact island'ları:** interaktif bileşenler için, yaklaşık 4 KB; sadece görünen yerde yüklenir.
- **Expressive Code + `expressive-code-twoslash`:** kod blokları, sekmeler, tipler.
- **starlight-typedoc:** API referansı.
- **Kütüphane workspace'ten** (`../src`) gelir: playground her zaman güncel kodu çalıştırır.
- **Yayın:** Cloudflare (Workers static assets veya Pages), alt alan adı ferhatyalcin.dev. Yayın en son, onayla.

## Bilgi mimarisi (üst menü: Docs · Strategies · Integrations · Guides · Reference · Playground)
- **Başlangıç:** Introduction, Installation, Quick start (5 dk), Why @ylcnfrht/resilience (karşılaştırma özeti)
- **Kavramlar:**
  - Pipelines & strategy order
  - What counts as a failure (varsayılanlar)
  - Context, signals & deadline propagation
  - Outcomes & typed errors (settle/match)
  - Telemetry model
  - Bundle size & tree-shaking
  - Runtimes (Node/Bun/Deno/Workers/browser)
- **Stratejiler (16 sayfa).** Her sayfanın şablonu: ne zaman kullanılır, çalışma şekli (diyagram), örnek, seçenekler tablosu,
  olaylar, hatalar, sık yapılan hatalar, ilgili tarifler. Sayfalar:
  - Retry (+ backoff & jitter, retry budget)
  - Timeout
  - Circuit breaker (+ policies, extensions, persistence)
  - Fallback (+ fallbackOn, mapError, stale fallback)
  - Hedging
  - Bulkhead/concurrency
  - Adaptive concurrency
  - Rate limiters (token bucket / window / sliding / partitioned / bursty)
  - Cache & coalesce
  - Lock
  - Chaos
  - Batches & waitFor
  - Batcher
- **Entegrasyonlar:**
  - fetch, axios
  - Express, Fastify, Koa, Hono, NestJS
  - Redis, adaptörler (rate-limiter-flexible, BentoCache, Verrou)
  - OpenTelemetry, Prometheus, createStats, Hystrix
- **Rehberler / tarifler:**
  - Testing with FakeTimeProvider
  - Databases (Postgres iptali)
  - Serverless & persistence
  - Multi-instance with Redis
  - Streaming & LLMs
  - Calling rate-limited APIs
  - Load shedding
  - Chaos game days
- **Geçiş rehberleri:** from Polly, cockatiel, opossum, p-retry/async-retry, axios-retry, tenace, @nestjs/resilience.
- **Referans:**
  - API (TypeDoc)
  - Events catalog
  - Error codes & HTTP mapping
  - Options cheat sheet
  - Comparison
  - Bundle sizes
  - Changelog
- **Playground:** tam ekran simülatör ve konfigüratör.

## Landing sayfası (sırasıyla)
1. **Hero:** "Every failure handled. Every type checked." + kısa açıklama, kurulum (kopyala butonlu), "Get started" / "Playground" /
   GitHub. Yanında **canlı mini demo:** arızalı servise 20 istek; pipeline'sız ve pipeline'lı iki sütun yan yana, başarı oranları.
2. **Rozet şeridi:** zero deps · ESM+CJS · Node/Bun/Deno/Workers/Browser · 3.9 KB'tan başlayan boyut · Polly v8 API.
3. **"One pipeline, every failure mode":** 6 kart (Failures, Overload, Latency, Distributed, Typed errors, Testing). Her kartta
   kısa kod ve "Read more".
4. **Sekmeli "In code":** HTTP, Database, NestJS, Typed errors, Streaming, Testing.
5. **"Only in @ylcnfrht/resilience":** hedging, adaptive concurrency, deadline propagation, retry budget, Postgres sorgu iptali
   tarifi, typed errors + Polly API birlikte.
6. **Karşılaştırma tablosu teaser'ı** (cockatiel / opossum / tenace / p-retry) + "Full comparison" linki.
7. **Toolbox:** tüm API'lerin kategorili listesi.
8. **Kapanış CTA'sı + footer.**

## Tasarım dili
- **Kişilik:** "kontrol paneli" estetiği. Koyu tema öncelikli, sinyal renkleri: yeşil = sağlıklı, amber = half-open, kırmızı = open.
- **Fontlar:** başlıklarda Inter Display, kodda JetBrains Mono. Bol boşluk, ince çerçeveli kartlar, durum renkleriyle uyumlu
  ince animasyonlar.
- **Logo:** stilize bir "sigorta" ya da devre sembolü (SVG, tek renkte de okunur).
- **Erişilebilirlik:** AA kontrast, klavye ile gezilebilen playground, `prefers-reduced-motion` ile animasyonları kapatma.

## Kalite kapıları (bitti sayılması için)
- `astro build` hatasız; Twoslash kod örneklerinin hepsi tip kontrolünden geçer.
- Kırık link kontrolü (starlight-links-validator).
- Lighthouse: performans ≥ 95, erişilebilirlik 100, SEO 100. Landing JS'i < 60 KB gzip.
- Playground'da sahte saat yerine gerçek saat; sabit tohumlu (seed) rastgelelik, tekrar üretilebilir senaryolar.
- Mobilde (375 px) bütün sayfalar okunur, playground kullanılabilir.

## Uygulama sırası
- **Faz 1, iskelet ve tasarım** (ben): Astro + Starlight kurulumu, tema, logo, menü, sayfa şablonları, Twoslash, TypeDoc,
  workspace bağlantısı.
- **Faz 2, paralel içerik** (ajanlar, her biri kendi klasöründe):
  - A, Kavramlar + Başlangıç
  - B, Stratejiler 1 (retry, timeout, breaker, fallback, hedging)
  - C, Stratejiler 2 (limiter'lar, cache, lock, chaos, batches)
  - D, Entegrasyonlar
  - E, Rehberler + Geçiş + Karşılaştırma
- **Faz 3, interaktif:** playground, backoff grafiği, breaker durum makinesi, konfigüratör, landing'deki canlı demo.
- **Faz 4, son kat** (ben): landing sayfası, llms.txt, OG görselleri, arama, Lighthouse, link kontrolü, mobil kontrol.
- **Faz 5, yayın** (onayınla): alan adı ve Cloudflare.
