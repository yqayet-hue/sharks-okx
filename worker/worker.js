export default {
  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname;
    const search = url.search;

    const OKX_HOSTS = [
      "https://www.okx.com"
    ];

    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,HEAD,OPTIONS",
      "Access-Control-Allow-Headers": "*",
      "Cache-Control": "no-store"
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: cors });
    }

    if (path === "/" || path === "") {
      return new Response(JSON.stringify({
        ok: true,
        proxy: "Adiga Eagle OKX Market Data Proxy",
        okx: true,
        markets: ["SPOT", "SWAP"]
      }), {
        status: 200,
        headers: { ...cors, "Content-Type": "application/json" }
      });
    }

    // Only public OKX market-data endpoints are exposed.
    // No API key, secret, trading, orders, or account endpoints.
    const allowed =
      path.startsWith("/api/v5/market/") ||
      path.startsWith("/api/v5/public/");

    if (!allowed) {
      return new Response(JSON.stringify({
        ok: false,
        error: "Only OKX public market-data endpoints are allowed"
      }), {
        status: 404,
        headers: { ...cors, "Content-Type": "application/json" }
      });
    }

    let lastStatus = 502;
    let lastBody = "";

    for (const host of OKX_HOSTS) {
      try {
        const response = await fetch(host + path + search, {
          method: "GET",
          headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/154 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9"
          },
          cf: {
            cacheTtl: 0,
            cacheEverything: false
          }
        });

        const body = await response.text();

        if (response.ok) {
          return new Response(body, {
            status: response.status,
            headers: {
              ...cors,
              "Content-Type":
                response.headers.get("Content-Type") || "application/json",
              "X-Adiga-Proxy-Source": "OKX"
            }
          });
        }

        lastStatus = response.status;
        lastBody = body;
      } catch (e) {
        lastStatus = 502;
        lastBody = String(e?.message || e);
      }
    }

    return new Response(JSON.stringify({
      ok: false,
      error: "OKX upstream request failed",
      status: lastStatus,
      body: lastBody.slice(0, 1000)
    }), {
      status: lastStatus,
      headers: { ...cors, "Content-Type": "application/json" }
    });
  }
};
