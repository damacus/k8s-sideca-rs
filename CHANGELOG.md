# Changelog

## [0.2.4](https://github.com/damacus/k8s-sideca-rs/compare/v0.2.3...v0.2.4) (2026-10-04)


### Bug Fixes

* **health:** park health task on bind failure instead of exiting ([#39](https://github.com/damacus/k8s-sideca-rs/issues/39)) ([bf4ce0c](https://github.com/damacus/k8s-sideca-rs/commit/bf4ce0c9046716e63ff87a2599516515e426ee55))

## [0.2.3](https://github.com/damacus/k8s-sideca-rs/compare/v0.2.2...v0.2.3) (2026-10-04)


### Bug Fixes

* **watch:** log routine stream rotation at info, not error ([#37](https://github.com/damacus/k8s-sideca-rs/issues/37)) ([5e2e513](https://github.com/damacus/k8s-sideca-rs/commit/5e2e513022fd0bfd39db29bad78b4078131c65a8))

## [0.2.2](https://github.com/damacus/k8s-sideca-rs/compare/v0.2.1...v0.2.2) (2026-10-03)


### Bug Fixes

* **watch:** bound watch event wait by WATCH_CLIENT_TIMEOUT ([#35](https://github.com/damacus/k8s-sideca-rs/issues/35)) ([3721961](https://github.com/damacus/k8s-sideca-rs/commit/372196189f1ad0ef6acb4eb61141ebd21c41a6f5))

## [0.2.1](https://github.com/damacus/k8s-sideca-rs/compare/v0.2.0...v0.2.1) (2026-10-02)


### Bug Fixes

* **config:** accept LOG_FORMAT case-insensitively ([#28](https://github.com/damacus/k8s-sideca-rs/issues/28)) ([6c2ac21](https://github.com/damacus/k8s-sideca-rs/commit/6c2ac21a76c7e88230f3c8d9ace34bf33559ecbb))
* **config:** apply LOG_TZ and parse it case-insensitively ([#32](https://github.com/damacus/k8s-sideca-rs/issues/32)) ([2b8ff18](https://github.com/damacus/k8s-sideca-rs/commit/2b8ff1855a54172873c33ca5082a7e5d0c8b9043))
* **config:** cap retry backoff at urllib3's BACKOFF_MAX ([#27](https://github.com/damacus/k8s-sideca-rs/issues/27)) ([ede5cc1](https://github.com/damacus/k8s-sideca-rs/commit/ede5cc1ceaa2c38abc8024bb508ff231c120c88d))
* **config:** clamp zero sleep intervals to 1s ([551cb14](https://github.com/damacus/k8s-sideca-rs/commit/551cb1419d6e2bf3dd2bb94e7fb242914a64e0d7))
* **config:** clamp zero sleep intervals to 1s ([344f89b](https://github.com/damacus/k8s-sideca-rs/commit/344f89be8c312e746abaaea6a96e46351d062a7c))
* **config:** honor REQ_RETRY_CONNECT and REQ_RETRY_READ budgets ([#30](https://github.com/damacus/k8s-sideca-rs/issues/30)) ([3a09c2d](https://github.com/damacus/k8s-sideca-rs/commit/3a09c2d05974d8ff14e58eeab7e74f28d00db815))
* **config:** reject non-positive/non-finite float settings ([#26](https://github.com/damacus/k8s-sideca-rs/issues/26)) ([4f6faa8](https://github.com/damacus/k8s-sideca-rs/commit/4f6faa8c0ffb51c2de0248d7e589040f5d8040dd))
* **config:** reject zero and u32-overflowing watch timeouts ([#24](https://github.com/damacus/k8s-sideca-rs/issues/24)) ([2a2aca2](https://github.com/damacus/k8s-sideca-rs/commit/2a2aca26e02cda1b9498393fbcafe6c7617baef7))
* **config:** validate HEALTH_PORT range instead of truncating ([#34](https://github.com/damacus/k8s-sideca-rs/issues/34)) ([51ec0fa](https://github.com/damacus/k8s-sideca-rs/commit/51ec0fa3d93959e5a09000a6443bc6112cb8e2a1))
* **config:** warn when RESOURCE_NAME is set with NAMESPACE=ALL ([#31](https://github.com/damacus/k8s-sideca-rs/issues/31)) ([50a008d](https://github.com/damacus/k8s-sideca-rs/commit/50a008df14a717f6f47bd287a9e415f947b76381))
* **files:** failed apply must not consume the resource_version ([#22](https://github.com/damacus/k8s-sideca-rs/issues/22)) ([f2a7c42](https://github.com/damacus/k8s-sideca-rs/commit/f2a7c426b1ec904f96cb2e2fa5cce10d01d8170e))
* **files:** keep ownership when file removal fails ([#33](https://github.com/damacus/k8s-sideca-rs/issues/33)) ([8c90060](https://github.com/damacus/k8s-sideca-rs/commit/8c900604f7313da24ea91fce378d7db91e60eb74))
* **health:** bound healthz request reads ([331949e](https://github.com/damacus/k8s-sideca-rs/commit/331949e3090618997899c1d51ad2abcfbc6c27bc))
* **health:** bound healthz request reads ([f00e9e3](https://github.com/damacus/k8s-sideca-rs/commit/f00e9e3e2e1d074dd37b9f6b9cdf4b23cdf57b37))
* **health:** exit the accept thread on shutdown ([#19](https://github.com/damacus/k8s-sideca-rs/issues/19)) ([8da2e70](https://github.com/damacus/k8s-sideca-rs/commit/8da2e703a6147d3f349e33b7694b6f47ee4f7dbd))
* **http:** REQ_SKIP_TLS_VERIFY actually disables verification ([#29](https://github.com/damacus/k8s-sideca-rs/issues/29)) ([f058cad](https://github.com/damacus/k8s-sideca-rs/commit/f058cadf15f07b448f202ba18a18a99eb0d5c1e3))
* **reload:** deliver pending callback before METHOD=LIST exits ([#21](https://github.com/damacus/k8s-sideca-rs/issues/21)) ([140b54e](https://github.com/damacus/k8s-sideca-rs/commit/140b54e9b52dc7b79b8ca13f7c184999fd20eb9c))
* **reload:** send text REQ_PAYLOAD as text/plain, not application/json ([#18](https://github.com/damacus/k8s-sideca-rs/issues/18)) ([b50ed54](https://github.com/damacus/k8s-sideca-rs/commit/b50ed54981dc79662ce2e1745394affb16410c96))
* **watch:** a dead stream stays dead until real events arrive ([#20](https://github.com/damacus/k8s-sideca-rs/issues/20)) ([7d7ac4d](https://github.com/damacus/k8s-sideca-rs/commit/7d7ac4dbdb5ccece8ae7f06dba49c23ce986efcd))
* **watch:** drop owners entries when keys leave known ([#16](https://github.com/damacus/k8s-sideca-rs/issues/16)) ([c08f96c](https://github.com/damacus/k8s-sideca-rs/commit/c08f96c2d7b472ba7ba82dd2d79710395bec6b64))
* **watch:** preserve owned files when apply fails ([d5dee62](https://github.com/damacus/k8s-sideca-rs/commit/d5dee62f38fbe66825130ad7deccd1a5ba091d5b))
* **watch:** preserve owned files when apply fails ([3741de6](https://github.com/damacus/k8s-sideca-rs/commit/3741de62d0f014b436f51cbbf2f53ec4ef83be83))

## [0.2.0](https://github.com/damacus/k8s-sideca-rs/compare/v0.1.0...v0.2.0) (2026-10-02)


### Features

* initial kiwigrid/k8s-sidecar port to Rust ([4720fb1](https://github.com/damacus/k8s-sideca-rs/commit/4720fb1b83695922e8c95ffd7c73b80366f5cdb6))


### Bug Fixes

* align with upstream 2.11.2 semantics ([3b0a8b8](https://github.com/damacus/k8s-sideca-rs/commit/3b0a8b8dd9311023a3359ac8940ad0e391edd142))
* **ci:** harden publish workflow and caller permissions ([5d3918c](https://github.com/damacus/k8s-sideca-rs/commit/5d3918ced52b290fd8699e9d89f35ea29fef91f7))
* drive image publish from release-please outputs ([1ffc3f2](https://github.com/damacus/k8s-sideca-rs/commit/1ffc3f2eccc87ed6cdf1320f30eaf41879139d49))
* native-runner matrix for multi-arch image publish ([968e402](https://github.com/damacus/k8s-sideca-rs/commit/968e402dcb022158bb5183d242f3808554a14fdf))
* per-arch tags for manifest merge ([2d028d6](https://github.com/damacus/k8s-sideca-rs/commit/2d028d6c4e5b96325ef76fed7afa5c9807a49083))

## 0.1.0 (2026-10-02)


### Features

* initial kiwigrid/k8s-sidecar port to Rust ([4720fb1](https://github.com/damacus/k8s-sideca-rs/commit/4720fb1b83695922e8c95ffd7c73b80366f5cdb6))


### Bug Fixes

* align with upstream 2.11.2 semantics ([3b0a8b8](https://github.com/damacus/k8s-sideca-rs/commit/3b0a8b8dd9311023a3359ac8940ad0e391edd142))
