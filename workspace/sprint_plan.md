# Acme SaaS — Product Sprint Q2-126

| # | Task | Definition | Links | End of Sprint Status |
| --- | --- | --- | --- | --- |
| 1 | User Onboarding Redesign | Redesign the 3-step signup flow to reduce drop-off. Replace modal wizard with inline progressive form. | https://github.com/acme/saas/issues/201 | In Progress |
| 2 | Dashboard Performance | Reduce initial dashboard load time from 4s to under 1s by lazy-loading widgets and virtualizing the activity feed. | https://github.com/acme/saas/issues/202 | In Progress |
| 3 | Mobile Push Notifications | Implement push notification support for iOS and Android. Covers permission flow, subscription management, and delivery. | https://github.com/acme/saas/issues/203 | Done |
| 4 | Billing Integration — Stripe | Integrate Stripe Checkout and Customer Portal. Handle subscription upgrades, downgrades, and cancellations. | https://github.com/acme/saas/issues/204 | In Progress |
| 5 | API Rate Limiting | Implement per-tenant rate limiting (token bucket). Add 429 responses with Retry-After headers. | https://github.com/acme/saas/issues/205 | Not started |
| 6 | Dark Mode Support | Add dark/light theme toggle using CSS variables. Persist preference to user profile. | https://github.com/acme/saas/issues/206 | On Hold |
| 7 | Data Export (CSV/JSON) | Allow users to export their data in CSV and JSON formats from the account settings page. | https://github.com/acme/saas/issues/207 | Will be deployed next sprint |
| 8 | Audit Log Viewer | Build a read-only audit trail UI showing who did what and when, filterable by user and action type. | https://github.com/acme/saas/issues/208 | Blocked — awaiting data schema sign-off |
