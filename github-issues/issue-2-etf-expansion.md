# Phase 1: Critical ETF Universe Expansion

**Labels**: `data-infrastructure`, `phase-1`, `enhancement`
**Status**: 🚨 BLOCKED - Major gap identified

## Current Status (2025-08-25)
- ❌ **Critical Gap**: Only 17 ETFs loaded vs 250+ required in PRD
- ❌ **Missing Key ETFs**: TLT, HYG, UUP, JNK, XLK, XLF, XLE not in current dataset
- ✅ Basic ETF infrastructure exists (`dev_etf_instruments` table)
- ⚠️ Need immediate ETF universe expansion to meet PRD requirements

## Description
Expand ETF coverage to include critical market factor ETFs required for comprehensive backtesting and portfolio analysis.

## Business Context
Current ETF coverage is limited. We need comprehensive coverage of market factors including fixed income (TLT, HYG), currency (UUP), and high-yield bonds (JNK) to enable sophisticated trading strategies.

## Acceptance Criteria
- [ ] Add 200+ critical market factor ETFs to symbol universe
- [ ] Fixed Income ETFs: TLT, IEF, HYG, LQD, JNK (treasury and corporate bonds)
- [ ] Currency ETFs: UUP, DXY, FXE, FXY (USD strength and major currencies)
- [ ] High Yield Bond ETFs: HYG, JNK, SJNK, BKLN (credit and leveraged loans)
- [ ] Commodity ETFs: GLD, SLV, USO, DBA (alternative assets)
- [ ] Sector ETFs: Complete SPDR sector suite (XLK, XLF, XLE, XLV, XLI, etc.)
- [ ] Factor ETFs: Style and international exposure (IVV, VTV, VUG, VEA, VWO)

## Technical Requirements
- Update symbol universe configuration
- Validate ETF data availability across all vendors
- Ensure proper historical coverage back to 1995 where available
- Corporate actions handling for ETF distributions and splits
- ETF-specific quality scoring and validation rules

## Definition of Done
- [ ] Tests written and passing (TDD)
- [ ] Schema validation completed
- [ ] All ETFs successfully backfilled with historical data
- [ ] Integration tests pass in K8s environment
- [ ] End-to-end validation successful
- [ ] Documentation updated with new ETF list

## Estimated Timeline
1 week

## Related Documentation
- [PRD](docs/projects/30year-price-history/PRD_30_Year_Daily_Price_History.md) - See "ETF Universe Scope"
- [DRD](docs/projects/30year-price-history/DRD_30_Year_Daily_Price_History.md)