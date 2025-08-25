# PROPOSED DOCUMENTATION STRUCTURE

## CURRENT PROBLEM
- 111+ documentation files
- Massive duplication and redundancy  
- Multiple PRDs for old features
- Scattered organization
- Engineers can't find what they need

## TARGET STRUCTURE (8 essential docs)

```
docs/
├── README.md                    # 🏠 Navigation hub - start here
├── QUICK_START.md               # 🚀 Get running in 15 minutes
├── DEVELOPMENT_GUIDE.md         # 💻 How to develop, test, deploy
├── SYSTEM_ARCHITECTURE.md       # 🏗️ High-level system understanding  
├── DEPLOYMENT_GUIDE.md          # 🚀 How to deploy to all environments
├── TROUBLESHOOTING.md           # 🔧 Common issues and solutions
├── product/
│   └── PRODUCT_REQUIREMENTS.md # 📋 What we're building (already good)
└── archive/                     # 📦 Minimal historical reference
    └── migration_summaries.md   # Key migration info only
```

## FILES TO DELETE (90+ files)

### Duplicate/Redundant Content
- All DRD_*.md files (old design docs)  
- Multiple PRD_*.md files for completed features
- Multiple deployment guides (consolidate to one)
- Multiple workflow docs (consolidate to one)
- All progress summaries and status reports
- Multiple setup guides

### Platform-Specific Guides (move to troubleshooting)
- WSL_*.md files
- MINIKUBE_*.md files  
- KIND_VS_MINIKUBE.md

### Implementation Details (not user-facing)
- CHART_VISUALIZATION_REGRESSION_PROTECTION.md
- DATASET_TABLE_REGRESSION_PROTECTION.md
- GIT_WORKFLOW_PROTECTION.md

### Historical/Completed Items
- Migration guides for completed migrations
- Old testing frameworks
- Completed feature implementations
- Archive/pre-consolidation content

## CONSOLIDATION PLAN
1. Create 8 essential docs from best content
2. Delete 90+ redundant files
3. Archive only critical historical info
4. Update main README as navigation hub

Target: From 111+ docs → 8 essential docs (92% reduction)